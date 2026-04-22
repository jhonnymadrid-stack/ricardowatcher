import asyncio
import io
import os
from datetime import datetime, timezone

import httpx
from curl_cffi.requests import AsyncSession
from dotenv import load_dotenv

from classifier import classify, SEARCH_QUERIES, PRODUCT_GROUPS, min_price
from db import (
    init_db, upsert_listing, save_snapshot, mark_critical,
    increment_miss, get_critical_listings, get_active_listing_ids,
    get_listing, record_sale, mark_status,
    get_price_stats, get_sales_for_chart, count_sales_by_category, get_counts,
    get_daily_top_sales, get_daily_top_gangas,
)
from scraper import search_ricardo, fetch_listing_status

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

CRITICAL_THRESHOLD_SECS = 2 * 3600   # listings with <2h go to critical
WATCHER_INTERVAL_NORMAL = 5 * 60     # 5 min
WATCHER_INTERVAL_URGENT = 60         # 1 min when any listing has <10 min
DISCOVERY_INTERVAL = 60 * 60         # 1 hour
MAX_MISS_BEFORE_GONE = 2             # consecutive misses before declaring gone
DAILY_SUMMARY_HOUR_UTC = 21          # 23:00 CEST / 22:00 CET

# Shared state for /status command
_state = {
    "started_at": None,
    "next_discovery": None,
    "last_discovery": None,
    "watcher_interval": WATCHER_INTERVAL_NORMAL,
    "new_last_discovery": None,  # count of new listings found in last discovery
}


# ── Telegram helpers ──────────────────────────────────────────────────────────

async def tg_send(text: str, client: httpx.AsyncClient):
    await client.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        json={"chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"},
        timeout=15,
    )


async def tg_photo(photo_bytes: bytes, caption: str, client: httpx.AsyncClient):
    await client.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto",
        data={"chat_id": CHAT_ID, "caption": caption, "parse_mode": "HTML"},
        files={"photo": ("chart.png", photo_bytes, "image/png")},
        timeout=20,
    )


# ── Scrape all queries, return {id: listing+category} ─────────────────────────

async def scrape_all(session: AsyncSession) -> dict[str, dict]:
    found: dict[str, dict] = {}
    for query in SEARCH_QUERIES:
        listings = await search_ricardo(query, session)
        for l in listings:
            if l["id"] in found:
                continue
            match = classify(l["title"])
            if match is None:
                continue
            if l["price"] < min_price(match.category):
                continue
            l["category"] = match.category
            l["display"] = match.display
            l["storage"] = match.storage
            found[l["id"]] = l
        await asyncio.sleep(2)
    return found


# ── Disappearance logic ───────────────────────────────────────────────────────

def resolve_disappearance(row) -> tuple[bool, str, float]:
    """
    Returns (sold, sale_type, final_price).
    sold=False means withdrawn/expired without sale.

    Bid count is unreliable from Ricardo's payload, so we use
    current_price > initial_price as proxy for "someone bid".
    """
    has_buy_now = bool(row["has_buy_now"])
    buy_now_price = row["buy_now_price"]
    current_price = row["current_price"]
    initial_price = row["initial_price"]
    secs = row["seconds_remaining"] or 9999

    # Listings with many hours left that disappear likely fell off search pagination,
    # not actually sold. Only declare sold if time was nearly up.
    if secs > CRITICAL_THRESHOLD_SECS:
        return False, "", 0.0

    # Price rose above starting price → auction sold
    if current_price > initial_price:
        return True, "auction", current_price

    # No bids but had Buy Now and was close to expiry → sold via BN
    if has_buy_now and buy_now_price:
        return True, "buy_now", buy_now_price

    return False, "", 0.0


# ── Discovery loop (hourly) ───────────────────────────────────────────────────

async def discovery_loop():
    from datetime import timedelta
    _state["started_at"] = datetime.now(timezone.utc)
    print("[Discovery] Iniciado")
    async with AsyncSession(impersonate="chrome124") as session:
        while True:
            try:
                _state["last_discovery"] = datetime.now(timezone.utc)
                ts = datetime.now().strftime("%H:%M:%S")
                print(f"[Discovery] {ts} — scrapeando Ricardo...")

                current = await scrape_all(session)
                active_ids = get_active_listing_ids()
                new_count = 0

                # Register / update found listings
                for lid, l in current.items():
                    if lid not in active_ids:
                        new_count += 1
                    upsert_listing(
                        lid, l["title"], l["url"], l["category"], l["storage"],
                        l["price"], l["buy_now_price"], l["has_buy_now"],
                        l["bid_count"], l["seconds_remaining"],
                    )
                    save_snapshot(lid, l["price"], l["bid_count"], l["seconds_remaining"])

                    secs = l["seconds_remaining"]
                    if secs is not None and 0 < secs <= CRITICAL_THRESHOLD_SECS:
                        row = get_listing(lid)
                        if row and not row["is_critical"]:
                            mark_critical(lid)
                            mins = secs // 60
                            print(f"  📍 CRÍTICO: {l['title']} — {mins} min restantes")

                _state["new_last_discovery"] = new_count

                # Check disappeared non-critical listings
                disappeared = active_ids - set(current.keys())
                for lid in disappeared:
                    row = get_listing(lid)
                    if not row or row["is_critical"]:
                        continue  # critical ones handled by watcher

                    misses = increment_miss(lid)
                    if misses < MAX_MISS_BEFORE_GONE:
                        continue

                    page_status = await fetch_listing_status(lid, session)
                    if page_status is False:
                        # Still active on Ricardo — just fell off search pagination
                        print(f"  ↩ Paginación (discovery): {row['title']} — sigue activo")
                        continue

                    if page_status is True:
                        # Confirmed sold on Ricardo's own page
                        _, sale_type, final_price = resolve_disappearance(row)
                        if not sale_type:
                            if row["has_buy_now"] and row["buy_now_price"]:
                                sale_type, final_price = "buy_now", float(row["buy_now_price"])
                            else:
                                sale_type, final_price = "auction", float(row["current_price"])
                        record_sale(
                            lid, row["title"], row["url"], row["category"], row["storage"],
                            row["initial_price"], final_price, sale_type, row["bid_count"],
                        )
                        print(f"  💰 VENTA (discovery): {row['title']} CHF {final_price} ({sale_type})")
                        continue

                    # page_status is None — fall back to time-based heuristic
                    sold, sale_type, final_price = resolve_disappearance(row)
                    if sold:
                        record_sale(
                            lid, row["title"], row["url"], row["category"], row["storage"],
                            row["initial_price"], final_price, sale_type, row["bid_count"],
                        )
                        print(f"  💰 VENTA (discovery, heur): {row['title']} CHF {final_price} ({sale_type})")
                    else:
                        mark_status(lid, "withdrawn")

            except Exception as e:
                print(f"[Discovery] Error: {e}")

            _state["next_discovery"] = datetime.now(timezone.utc) + timedelta(seconds=DISCOVERY_INTERVAL)
            print(f"[Discovery] Esperando 1 hora...")
            await asyncio.sleep(DISCOVERY_INTERVAL)


# ── Watcher loop (variable frequency) ────────────────────────────────────────

async def watcher_loop():
    await asyncio.sleep(15)
    print("[Watcher] Iniciado")

    async with AsyncSession(impersonate="chrome124") as session:
        while True:
            critical = get_critical_listings()
            if not critical:
                await asyncio.sleep(60)
                continue

            min_secs = min((r["seconds_remaining"] or 99999) for r in critical)
            interval = WATCHER_INTERVAL_URGENT if min_secs <= 600 else WATCHER_INTERVAL_NORMAL

            ts = datetime.now().strftime("%H:%M:%S")
            print(f"[Watcher] {ts} — {len(critical)} críticos (intervalo {interval}s)")

            current = await scrape_all(session)

            for row in critical:
                lid = row["id"]

                if lid in current:
                    l = current[lid]
                    upsert_listing(
                        lid, l["title"], l["url"], l["category"], l["storage"],
                        l["price"], l["buy_now_price"], l["has_buy_now"],
                        l["bid_count"], l["seconds_remaining"],
                    )
                    save_snapshot(lid, l["price"], l["bid_count"], l["seconds_remaining"])
                else:
                    misses = increment_miss(lid)
                    if misses < MAX_MISS_BEFORE_GONE:
                        continue

                    # Refresh row after updates
                    row = get_listing(lid)

                    page_status = await fetch_listing_status(lid, session)
                    if page_status is False:
                        print(f"  ↩ Paginación (watcher): {row['title']} — sigue activo")
                        continue

                    if page_status is True:
                        _, sale_type, final_price = resolve_disappearance(row)
                        if not sale_type:
                            if row["has_buy_now"] and row["buy_now_price"]:
                                sale_type, final_price = "buy_now", float(row["buy_now_price"])
                            else:
                                sale_type, final_price = "auction", float(row["current_price"])
                        record_sale(
                            lid, row["title"], row["url"], row["category"], row["storage"],
                            row["initial_price"], final_price, sale_type, row["bid_count"],
                        )
                        print(f"  ✅ VENDIDO: {row['title']} CHF {final_price} ({sale_type})")
                        continue

                    # page_status is None — fall back to heuristic
                    sold, sale_type, final_price = resolve_disappearance(row)
                    if sold:
                        record_sale(
                            lid, row["title"], row["url"], row["category"], row["storage"],
                            row["initial_price"], final_price, sale_type, row["bid_count"],
                        )
                        print(f"  ✅ VENDIDO (heur): {row['title']} CHF {final_price} ({sale_type})")
                    else:
                        mark_status(lid, "expired_no_sale")
                        print(f"  ❌ EXPIRADO sin venta: {row['title']}")

            _state["watcher_interval"] = interval
            await asyncio.sleep(interval)


# ── Daily summary ────────────────────────────────────────────────────────────

async def send_daily_summary():
    from datetime import timedelta
    top_sales = get_daily_top_sales(3)
    top_gangas = get_daily_top_gangas(3)

    lines = ["📊 <b>Resumen del día — Ricardo Watcher</b>\n"]

    lines.append("🏆 <b>Top ventas</b> (precio final más alto)")
    if top_sales:
        for i, r in enumerate(top_sales, 1):
            tipo = "subasta" if r["sale_type"] == "auction" else "buy now"
            lines.append(f"{i}. CHF {r['final_price']:.0f} ({tipo}) — {r['title']}\n   {r['url']}")
    else:
        lines.append("Sin ventas registradas hoy.")

    lines.append("\n💰 <b>Top gangas</b> (más baratas vs media histórica)")
    if top_gangas:
        for i, r in enumerate(top_gangas, 1):
            pct = int(r["saving"] / r["cat_avg"] * 100) if r["cat_avg"] else 0
            lines.append(
                f"{i}. CHF {r['final_price']:.0f} "
                f"(media {r['cat_avg']:.0f}, -{pct}%) — {r['title']}\n   {r['url']}"
            )
    else:
        lines.append("Sin datos suficientes para calcular gangas hoy.")

    async with httpx.AsyncClient() as client:
        await tg_send("\n".join(lines), client)
    print("[Summary] Resumen diario enviado")


async def daily_summary_loop():
    from datetime import timedelta
    while True:
        now = datetime.now(timezone.utc)
        target = now.replace(hour=DAILY_SUMMARY_HOUR_UTC, minute=0, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        secs = (target - now).total_seconds()
        print(f"[Summary] Próximo resumen en {int(secs // 3600)}h {int((secs % 3600) // 60)}min")
        await asyncio.sleep(secs)
        await send_daily_summary()


# ── Telegram command handlers ─────────────────────────────────────────────────

async def cmd_help(chat_id: int, client: httpx.AsyncClient):
    text = (
        "<b>Comandos disponibles</b>\n\n"
        "<b>/status</b>\n"
        "  Estado del bot: uptime, proximo scrape, anuncios activos y ventas registradas.\n\n"
        "<b>/articulos</b>\n"
        "  Lista todos los grupos monitorizados (PS5, iPhone, Samsung...).\n\n"
        "<b>/articulos &lt;grupo&gt;</b>\n"
        "  Muestra todos los modelos de un grupo con sus ventas registradas.\n"
        "  Ejemplos: /articulos ps5  /articulos iphone  /articulos samsung\n\n"
        "<b>/precio &lt;articulo&gt;</b>\n"
        "  Precio medio, minimo y maximo de venta en los ultimos 30 dias.\n"
        "  Ejemplo: /precio ps5 disc\n\n"
        "<b>/grafico &lt;articulo&gt;</b>\n"
        "  Grafico de evolucion de precios de venta (ultimos 90 dias) enviado como imagen.\n"
        "  Ejemplo: /grafico iphone 13 pro\n\n"
        "<b>/help</b>\n"
        "  Muestra este mensaje."
    )
    await client.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
        timeout=15,
    )


async def cmd_status(chat_id: int, client: httpx.AsyncClient):
    now = datetime.now(timezone.utc)
    counts = get_counts()

    started = _state["started_at"]
    uptime = str(now - started).split(".")[0] if started else "?"

    next_d = _state["next_discovery"]
    if next_d:
        secs_left = int((next_d - now).total_seconds())
        if secs_left > 0:
            mins_left = secs_left // 60
            next_str = f"en {mins_left} min"
        else:
            next_str = "scrapeando ahora"
    else:
        next_str = "scrapeando ahora"

    watcher_int = _state["watcher_interval"]
    watcher_str = "1 min (urgente)" if watcher_int <= 60 else "5 min"

    new_count = _state["new_last_discovery"]
    new_str = f"<b>{new_count}</b>" if new_count is not None else "?"

    text = (
        f"<b>Ricardo Watcher activo</b>\n\n"
        f"Uptime: {uptime}\n"
        f"Proximo discovery: {next_str}\n"
        f"Intervalo watcher: {watcher_str}\n\n"
        f"Anuncios activos: <b>{counts['active']}</b>\n"
        f"Criticos (&lt;2h): <b>{counts['critical']}</b>\n"
        f"Nuevos ultimo scrape: {new_str}\n"
        f"Ventas registradas: <b>{counts['sales']}</b>"
    )
    await client.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
        timeout=15,
    )


GROUP_ALIASES = {
    "ps5":           "PlayStation 5",
    "playstation5":  "PlayStation 5",
    "playstation 5": "PlayStation 5",
    "ps4":           "PlayStation 4",
    "playstation4":  "PlayStation 4",
    "playstation 4": "PlayStation 4",
    "switch":        "Nintendo Switch",
    "nintendo":      "Nintendo Switch",
    "nintendo switch": "Nintendo Switch",
    "gameboy":       "GameBoy",
    "game boy":      "GameBoy",
    "iphone":        "iPhone 11+",
    "samsung":       "Samsung Galaxy S21+",
    "galaxy":        "Samsung Galaxy S21+",
    "ipad pro":      "iPad Pro",
    "ipad air":      "iPad Air",
    "ipad mini":     "iPad mini",
    "ipad":          "iPad",
    "macbook air":   "MacBook Air",
    "macbook pro":   "MacBook Pro",
    "macbook":       "MacBook Air",
    "mac mini":      "Mac mini",
    "imac":          "iMac",
    "mac":           "MacBook Air",
}


async def cmd_articulos(chat_id: int, arg: str, client: httpx.AsyncClient):
    async def send(text: str):
        await client.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=15,
        )

    if not arg:
        # Top-level: show group names only
        lines = ["<b>Articulos monitorizados:</b>\n"]
        for group_name, items in PRODUCT_GROUPS:
            n_total = sum(count_sales_by_category(cat) for cat, _ in items)
            lines.append(f"  - {group_name} ({n_total} ventas)")
        lines.append("\nUsa /articulos &lt;nombre&gt; para ver modelos detallados.")
        await send("\n".join(lines))
        return

    # Find matching group
    target = GROUP_ALIASES.get(arg.lower().strip())
    if not target:
        await send(f"Grupo no encontrado: '{arg}'\nOpciones: ps5, ps4, switch, gameboy, iphone, samsung")
        return

    group_items = next((items for name, items in PRODUCT_GROUPS if name == target), None)
    if not group_items:
        return

    lines = [f"<b>{target}</b>\n"]
    for cat_pattern, display_name in group_items:
        n = count_sales_by_category(cat_pattern)
        lines.append(f"  - {display_name} ({n} ventas)")

    # Split into chunks of max 50 items to avoid Telegram message limit
    chunk_size = 50
    all_lines = lines[:]
    header = all_lines[0]
    items_lines = all_lines[1:]
    for i in range(0, len(items_lines), chunk_size):
        chunk = items_lines[i:i + chunk_size]
        prefix = header if i == 0 else f"<b>{target} (cont.)</b>\n"
        await send(prefix + "\n".join(chunk))


async def cmd_precio(chat_id: int, query: str, client: httpx.AsyncClient):
    pattern = f"%{query.lower().replace(' ', '_')}%"
    rows = get_price_stats(pattern, days=30)

    if not rows:
        await client.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": chat_id,
                  "text": f"Sin datos para <b>{query}</b> (últimos 30 días).",
                  "parse_mode": "HTML"},
            timeout=15,
        )
        return

    finals = [r["final_price"] for r in rows]
    initials = [r["initial_price"] for r in rows]
    auctions = [r for r in rows if r["sale_type"] == "auction"]
    buyNows = [r for r in rows if r["sale_type"] == "buy_now"]

    avg_f = sum(finals) / len(finals)
    avg_i = sum(initials) / len(initials)

    text = (
        f"📊 <b>{query}</b> — últimos 30 días\n\n"
        f"Ventas registradas: <b>{len(rows)}</b> "
        f"({len(auctions)} subasta, {len(buyNows)} buy now)\n\n"
        f"Precio final\n"
        f"  Promedio: <b>CHF {avg_f:.0f}</b>\n"
        f"  Mínimo:   CHF {min(finals):.0f}\n"
        f"  Máximo:   CHF {max(finals):.0f}\n\n"
        f"Precio inicial promedio: CHF {avg_i:.0f}"
    )

    await client.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
        timeout=15,
    )


async def cmd_grafico(chat_id: int, query: str, client: httpx.AsyncClient):
    pattern = f"%{query.lower().replace(' ', '_')}%"
    rows = get_sales_for_chart(pattern, days=90)

    if not rows:
        await client.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": chat_id,
                  "text": f"Sin datos para <b>{query}</b>.",
                  "parse_mode": "HTML"},
            timeout=15,
        )
        return

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    dates = [datetime.fromisoformat(r["sold_at"]) for r in rows]
    prices = [r["final_price"] for r in rows]
    avg = sum(prices) / len(prices)

    _, ax = plt.subplots(figsize=(10, 5))
    ax.plot(dates, prices, "o-", color="#2196F3", linewidth=2, markersize=6, label="Venta")
    ax.axhline(avg, color="#FF5722", linewidth=1.5, linestyle="--", label=f"Media CHF {avg:.0f}")
    ax.set_title(f"Evolución precios: {query} (últimos 90 días)", fontsize=13)
    ax.set_ylabel("CHF")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m"))
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=150)
    buf.seek(0)
    plt.close()

    await client.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto",
        data={"chat_id": chat_id,
              "caption": f"📈 {query} — {len(rows)} ventas",
              "parse_mode": "HTML"},
        files={"photo": ("chart.png", buf.read(), "image/png")},
        timeout=20,
    )


# ── Telegram polling loop ─────────────────────────────────────────────────────

async def telegram_loop():
    offset = 0
    print("[Telegram] Bot iniciado")

    async with httpx.AsyncClient() as client:
        while True:
            try:
                resp = await client.get(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
                    params={"offset": offset, "timeout": 30},
                    timeout=35,
                )
                data = resp.json()

                updates = data.get("result", [])
                if updates:
                    print(f"[Telegram] {len(updates)} update(s) recibidos")
                for update in updates:
                    offset = update["update_id"] + 1
                    msg = update.get("message", {})
                    text = msg.get("text", "").strip()
                    chat_id = msg.get("chat", {}).get("id")
                    if not chat_id:
                        continue

                    if text.startswith("/help"):
                        await cmd_help(chat_id, client)

                    elif text.startswith("/status"):
                        await cmd_status(chat_id, client)

                    elif text.startswith("/articulos"):
                        arg = text[10:].strip().strip('"').strip("'")
                        await cmd_articulos(chat_id, arg, client)

                    elif text.startswith("/precio"):
                        q = text[7:].strip().strip('"').strip("'")
                        if q:
                            await cmd_precio(chat_id, q, client)

                    elif text.startswith("/grafico"):
                        q = text[8:].strip().strip('"').strip("'")
                        if q:
                            await cmd_grafico(chat_id, q, client)

            except Exception as e:
                print(f"[Telegram] Error: {e}")
                await asyncio.sleep(5)


# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    init_db()
    print("=== Ricardo Watcher iniciado ===")
    await asyncio.gather(
        discovery_loop(),
        watcher_loop(),
        telegram_loop(),
        daily_summary_loop(),
        return_exceptions=True,
    )


if __name__ == "__main__":
    asyncio.run(main())
