import asyncio
import io
import os
from datetime import datetime, timezone, timedelta
from html import escape

import httpx
from curl_cffi.requests import AsyncSession
from dotenv import load_dotenv

from classifier import classify, SEARCH_QUERIES, PRODUCT_GROUPS
from db import (
    init_db, upsert_listing, save_snapshot, mark_ending_soon_notified,
    increment_miss, get_active_listing_ids,
    get_active_unnotified_listings, get_listing, record_sale, mark_status,
    set_recheck, get_pending_rechecks, get_price_snapshots,
    get_price_stats, get_sales_for_chart, count_sales_by_category, get_counts,
    get_daily_top_sales, get_daily_top_gangas, get_article_stats,
)
from scraper import search_ricardo, fetch_listing_prices, fetch_listing_status

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

SCAN_INTERVAL   = 2 * 60   # scrape every 2 minutes
MAX_MISS        = 2        # misses before checking the listing page
ENDING_SOON_SECS = 30 * 60
DAILY_SUMMARY_HOUR_UTC = 21
DEFAULT_BID_INCREMENT = 1
VALID_BID_INCREMENTS = (1, 5, 10, 50)

_state = {
    "started_at":      None,
    "next_scan":       None,
    "last_scan":       None,
    "new_last_scan":   None,
}


def _is_next_bid_source(source: str | None) -> bool:
    return source in {
        "search_bid_price_next_bid",
        "legacy_search_bid_price",
    }


def _snap_bid_increment(raw: float) -> int | None:
    for inc in VALID_BID_INCREMENTS:
        tolerance = 0.4 if inc == 1 else 1.0
        if abs(raw - inc) <= tolerance:
            return inc
    return None


def _estimate_bid_increment(l: dict, previous) -> int:
    observed = l.get("observed_bid_price")
    bid_count = l.get("bid_count") or 0
    if observed is None or bid_count <= 0:
        return 0

    if previous and previous["observed_bid_price"] is not None:
        prev_bids = previous["bid_count"] or 0
        new_bids = bid_count - prev_bids
        delta = observed - previous["observed_bid_price"]
        if new_bids > 0 and delta > 0:
            # Example: previous next bid CHF 100 with 1 bid, now CHF 105 with
            # 2 bids means one new bid moved the next minimum by CHF 5.
            inc = _snap_bid_increment(delta / new_bids)
            if inc is not None:
                return inc

    return DEFAULT_BID_INCREMENT


def _normalize_search_prices(l: dict, previous=None):
    """Convert Ricardo search bidPrice into estimated current winning price."""
    if not _is_next_bid_source(l.get("price_source")):
        return
    if (l.get("bid_count") or 0) <= 0 or l.get("observed_bid_price") is None:
        return

    # If the bid count did not change since the previous scan, keep the
    # previous stable estimate instead of overwriting it with a fresh guess.
    if previous and previous["current_price"] is not None:
        prev_bids = previous["bid_count"] or 0
        curr_bids = l.get("bid_count") or 0
        if curr_bids == prev_bids:
            l["current_price"] = float(previous["current_price"])
            l["price"] = float(previous["current_price"])
            l["price_source"] = previous["price_source"] or l["price_source"]
            if l.get("initial_price") is None and previous["initial_price"] is not None:
                l["initial_price"] = previous["initial_price"]
                l["initial_price_source"] = previous["initial_price_source"]
            return

    inc = _estimate_bid_increment(l, previous)
    l["current_price"] = max(1.0, float(l["observed_bid_price"]) - inc)
    l["price"] = l["current_price"]
    l["price_source"] = f"estimated_next_bid_minus_{inc}"

    if (
        l.get("initial_price") is not None
        and l["current_price"] is not None
        and float(l["initial_price"]) > float(l["current_price"])
    ):
        l["initial_price"] = None
        l["initial_price_source"] = None

    if l.get("initial_price") is None and l["bid_count"] == 1:
        l["initial_price"] = l["current_price"]
        l["initial_price_source"] = l["price_source"]


def _stable_sale_price(lid: str, fallback_row) -> tuple[float, str]:
    """Pick the most reliable observed price before the listing disappeared."""
    snaps = get_price_snapshots(lid, limit=12)
    if not snaps:
        return float(fallback_row["current_price"]), fallback_row["price_source"] or "unknown"

    # A direct page scrape is always more reliable than any estimate.
    for snap in snaps:
        if snap["price_source"] == "detail_current" and snap["price"] is not None:
            return float(snap["price"]), "detail_current"

    latest_bid_count = snaps[0]["bid_count"]
    stable = snaps[0]
    for snap in snaps[1:]:
        if snap["bid_count"] != latest_bid_count:
            break
        stable = snap

    return float(stable["price"]), stable["price_source"] or "unknown"


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
    """Return classified auction listings, including zero-bid starts."""
    found: dict[str, dict] = {}
    for query in SEARCH_QUERIES:
        # newest: discover new listings; end_date: keep ending auctions visible
        for sort in ("newest", "end_date"):
            listings = await search_ricardo(query, session, sort=sort)
            for l in listings:
                if l["id"] in found:
                    continue
                match = classify(l["title"])
                if match is None:
                    continue

                detail_prices = await fetch_listing_prices(l["id"], session)
                if detail_prices["current_price"] is not None:
                    l["current_price"] = detail_prices["current_price"]
                    l["price"] = detail_prices["current_price"]
                    l["price_source"] = "detail_current"
                if detail_prices["initial_price"] is not None:
                    l["initial_price"] = detail_prices["initial_price"]
                    l["initial_price_source"] = "detail_start"
                elif l["initial_price"] is not None:
                    l["initial_price_source"] = l["price_source"]
                else:
                    l["initial_price_source"] = None

                l["category"] = match.category
                l["display"] = match.display
                l["storage"] = match.storage
                found[l["id"]] = l
            await asyncio.sleep(2)
    return found


# ── Disappearance logic ───────────────────────────────────────────────────────

async def _notify_ending_soon(l: dict):
    """Send the once-per-listing alert when an auction has less than 30 minutes left."""
    seconds = l.get("seconds_remaining")
    mins = max(0, int(seconds or 0) // 60)
    initial = l.get("initial_price")
    current = l.get("current_price")
    bids = l.get("bid_count") or 0

    initial_line = (
        f"Precio inicial: CHF {float(initial):.0f}"
        if initial is not None
        else "Precio inicial: no disponible"
    )
    current_line = (
        f"Precio actual:  <b>CHF {float(current):.0f}</b>"
        if current is not None
        else "Precio actual: no disponible"
    )

    msg = (
        f"⏳ <b>SUBASTA &lt;30 MIN</b>\n"
        f"{escape(l['title'])}\n\n"
        f"Quedan: {mins} min\n"
        f"{initial_line}\n"
        f"{current_line}\n"
        f"Pujas: {bids}\n"
        f"{l['url']}"
    )

    try:
        async with httpx.AsyncClient() as client:
            await tg_send(msg, client)
        mark_ending_soon_notified(l["id"])
    except Exception as e:
        print(f"  [tg] Error enviando aviso <30 min: {e}")


def _effective_seconds_remaining(row) -> int | None:
    if row["seconds_remaining"] is None:
        return None
    try:
        last_seen = datetime.fromisoformat(row["last_seen"])
    except Exception:
        return row["seconds_remaining"]
    elapsed = (datetime.now(timezone.utc) - last_seen).total_seconds()
    return int(row["seconds_remaining"] - elapsed)


def _row_to_listing_dict(row, seconds_remaining: int | None = None) -> dict:
    return {
        "id": row["id"],
        "title": row["title"],
        "url": row["url"],
        "initial_price": row["initial_price"],
        "current_price": row["current_price"],
        "bid_count": row["bid_count"],
        "seconds_remaining": seconds_remaining,
    }


async def _notify_sale(lid: str, row, sale_type: str, final_price: float):
    """Send Telegram notification for a confirmed sale."""
    cat_avg = None
    try:
        from db import get_article_stats
        stats = get_article_stats(f"%{row['category']}%")
        if stats:
            cat_avg = stats["avg_final"]
    except Exception:
        pass

    tipo = "subasta" if sale_type == "auction" else "buy now"
    initial = float(row["initial_price"]) if row["initial_price"] is not None else None
    bids = row["bid_count"] or 0
    if initial is not None:
        uplift = final_price - initial
        uplift_str = f"+CHF {uplift:.0f}" if uplift >= 0 else f"-CHF {abs(uplift):.0f}"
        initial_line = f"Precio inicial: CHF {initial:.0f}\n"
        uplift_part = f" ({uplift_str}, {bids} puja{'s' if bids != 1 else ''})"
    else:
        initial_line = "Precio inicial: no disponible\n"
        uplift_part = f" ({bids} puja{'s' if bids != 1 else ''})"

    msg = (
        f"💰 <b>VENDIDO ({tipo})</b>\n"
        f"{escape(row['title'])}\n\n"
        f"{initial_line}"
        f"Precio final:   <b>CHF {final_price:.0f}</b>{uplift_part}"
    )
    if cat_avg:
        diff = final_price - cat_avg
        sign = "+" if diff >= 0 else ""
        msg += f"\nMedia categoría: CHF {cat_avg:.0f} ({sign}{diff:.0f})"
    if row["price_source"] and row["price_source"] != "detail_current":
        msg += "\nNota: precio final estimado desde busqueda de Ricardo"
    msg += f"\n{row['url']}"

    try:
        async with httpx.AsyncClient() as client:
            await tg_send(msg, client)
    except Exception as e:
        print(f"  [tg] Error enviando notificación: {e}")


def infer_sale_type(row) -> tuple[str, float, str] | None:
    """Return (sale_type, final_price) or None if no reliable price.
    We only track listings with ≥1 bid, so bid_count should always be > 0 here.
    current_price is refreshed from the listing detail page during scans when available,
    but the final sale should use the last stable snapshot to avoid a noisy
    next-bid estimate from overwriting a better value.
    """
    if (row["bid_count"] or 0) > 0:
        final_price, source = _stable_sale_price(row["id"], row)
        return "auction", final_price, source
    return None


async def _record_ended_listing(lid: str, row, label: str, session=None):
    result = infer_sale_type(row)
    if result is None:
        mark_status(lid, "expired_no_sale")
        print(f"  ⚠ Sin precio fiable{label}: {row['title']}")
        return

    sale_type, final_price, final_price_source = result

    # Re-fetch the listing page to get the real closing price, which may differ
    # from snapshots if the listing fell off search results before the last bids.
    if session is not None:
        try:
            detail = await fetch_listing_prices(lid, session)
            if detail["current_price"] is not None:
                final_price = detail["current_price"]
                final_price_source = "detail_current"
        except Exception:
            pass

    record_sale(
        lid, row["title"], row["url"], row["category"], row["storage"],
        row["initial_price"], final_price, sale_type, row["bid_count"],
        final_price_source, row["initial_price_source"],
    )
    print(f"  ✅ VENDIDO{label}: {row['title']} CHF {final_price}")
    await _notify_sale(lid, row, sale_type, final_price)


# ── Main scan loop (every 2 min) ─────────────────────────────────────────────

async def scan_loop():
    _state["started_at"] = datetime.now(timezone.utc)
    print("[Scan] Iniciado — scrapeando cada 2 min buscando subastas con pujas")

    async with AsyncSession(impersonate="chrome124") as session:
        while True:
            try:
                _state["last_scan"] = datetime.now(timezone.utc)
                ts = datetime.now().strftime("%H:%M:%S")

                current = await scrape_all(session)
                active_ids = get_active_listing_ids()
                new_count = 0

                for lid, l in current.items():
                    is_new = lid not in active_ids
                    previous = None if is_new else get_listing(lid)
                    _normalize_search_prices(l, previous)
                    already_alerted = bool(previous["ending_soon_notified"]) if previous else False
                    if is_new:
                        new_count += 1
                        mins = (l["seconds_remaining"] or 0) // 60
                        bid_label = "pujas" if l["bid_count"] != 1 else "puja"
                        print(f"  ➕ {ts} Nuevo: {l['title'][:50]} — CHF {l['current_price']:.0f} ({l['bid_count']} {bid_label}, {mins} min)")
                    upsert_listing(
                        lid, l["title"], l["url"], l["category"], l["storage"],
                        l["initial_price"], l["current_price"], l["buy_now_price"],
                        l["has_buy_now"], l["bid_count"], l["seconds_remaining"],
                        l.get("observed_bid_price"), l.get("price_source"),
                        l.get("initial_price_source"),
                    )
                    save_snapshot(
                        lid, l["current_price"], l["bid_count"], l["seconds_remaining"],
                        l.get("observed_bid_price"), l.get("price_source"),
                    )
                    if (
                        not already_alerted
                        and l.get("seconds_remaining") is not None
                        and l["seconds_remaining"] <= ENDING_SOON_SECS
                    ):
                        await _notify_ending_soon(l)

                _state["new_last_scan"] = new_count

                for row in get_active_unnotified_listings():
                    remaining = _effective_seconds_remaining(row)
                    if remaining is not None and remaining <= ENDING_SOON_SECS:
                        await _notify_ending_soon(_row_to_listing_dict(row, remaining))

                # Listings that disappeared from search results
                disappeared = active_ids - set(current.keys())
                for lid in disappeared:
                    row = get_listing(lid)
                    if not row:
                        continue

                    misses = increment_miss(lid)
                    if misses < MAX_MISS:
                        continue

                    row = get_listing(lid)
                    page_status = await fetch_listing_status(lid, session)
                    remaining = _effective_seconds_remaining(row)

                    if page_status is True:
                        await _record_ended_listing(lid, row, "", session)

                    elif page_status is False:
                        if remaining is not None and remaining <= 0:
                            await _record_ended_listing(lid, row, " (tiempo agotado)", session)
                        else:
                            print(f"  ↩ Sigue activo (paginación): {row['title'][:50]}")

                    else:
                        set_recheck(lid, 15)
                        print(f"  ⏳ Recheck en 15 min: {row['title'][:50]}")

            except Exception as e:
                print(f"[Scan] Error: {e}")

            _state["next_scan"] = datetime.now(timezone.utc) + timedelta(seconds=SCAN_INTERVAL)
            await asyncio.sleep(SCAN_INTERVAL)


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
        "<b>/stats &lt;articulo&gt;</b>\n"
        "  Estadisticas de ventas: precio medio de inicio y venta, precio maximo de compra\n"
        "  para revender con +20% (tras comision 12%), y casos extremos de precio.\n"
        "  Ejemplo: /stats ps5  /stats iphone  /stats switch\n\n"
        "<b>/backup</b>\n"
        "  Hace un backup inmediato de la base de datos y lo sube a Google Drive.\n\n"
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

    next_s = _state["next_scan"]
    if next_s:
        secs_left = int((next_s - now).total_seconds())
        next_str = f"en {max(0, secs_left)}s" if secs_left > 0 else "ahora"
    else:
        next_str = "iniciando..."

    new_count = _state["new_last_scan"]
    new_str = f"<b>{new_count}</b>" if new_count is not None else "?"

    text = (
        f"<b>Ricardo Watcher activo</b>\n\n"
        f"Uptime: {uptime}\n"
        f"Proximo scan: {next_str} (cada 2 min)\n\n"
        f"Subastas seguidas: <b>{counts['active']}</b>\n"
        f"Nuevas ultimo scan: {new_str}\n"
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
    initials = [r["initial_price"] for r in rows if r["initial_price"] is not None]
    auctions = [r for r in rows if r["sale_type"] == "auction"]
    buyNows = [r for r in rows if r["sale_type"] == "buy_now"]

    avg_f = sum(finals) / len(finals)
    avg_i = (sum(initials) / len(initials)) if initials else None

    text = (
        f"📊 <b>{query}</b> — últimos 30 días\n\n"
        f"Ventas registradas: <b>{len(rows)}</b> "
        f"({len(auctions)} subasta, {len(buyNows)} buy now)\n\n"
        f"Precio final\n"
        f"  Promedio: <b>CHF {avg_f:.0f}</b>\n"
        f"  Mínimo:   CHF {min(finals):.0f}\n"
        f"  Máximo:   CHF {max(finals):.0f}\n\n"
        f"Precio inicial promedio: {'CHF %.0f' % avg_i if avg_i is not None else 'no disponible'}"
    )

    await client.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
        timeout=15,
    )


async def cmd_backup(chat_id: int, client: httpx.AsyncClient):
    await tg_send("⏳ Haciendo backup...", client)
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backup.sh")
    proc = await asyncio.create_subprocess_exec(
        "bash", script,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    stdout, _ = await proc.communicate()
    output = stdout.decode().strip()
    if proc.returncode == 0:
        await tg_send(f"✅ {output}", client)
    else:
        await tg_send(f"❌ Backup fallido:\n{output}", client)


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


async def cmd_stats(chat_id: int, query: str, client: httpx.AsyncClient):
    if not query:
        await client.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": chat_id,
                  "text": "Uso: /stats &lt;articulo&gt;\nEjemplo: /stats ps5",
                  "parse_mode": "HTML"},
            timeout=15,
        )
        return

    pattern = f"%{query.lower().replace(' ', '_')}%"
    s = get_article_stats(pattern)

    if not s:
        await client.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": chat_id,
                  "text": f"Sin ventas registradas para <b>{query}</b>.",
                  "parse_mode": "HTML"},
            timeout=15,
        )
        return

    lines = [f"📊 <b>Stats: {query}</b>\n",
             f"Ventas registradas: <b>{s['n']}</b>\n",
             f"Inicio subasta (media): {'CHF %.0f' % s['avg_initial'] if s['avg_initial'] is not None else 'no disponible'}",
             f"Precio venta (media):   CHF {s['avg_final']:.0f}",
             f"  Mínimo: CHF {s['min_final']:.0f}  |  Máximo: CHF {s['max_final']:.0f}\n",
             f"💰 <b>Precio máx. de compra</b> (revender con +20% tras comisión 12%)",
             f"  → <b>CHF {s['max_buy']:.0f}</b>"]

    if s["extremes"] and s["n"] >= 10:
        lines.append("\n⚠️ <b>Casos extremos</b> (±30% de la media)")
        avg = s["avg_final_ref"]
        for r in s["extremes"]:
            pct = (r["final_price"] - avg) / avg * 100
            icon = "📉" if pct < 0 else "📈"
            title = r["title"][:45] + "…" if len(r["title"]) > 45 else r["title"]
            date = r["sold_at"][:10]
            lines.append(f"  {icon} CHF {r['final_price']:.0f} ({pct:+.0f}%) — {title} [{date}]")

    await client.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        json={"chat_id": chat_id, "text": "\n".join(lines), "parse_mode": "HTML"},
        timeout=15,
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

                    elif text.startswith("/backup"):
                        await cmd_backup(chat_id, client)

                    elif text.startswith("/stats"):
                        q = text[6:].strip().strip('"').strip("'")
                        await cmd_stats(chat_id, q, client)

            except Exception as e:
                print(f"[Telegram] Error: {e}")
                await asyncio.sleep(5)


# ── Recheck loop (every 5 min, resolves pending_recheck listings) ─────────────

async def recheck_loop():
    await asyncio.sleep(30)
    print("[Recheck] Iniciado")
    async with AsyncSession(impersonate="chrome124") as session:
        while True:
            pending = get_pending_rechecks()
            for row in pending:
                lid = row["id"]
                page_status = await fetch_listing_status(lid, session)
                if page_status is True:
                    await _record_ended_listing(lid, row, " (recheck)", session)
                elif page_status is False:
                    remaining = _effective_seconds_remaining(row)
                    if remaining is not None and remaining <= 0:
                        await _record_ended_listing(lid, row, " (recheck, tiempo agotado)", session)
                    else:
                        mark_status(lid, "active")
                        print(f"  ↩ Activo (recheck): {row['title']}")
                else:
                    mark_status(lid, "expired_no_sale")
                    print(f"  ❌ EXPIRADO sin venta (recheck): {row['title']}")
            await asyncio.sleep(5 * 60)


# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    init_db()
    print("=== Ricardo Watcher iniciado ===")
    await asyncio.gather(
        scan_loop(),
        recheck_loop(),
        telegram_loop(),
        daily_summary_loop(),
        return_exceptions=True,
    )


if __name__ == "__main__":
    asyncio.run(main())
