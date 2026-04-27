"""Fix incorrectly estimated sale prices and send Telegram corrections."""
import asyncio
import sqlite3
import httpx

BOT_TOKEN = "8618421025:AAElpCoqZyKw_J2Ry9d-Y8J2SNC2Ro0ifbQ"
CHAT_ID = "-1003741124170"
DB_PATH = "/home/jhonnyvb8/ricardowatcher/precios.db"

corrections = [
    {
        "id": 25,
        "listing_id": "1316619614",
        "title": "IPhone 12 mini Red 256gb 🔋100%",
        "url": "https://www.ricardo.ch/de/a/1316619614",
        "old_final": 174.0,
        "new_final": 170.0,
        "update_initial": True,   # bid_count=1 → initial=final
        "old_initial": 174.0,
        "new_initial": 170.0,
        "bid_count": 1,
        "source": "corrected_user_reported",
    },
    {
        "id": 21,
        "listing_id": "1316936032",
        "title": "Apple Mac Mini 2018 Intel i5 / 16 GB RAM / 512 GB SSD",
        "url": "https://www.ricardo.ch/de/a/1316936032",
        "old_final": 179.0,
        "new_final": 195.0,
        "update_initial": False,  # real starting price unknown (bot lost track)
        "old_initial": 179.0,
        "new_initial": None,
        "bid_count": 1,
        "source": "corrected_user_reported",
    },
    {
        "id": 24,
        "listing_id": "1316791094",
        "title": "Nintendo Gameboy Pocket Silver + Super Mario Land!",
        "url": "https://www.ricardo.ch/de/a/1316791094",
        "old_final": 90.0,
        "new_final": 86.0,
        "update_initial": False,  # initial_price=1 CHF (starting bid) is correct
        "old_initial": 1.0,
        "new_initial": None,
        "bid_count": 28,
        "source": "detail_current",
    },
]


def apply_db_fixes():
    conn = sqlite3.connect(DB_PATH)
    for c in corrections:
        if c["update_initial"]:
            conn.execute(
                "UPDATE sales SET final_price=?, initial_price=?, "
                "final_price_source=?, initial_price_source=? WHERE id=?",
                (c["new_final"], c["new_initial"],
                 c["source"], c["source"], c["id"]),
            )
        else:
            conn.execute(
                "UPDATE sales SET final_price=?, final_price_source=? WHERE id=?",
                (c["new_final"], c["source"], c["id"]),
            )
        print(f"  DB fix #{c['id']}: CHF {c['old_final']:.0f} → {c['new_final']:.0f}")
    conn.commit()
    conn.close()


async def send_telegram_corrections():
    async with httpx.AsyncClient() as client:
        for c in corrections:
            diff = c["new_final"] - c["old_final"]
            sign = "+" if diff >= 0 else ""
            pujas = f"{c['bid_count']} puja{'s' if c['bid_count'] != 1 else ''}"
            msg = (
                f"✏️ <b>CORRECCIÓN DE PRECIO</b>\n"
                f"{c['title']}\n\n"
                f"Registrado: CHF {c['old_final']:.0f}\n"
                f"Correcto:   <b>CHF {c['new_final']:.0f}</b> ({sign}{diff:.0f} CHF, {pujas})\n"
                f"Fuente: {c['source']}\n"
                f"{c['url']}"
            )
            resp = await client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
                timeout=15,
            )
            print(f"  Telegram #{c['id']}: {resp.status_code}")


if __name__ == "__main__":
    print("Aplicando correcciones en DB...")
    apply_db_fixes()
    print("Enviando notificaciones Telegram...")
    asyncio.run(send_telegram_corrections())
    print("Hecho.")
