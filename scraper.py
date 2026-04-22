import re
import json
from datetime import datetime, timezone
from typing import Optional

from curl_cffi.requests import AsyncSession

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "de-CH,de;q=0.9,en;q=0.8",
}


def _decode_rsc(html: str) -> str:
    chunks = re.findall(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', html, re.DOTALL)
    full = "".join(chunks)
    try:
        return json.loads('"' + full + '"')
    except Exception:
        return full


def _extract_objects(decoded: str) -> list[dict]:
    results = []
    for m in re.finditer(r'\{"id":"(\d+)","title":', decoded):
        start = m.start()
        depth = 0
        end = start
        for i, c in enumerate(decoded[start:], start):
            if c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break
        try:
            obj = json.loads(decoded[start:end])
            if "hasBuyNow" in obj or "hasAuction" in obj:
                results.append(obj)
        except Exception:
            continue
    return results


def _seconds_remaining(end_date_str: Optional[str]) -> Optional[int]:
    if not end_date_str:
        return None
    try:
        end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        delta = (end_dt - datetime.now(timezone.utc)).total_seconds()
        return int(delta)
    except Exception:
        return None


def _parse_obj(obj: dict) -> Optional[dict]:
    lid = obj.get("id")
    if not lid:
        return None

    title = obj.get("title", "")
    has_buy_now = bool(obj.get("hasBuyNow", False))
    has_auction = bool(obj.get("hasAuction", False))
    buy_now_price = obj.get("buyNowPrice")
    bid_price = obj.get("bidPrice")
    bid_count = obj.get("numberOfBids") or 0
    secs = _seconds_remaining(obj.get("endDate"))

    # Skip listings with no remaining time or already ended
    if secs is not None and secs <= 0:
        return None

    price = bid_price if bid_price is not None else buy_now_price
    if price is None:
        return None

    return {
        "id": str(lid),
        "title": title,
        "url": f"https://www.ricardo.ch/de/a/{lid}",
        "price": float(price),
        "buy_now_price": float(buy_now_price) if buy_now_price is not None else None,
        "has_buy_now": has_buy_now,
        "has_auction": has_auction,
        "bid_count": int(bid_count),
        "seconds_remaining": secs,
    }


async def fetch_listing_status(lid: str, session: AsyncSession) -> bool | None:
    """
    Fetch the individual listing page and determine whether it's sold.
    Returns True=sold, False=still active, None=undetermined/error.
    """
    url = f"https://www.ricardo.ch/de/a/{lid}"
    try:
        resp = await session.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 404:
            return None
        if resp.status_code != 200:
            return None

        decoded = _decode_rsc(resp.text)
        combined = (decoded + resp.text).lower()

        # If any active-purchase UI is present, the listing is still live
        active_signals = ["sofort kaufen", "bieten", "in den warenkorb", "\"hasbuyno\":true", "\"hasauction\":true"]
        for s in active_signals:
            if s in combined:
                return False

        sold_signals = ["verkauft", "nicht mehr verfügbar", "article vendu", "sold"]
        for s in sold_signals:
            if s in combined:
                return True

        return None
    except Exception:
        return None


async def search_ricardo(query: str, session: AsyncSession) -> list[dict]:
    url = f"https://www.ricardo.ch/de/s/{query}/?sort=newest&type=auction"
    try:
        resp = await session.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            print(f"  [scraper] Ricardo '{query}': HTTP {resp.status_code}")
            return []
        decoded = _decode_rsc(resp.text)
        raw = _extract_objects(decoded)
        results = []
        for obj in raw:
            parsed = _parse_obj(obj)
            if parsed:
                results.append(parsed)
        return results
    except Exception as e:
        print(f"  [scraper] Error buscando '{query}': {e}")
        return []
