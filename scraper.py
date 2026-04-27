import re
import json
from html import unescape
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


def _parse_chf(value: str) -> Optional[float]:
    value = value.strip().replace("'", "").replace("’", "")
    if "," in value and "." in value:
        value = value.replace(".", "").replace(",", ".")
    elif "." in value and len(value.rsplit(".", 1)[1]) == 3:
        value = value.replace(".", "")
    else:
        value = value.replace(",", ".")
    try:
        return float(value)
    except ValueError:
        return None


def _extract_detail_prices(html: str) -> dict[str, Optional[float]]:
    text = unescape(html)
    current_price = None
    initial_price = None

    offer = re.search(
        r'"@type"\s*:\s*"Offer".{0,2500}?"price"\s*:\s*"?([0-9][0-9\'’.,]*)"?',
        text,
        re.DOTALL,
    )
    if offer:
        current_price = _parse_chf(offer.group(1))

    start = re.search(
        r"(?:Startpreis|Prix de départ|Prezzo di partenza)\s*:\s*CHF\s*([0-9][0-9'’.,]*)",
        text,
        re.IGNORECASE,
    )
    if start:
        initial_price = _parse_chf(start.group(1))

    return {"current_price": current_price, "initial_price": initial_price}


def _parse_obj(obj: dict) -> Optional[dict]:
    lid = obj.get("id")
    if not lid:
        return None

    title       = obj.get("title", "")
    has_buy_now = bool(obj.get("hasBuyNow", False))
    has_auction = bool(obj.get("hasAuction", False))
    buy_now_price = obj.get("buyNowPrice")
    bid_price   = obj.get("bidPrice")
    bid_count   = int(obj.get("numberOfBids") or obj.get("bidsCount") or 0)
    secs        = _seconds_remaining(obj.get("endDate"))
    condition   = obj.get("conditionKey") or ""

    # Extract city from first shipping option
    city = ""
    shipping = obj.get("shipping") or []
    if shipping and isinstance(shipping, list):
        city = shipping[0].get("city", "")

    # Only track auction listings (with or without buy-now option)
    if not has_auction:
        return None

    # Skip listings already ended
    if secs is not None and secs <= 0:
        return None

    if bid_price is not None:
        # Search results expose bidPrice. For active auctions with bids this is
        # usually the next minimum bid, not the current winning price. scrape_all
        # refines it from the listing detail page before persisting.
        price         = float(bid_price)
        current_price = float(bid_price)
        initial_price = float(bid_price) if bid_count == 0 else None
        price_source  = "search_start_exact" if bid_count == 0 else "search_bid_price_next_bid"
    elif buy_now_price is not None:
        current_price = float(buy_now_price)
        initial_price = float(buy_now_price)
        price         = float(buy_now_price)
        price_source  = "buy_now"
    else:
        return None

    return {
        "id":            str(lid),
        "title":         title,
        "url":           f"https://www.ricardo.ch/de/a/{lid}",
        "price":         price,
        "current_price": current_price,
        "initial_price": initial_price,
        "observed_bid_price": float(bid_price) if bid_price is not None else None,
        "price_source":  price_source,
        "buy_now_price": float(buy_now_price) if buy_now_price is not None else None,
        "has_buy_now":   has_buy_now,
        "has_auction":   has_auction,
        "bid_count":     bid_count,
        "seconds_remaining": secs,
        "condition":     condition,
        "city":          city,
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
            return False  # 403/5xx = can't verify, assume still active

        decoded = _decode_rsc(resp.text)
        combined = (decoded + resp.text).lower()

        # "verkauft" appears on every Ricardo page (site slogan) — too generic.
        # Only use specific sold-state strings that appear exclusively on ended listings.
        sold_signals = [
            "nicht mehr verfügbar",   # "no longer available" — appears on closed listings
            "article vendu",          # French sold page
            "dieses angebot ist abgelaufen",  # "this offer has expired"
            "das angebot wurde beendet",      # "the offer has ended"
        ]
        for s in sold_signals:
            if s in combined:
                return True

        # HTTP 200 with no sold signals = still active
        return False
    except Exception:
        return False  # network error, assume still active


async def fetch_listing_prices(lid: str, session: AsyncSession) -> dict[str, Optional[float]]:
    """Fetch a listing page and return exact current/start prices when available."""
    url = f"https://www.ricardo.ch/de/a/{lid}"
    try:
        resp = await session.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return {"current_price": None, "initial_price": None}
        return _extract_detail_prices(resp.text)
    except Exception:
        return {"current_price": None, "initial_price": None}


async def search_ricardo(query: str, session: AsyncSession, sort: str = "newest") -> list[dict]:
    url = f"https://www.ricardo.ch/de/s/{query}/?sort={sort}&type=auction"
    try:
        resp = await session.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            print(f"  [scraper] Ricardo '{query}' (sort={sort}): HTTP {resp.status_code}")
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
        print(f"  [scraper] Error buscando '{query}' (sort={sort}): {e}")
        return []
