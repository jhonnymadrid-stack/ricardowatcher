import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

DB_PATH = Path(__file__).parent / "precios.db"


def _conn():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def init_db():
    with _conn() as c:
        c.executescript("""
            CREATE TABLE IF NOT EXISTS listings (
                id               TEXT PRIMARY KEY,
                title            TEXT,
                url              TEXT,
                category         TEXT,
                storage          TEXT,
                initial_price    REAL,
                current_price    REAL,
                buy_now_price    REAL,
                has_buy_now      INTEGER DEFAULT 0,
                bid_count        INTEGER DEFAULT 0,
                seconds_remaining INTEGER,
                is_critical      INTEGER DEFAULT 0,
                miss_count       INTEGER DEFAULT 0,
                first_seen       TEXT,
                last_seen        TEXT,
                status           TEXT DEFAULT 'active'
            );

            CREATE TABLE IF NOT EXISTS sales (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                listing_id    TEXT,
                title         TEXT,
                url           TEXT,
                category      TEXT,
                storage       TEXT,
                initial_price REAL,
                final_price   REAL,
                sale_type     TEXT,
                bid_count     INTEGER,
                sold_at       TEXT
            );

            CREATE TABLE IF NOT EXISTS price_snapshots (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                listing_id        TEXT,
                price             REAL,
                bid_count         INTEGER,
                seconds_remaining INTEGER,
                ts                TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_listings_status   ON listings (status);
            CREATE INDEX IF NOT EXISTS idx_listings_critical ON listings (is_critical, status);
            CREATE INDEX IF NOT EXISTS idx_sales_category    ON sales (category);
            CREATE INDEX IF NOT EXISTS idx_snapshots_lid     ON price_snapshots (listing_id);
        """)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def upsert_listing(lid, title, url, category, storage,
                   price, buy_now_price, has_buy_now, bid_count, seconds_remaining):
    ts = now_iso()
    with _conn() as c:
        existing = c.execute("SELECT id FROM listings WHERE id=?", (lid,)).fetchone()
        if existing:
            c.execute("""
                UPDATE listings
                SET title=?, current_price=?, buy_now_price=?, has_buy_now=?,
                    bid_count=?, seconds_remaining=?, last_seen=?, miss_count=0,
                    status='active'
                WHERE id=?
            """, (title, price, buy_now_price, int(has_buy_now),
                  bid_count, seconds_remaining, ts, lid))
        else:
            c.execute("""
                INSERT INTO listings
                    (id, title, url, category, storage, initial_price, current_price,
                     buy_now_price, has_buy_now, bid_count, seconds_remaining,
                     first_seen, last_seen)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (lid, title, url, category, storage,
                  price, price, buy_now_price, int(has_buy_now),
                  bid_count, seconds_remaining, ts, ts))


def save_snapshot(lid, price, bid_count, seconds_remaining):
    with _conn() as c:
        c.execute("""
            INSERT INTO price_snapshots (listing_id, price, bid_count, seconds_remaining, ts)
            VALUES (?,?,?,?,?)
        """, (lid, price, bid_count, seconds_remaining, now_iso()))


def mark_critical(lid):
    with _conn() as c:
        c.execute("UPDATE listings SET is_critical=1 WHERE id=?", (lid,))


def increment_miss(lid) -> int:
    """Increment miss counter; returns new value."""
    with _conn() as c:
        c.execute("UPDATE listings SET miss_count = miss_count + 1 WHERE id=?", (lid,))
        row = c.execute("SELECT miss_count FROM listings WHERE id=?", (lid,)).fetchone()
        return row["miss_count"] if row else 0


def get_critical_listings() -> list:
    with _conn() as c:
        return c.execute(
            "SELECT * FROM listings WHERE is_critical=1 AND status='active'"
        ).fetchall()


def get_active_listing_ids() -> set:
    with _conn() as c:
        rows = c.execute("SELECT id FROM listings WHERE status='active'").fetchall()
        return {r["id"] for r in rows}


def get_listing(lid) -> Optional[sqlite3.Row]:
    with _conn() as c:
        return c.execute("SELECT * FROM listings WHERE id=?", (lid,)).fetchone()


def record_sale(lid, title, url, category, storage,
                initial_price, final_price, sale_type, bid_count):
    with _conn() as c:
        c.execute("""
            INSERT INTO sales
                (listing_id, title, url, category, storage,
                 initial_price, final_price, sale_type, bid_count, sold_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (lid, title, url, category, storage,
              initial_price, final_price, sale_type, bid_count, now_iso()))
        c.execute("UPDATE listings SET status='sold' WHERE id=?", (lid,))


def mark_status(lid, status: str):
    with _conn() as c:
        c.execute("UPDATE listings SET status=? WHERE id=?", (status, lid))


def get_price_stats(pattern: str, days: int = 30) -> list:
    with _conn() as c:
        return c.execute("""
            SELECT final_price, initial_price, sale_type, bid_count, sold_at, title
            FROM sales
            WHERE category LIKE ?
              AND sold_at >= datetime('now', ? || ' days')
            ORDER BY sold_at ASC
        """, (pattern, f"-{days}")).fetchall()


def get_sales_for_chart(pattern: str, days: int = 90) -> list:
    with _conn() as c:
        return c.execute("""
            SELECT final_price, sold_at
            FROM sales
            WHERE category LIKE ?
              AND sold_at >= datetime('now', ? || ' days')
            ORDER BY sold_at ASC
        """, (pattern, f"-{days}")).fetchall()


def get_counts() -> dict:
    with _conn() as c:
        active = c.execute("SELECT COUNT(*) FROM listings WHERE status='active'").fetchone()[0]
        critical = c.execute("SELECT COUNT(*) FROM listings WHERE is_critical=1 AND status='active'").fetchone()[0]
        sales = c.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
    return {"active": active, "critical": critical, "sales": sales}


def get_daily_top_sales(limit: int = 3) -> list:
    """Top sales by final_price today (UTC)."""
    with _conn() as c:
        return c.execute("""
            SELECT title, url, category, final_price, sale_type
            FROM sales
            WHERE sold_at >= date('now', 'start of day')
            ORDER BY final_price DESC
            LIMIT ?
        """, (limit,)).fetchall()


def get_daily_top_gangas(limit: int = 3) -> list:
    """Today's sales with biggest CHF saving vs 30-day category average (min 2 historical sales)."""
    with _conn() as c:
        return c.execute("""
            SELECT
                s.title, s.url, s.final_price,
                ROUND(hist.cat_avg, 0) AS cat_avg,
                ROUND(hist.cat_avg - s.final_price, 0) AS saving
            FROM sales s
            JOIN (
                SELECT category, AVG(final_price) AS cat_avg
                FROM sales
                WHERE sold_at < date('now', 'start of day')
                  AND sold_at >= date('now', '-30 days')
                GROUP BY category
                HAVING COUNT(*) >= 2
            ) hist ON hist.category = s.category
            WHERE s.sold_at >= date('now', 'start of day')
              AND hist.cat_avg > s.final_price
            ORDER BY saving DESC
            LIMIT ?
        """, (limit,)).fetchall()


def count_sales_by_category(pattern: str) -> int:
    with _conn() as c:
        row = c.execute(
            "SELECT COUNT(*) as n FROM sales WHERE category LIKE ?", (pattern,)
        ).fetchone()
        return row["n"] if row else 0
