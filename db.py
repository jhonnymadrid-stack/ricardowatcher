import sqlite3
from datetime import datetime, timezone, timedelta
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
        # Migrations
        cols = {r[1] for r in c.execute("PRAGMA table_info(listings)").fetchall()}
        if "recheck_after" not in cols:
            c.execute("ALTER TABLE listings ADD COLUMN recheck_after TEXT DEFAULT NULL")
        if "observed_bid_price" not in cols:
            c.execute("ALTER TABLE listings ADD COLUMN observed_bid_price REAL DEFAULT NULL")
        if "price_source" not in cols:
            c.execute("ALTER TABLE listings ADD COLUMN price_source TEXT DEFAULT NULL")
        if "initial_price_source" not in cols:
            c.execute("ALTER TABLE listings ADD COLUMN initial_price_source TEXT DEFAULT NULL")
        if "ending_soon_notified" not in cols:
            c.execute("ALTER TABLE listings ADD COLUMN ending_soon_notified INTEGER DEFAULT 0")

        sale_cols = {r[1] for r in c.execute("PRAGMA table_info(sales)").fetchall()}
        if "final_price_source" not in sale_cols:
            c.execute("ALTER TABLE sales ADD COLUMN final_price_source TEXT DEFAULT NULL")
        if "initial_price_source" not in sale_cols:
            c.execute("ALTER TABLE sales ADD COLUMN initial_price_source TEXT DEFAULT NULL")

        snap_cols = {r[1] for r in c.execute("PRAGMA table_info(price_snapshots)").fetchall()}
        if "observed_bid_price" not in snap_cols:
            c.execute("ALTER TABLE price_snapshots ADD COLUMN observed_bid_price REAL DEFAULT NULL")
        if "price_source" not in snap_cols:
            c.execute("ALTER TABLE price_snapshots ADD COLUMN price_source TEXT DEFAULT NULL")

        c.executescript("""
            CREATE VIEW IF NOT EXISTS article_stats AS
            SELECT
                category,
                COUNT(*)                                        AS n_sales,
                ROUND(AVG(initial_price), 0)                   AS avg_initial,
                ROUND(AVG(final_price),   0)                   AS avg_final,
                ROUND(MIN(final_price),   0)                   AS min_final,
                ROUND(MAX(final_price),   0)                   AS max_final,
                ROUND(AVG(final_price) * 0.88 / 1.20, 0)      AS max_buy_price
            FROM sales
            GROUP BY category
            ORDER BY n_sales DESC;

            CREATE VIEW IF NOT EXISTS article_extremes AS
            WITH avg_by_cat AS (
                SELECT category, AVG(final_price) AS avg_final
                FROM sales
                GROUP BY category
                HAVING COUNT(*) >= 10
            )
            SELECT
                s.category,
                s.title,
                ROUND(s.final_price, 0)                                        AS final_price,
                ROUND(a.avg_final,   0)                                        AS cat_avg,
                ROUND((s.final_price - a.avg_final) / a.avg_final * 100, 1)   AS deviation_pct,
                s.sold_at
            FROM sales s
            JOIN avg_by_cat a ON a.category = s.category
            WHERE ABS(s.final_price - a.avg_final) / a.avg_final > 0.30
            ORDER BY s.category, s.final_price ASC;
        """)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def upsert_listing(lid, title, url, category, storage,
                   initial_price, current_price, buy_now_price,
                   has_buy_now, bid_count, seconds_remaining,
                   observed_bid_price=None, price_source=None,
                   initial_price_source=None):
    # current_price is the current winning price when the detail page was
    # available. Search-result bidPrice is only used as a fallback.
    ts = now_iso()
    with _conn() as c:
        existing = c.execute(
            "SELECT initial_price, initial_price_source "
            "FROM listings WHERE id=?", (lid,)
        ).fetchone()
        if existing:
            actual_initial = initial_price
            actual_initial_source = initial_price_source
            if actual_initial is None:
                actual_initial = existing["initial_price"]
                actual_initial_source = existing["initial_price_source"]
            if actual_initial is not None and actual_initial < 0:
                actual_initial = None
                actual_initial_source = None

            c.execute("""
                UPDATE listings
                SET title=?, initial_price=?, current_price=?, buy_now_price=?, has_buy_now=?,
                    bid_count=?, seconds_remaining=?, last_seen=?, miss_count=0,
                    status='active', recheck_after=NULL, observed_bid_price=?,
                    price_source=?, initial_price_source=?
                WHERE id=?
            """, (title, actual_initial, current_price, buy_now_price, int(has_buy_now),
                  bid_count, seconds_remaining, ts, observed_bid_price,
                  price_source, actual_initial_source, lid))
        else:
            c.execute("""
                INSERT INTO listings
                    (id, title, url, category, storage, initial_price, current_price,
                     buy_now_price, has_buy_now, bid_count, seconds_remaining,
                     first_seen, last_seen, observed_bid_price, price_source,
                     initial_price_source)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (lid, title, url, category, storage,
                  initial_price, current_price, buy_now_price, int(has_buy_now),
                  bid_count, seconds_remaining, ts, ts, observed_bid_price,
                  price_source, initial_price_source))


def save_snapshot(lid, price, bid_count, seconds_remaining,
                  observed_bid_price=None, price_source=None):
    with _conn() as c:
        c.execute("""
            INSERT INTO price_snapshots
                (listing_id, price, bid_count, seconds_remaining, ts,
                 observed_bid_price, price_source)
            VALUES (?,?,?,?,?,?,?)
        """, (lid, price, bid_count, seconds_remaining, now_iso(),
              observed_bid_price, price_source))


def mark_critical(lid):
    with _conn() as c:
        c.execute("UPDATE listings SET is_critical=1 WHERE id=?", (lid,))


def mark_ending_soon_notified(lid):
    with _conn() as c:
        c.execute(
            "UPDATE listings SET is_critical=1, ending_soon_notified=1 WHERE id=?",
            (lid,),
        )


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


def get_active_unnotified_listings() -> list:
    with _conn() as c:
        return c.execute(
            "SELECT * FROM listings WHERE status='active' AND COALESCE(ending_soon_notified, 0)=0"
        ).fetchall()


def get_listing(lid) -> Optional[sqlite3.Row]:
    with _conn() as c:
        return c.execute("SELECT * FROM listings WHERE id=?", (lid,)).fetchone()


def record_sale(lid, title, url, category, storage,
                initial_price, final_price, sale_type, bid_count,
                final_price_source=None, initial_price_source=None):
    with _conn() as c:
        c.execute("""
            INSERT INTO sales
                (listing_id, title, url, category, storage,
                 initial_price, final_price, sale_type, bid_count, sold_at,
                 final_price_source, initial_price_source)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (lid, title, url, category, storage,
              initial_price, final_price, sale_type, bid_count, now_iso(),
              final_price_source, initial_price_source))
        c.execute("UPDATE listings SET status='sold' WHERE id=?", (lid,))


def mark_status(lid, status: str):
    with _conn() as c:
        c.execute("UPDATE listings SET status=? WHERE id=?", (status, lid))


def set_recheck(lid, minutes: int = 15):
    ts = (datetime.now(timezone.utc) + timedelta(minutes=minutes)).isoformat()
    with _conn() as c:
        c.execute(
            "UPDATE listings SET status='pending_recheck', recheck_after=? WHERE id=?",
            (ts, lid),
        )


def get_pending_rechecks() -> list:
    with _conn() as c:
        return c.execute(
            "SELECT * FROM listings WHERE status='pending_recheck' AND recheck_after <= ?",
            (now_iso(),),
        ).fetchall()


def get_price_snapshots(lid: str, limit: int = 12) -> list:
    with _conn() as c:
        return c.execute(
            """
            SELECT price, bid_count, seconds_remaining, ts, observed_bid_price, price_source
            FROM price_snapshots
            WHERE listing_id=?
            ORDER BY ts DESC, id DESC
            LIMIT ?
            """,
            (lid, limit),
        ).fetchall()


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


def get_article_stats(pattern: str) -> dict | None:
    with _conn() as c:
        rows = c.execute("""
            SELECT final_price, initial_price, sale_type, title, sold_at
            FROM sales
            WHERE category LIKE ?
            ORDER BY final_price ASC
        """, (pattern,)).fetchall()

    if not rows:
        return None

    finals   = [r["final_price"]   for r in rows]
    initials = [r["initial_price"] for r in rows if r["initial_price"] is not None]
    n = len(rows)

    avg_final   = sum(finals)   / n
    avg_initial = (sum(initials) / len(initials)) if initials else None
    max_buy     = avg_final * 0.88 / 1.20

    THRESHOLD = 0.30
    low  = [r for r in rows if r["final_price"] < avg_final * (1 - THRESHOLD)]
    high = [r for r in rows if r["final_price"] > avg_final * (1 + THRESHOLD)]

    # up to 2 most extreme low + 1 most extreme high
    extremes = low[:2] + (sorted(high, key=lambda r: r["final_price"], reverse=True)[:1])

    return {
        "n":           n,
        "avg_initial": avg_initial,
        "avg_final":   avg_final,
        "min_final":   min(finals),
        "max_final":   max(finals),
        "max_buy":     max_buy,
        "extremes":    extremes,
        "avg_final_ref": avg_final,
    }


def count_sales_by_category(pattern: str) -> int:
    with _conn() as c:
        row = c.execute(
            "SELECT COUNT(*) as n FROM sales WHERE category LIKE ?", (pattern,)
        ).fetchone()
        return row["n"] if row else 0
