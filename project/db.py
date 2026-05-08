"""SQLite data access layer for bookkeeping records."""

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

DB_PATH = Path(__file__).resolve().parent / "records.db"

# This list defines the insertion order for batch writes.
RECORD_COLUMNS = [
    "source_id",
    "group_key",
    "amount",
    "cash_flow_amount",
    "net_personal_amount",
    "advance_amount",
    "accounting_basis",
    "category",
    "time",
    "title",
    "segment_text",
    "pay_method",
    "merchant",
    "note",
    "direction",
    "relation_type",
    "relation_group_id",
    "confidence",
]


def _get_connection() -> sqlite3.Connection:
    """Create and return a SQLite connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _safe_add_column(conn: sqlite3.Connection, table: str, column_sql: str) -> None:
    """Try to add a column for migration and ignore if it already exists."""
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column_sql}")
    except sqlite3.OperationalError:
        pass


def init_db() -> None:
    """Initialize database tables and apply lightweight schema migrations."""
    with _get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS source_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                full_text TEXT NOT NULL,
                summary_text TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER,
                group_key TEXT,
                amount REAL,
                cash_flow_amount REAL,
                net_personal_amount REAL,
                advance_amount REAL,
                accounting_basis TEXT DEFAULT 'personal_net',
                category TEXT,
                time TEXT,
                title TEXT,
                segment_text TEXT,
                pay_method TEXT,
                merchant TEXT,
                note TEXT,
                direction TEXT DEFAULT 'expense',
                relation_type TEXT,
                relation_group_id TEXT,
                confidence REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(source_id) REFERENCES source_messages(id)
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS operation_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation_type TEXT,
                record_ids TEXT,
                before_payload TEXT,
                after_payload TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # Compatibility migration from old versions.
        _safe_add_column(conn, "records", "source_id INTEGER")
        _safe_add_column(conn, "records", "group_key TEXT")
        _safe_add_column(conn, "records", "amount REAL")
        _safe_add_column(conn, "records", "cash_flow_amount REAL")
        _safe_add_column(conn, "records", "net_personal_amount REAL")
        _safe_add_column(conn, "records", "advance_amount REAL")
        _safe_add_column(conn, "records", "accounting_basis TEXT DEFAULT 'personal_net'")
        _safe_add_column(conn, "records", "title TEXT")
        _safe_add_column(conn, "records", "segment_text TEXT")
        _safe_add_column(conn, "records", "pay_method TEXT")
        _safe_add_column(conn, "records", "merchant TEXT")
        _safe_add_column(conn, "records", "note TEXT")
        _safe_add_column(conn, "records", "direction TEXT DEFAULT 'expense'")
        _safe_add_column(conn, "records", "relation_type TEXT")
        _safe_add_column(conn, "records", "relation_group_id TEXT")
        _safe_add_column(conn, "records", "confidence REAL")

        # Legacy support: old schema used raw_text and no segment_text.
        _safe_add_column(conn, "records", "raw_text TEXT")
        conn.execute(
            """
            UPDATE records
            SET segment_text = COALESCE(segment_text, raw_text)
            WHERE segment_text IS NULL
            """
        )

        # Backfill amount variants for historical rows.
        conn.execute(
            """
            UPDATE records
            SET cash_flow_amount = COALESCE(cash_flow_amount, amount),
                net_personal_amount = COALESCE(net_personal_amount, amount),
                accounting_basis = COALESCE(accounting_basis, 'personal_net')
            WHERE cash_flow_amount IS NULL OR net_personal_amount IS NULL OR accounting_basis IS NULL
            """
        )

        conn.commit()


def insert_source_message(full_text: str, summary_text: str) -> int:
    """Insert one source message and return its ID."""
    with _get_connection() as conn:
        cursor = conn.execute(
            "INSERT INTO source_messages (full_text, summary_text) VALUES (?, ?)",
            (full_text, summary_text),
        )
        conn.commit()
        return int(cursor.lastrowid)


def insert_operation_log(
    operation_type: str,
    record_ids: Optional[List[int]] = None,
    before_payload: Optional[Dict[str, Any]] = None,
    after_payload: Optional[Dict[str, Any]] = None,
) -> int:
    """Insert one operation log row and return its ID."""
    with _get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO operation_log (operation_type, record_ids, before_payload, after_payload)
            VALUES (?, ?, ?, ?)
            """,
            (
                operation_type,
                json.dumps(record_ids or [], ensure_ascii=False),
                json.dumps(before_payload or {}, ensure_ascii=False),
                json.dumps(after_payload or {}, ensure_ascii=False),
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)


def get_last_operation_log() -> Optional[Dict[str, Any]]:
    """Return latest operation log row for potential undo."""
    with _get_connection() as conn:
        row = conn.execute(
            "SELECT id, operation_type, record_ids, before_payload, after_payload, created_at FROM operation_log ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if not row:
            return None
        result = dict(row)
        for key in ["record_ids", "before_payload", "after_payload"]:
            try:
                result[key] = json.loads(result.get(key) or "{}")
            except json.JSONDecodeError:
                result[key] = [] if key == "record_ids" else {}
        return result


def insert_records_batch(items: List[Dict[str, Any]]) -> List[int]:
    """Insert multiple records in one transaction and return inserted IDs."""
    inserted_ids: List[int] = []
    with _get_connection() as conn:
        for item in items:
            values = [item.get(col) for col in RECORD_COLUMNS]
            cursor = conn.execute(
                """
                INSERT INTO records (
                    source_id, group_key, amount, cash_flow_amount, net_personal_amount,
                    advance_amount, accounting_basis, category, time, title, segment_text,
                    pay_method, merchant, note, direction, relation_type, relation_group_id,
                    confidence
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                tuple(values),
            )
            inserted_ids.append(int(cursor.lastrowid))
        conn.commit()
    return inserted_ids


def get_summary_by_date(date_text: str) -> Dict[str, float]:
    """Return expense/income/net summary for one date."""
    with _get_connection() as conn:
        cursor = conn.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN COALESCE(direction, 'expense') = 'expense' THEN amount ELSE 0 END), 0) AS expense,
                COALESCE(SUM(CASE WHEN COALESCE(direction, 'expense') = 'income' THEN amount ELSE 0 END), 0) AS income,
                COUNT(*) AS record_count
            FROM records
            WHERE time = ?
            """,
            (date_text,),
        )
        row = cursor.fetchone()
        expense = float(row["expense"]) if row else 0.0
        income = float(row["income"]) if row else 0.0
        count = float(row["record_count"]) if row else 0.0
        return {
            "expense": expense,
            "income": income,
            "net_expense": expense - income,
            "record_count": count,
        }


def get_summary_by_date_range(start_date: str, end_date: str) -> Dict[str, float]:
    """Return expense/income/net summary for date range."""
    with _get_connection() as conn:
        row = conn.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN COALESCE(direction, 'expense') = 'expense' THEN amount ELSE 0 END), 0) AS expense,
                COALESCE(SUM(CASE WHEN COALESCE(direction, 'expense') = 'income' THEN amount ELSE 0 END), 0) AS income,
                COUNT(*) AS record_count
            FROM records
            WHERE time BETWEEN ? AND ?
            """,
            (start_date, end_date),
        ).fetchone()
        expense = float(row["expense"]) if row else 0.0
        income = float(row["income"]) if row else 0.0
        count = float(row["record_count"]) if row else 0.0
        return {
            "expense": expense,
            "income": income,
            "net_expense": expense - income,
            "record_count": count,
        }


def get_category_summary_by_date(date_text: str) -> List[Dict[str, Any]]:
    """Return expense totals grouped by category for one date."""
    with _get_connection() as conn:
        cursor = conn.execute(
            """
            SELECT category, COALESCE(SUM(amount), 0) AS total, COUNT(*) AS cnt
            FROM records
            WHERE time = ? AND COALESCE(direction, 'expense') = 'expense'
            GROUP BY category
            ORDER BY total DESC
            """,
            (date_text,),
        )
        return [dict(row) for row in cursor.fetchall()]


def get_records_by_date(date_text: str, limit: int = 10) -> List[Dict[str, Any]]:
    """Return recent records for a date, ordered by id desc."""
    with _get_connection() as conn:
        cursor = conn.execute(
            """
            SELECT
                id, source_id, group_key, amount, cash_flow_amount, net_personal_amount,
                advance_amount, accounting_basis, category, time, title, segment_text,
                pay_method, merchant, note, direction, relation_type, relation_group_id,
                confidence, created_at
            FROM records
            WHERE time = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (date_text, limit),
        )
        return [dict(row) for row in cursor.fetchall()]


def get_records_by_date_range(start_date: str, end_date: str, limit: int = 2000) -> List[Dict[str, Any]]:
    """Return records within date range ordered by id desc."""
    with _get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                id, source_id, group_key, amount, cash_flow_amount, net_personal_amount,
                advance_amount, accounting_basis, category, time, title, segment_text,
                pay_method, merchant, note, direction, relation_type, relation_group_id,
                confidence, created_at
            FROM records
            WHERE time BETWEEN ? AND ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (start_date, end_date, limit),
        ).fetchall()
        return [dict(row) for row in rows]


def get_records_by_ids(record_ids: List[int]) -> List[Dict[str, Any]]:
    """Return records by ID list, preserving descending id order."""
    if not record_ids:
        return []
    placeholders = ",".join(["?"] * len(record_ids))
    with _get_connection() as conn:
        cursor = conn.execute(
            f"""
            SELECT
                id, source_id, group_key, amount, cash_flow_amount, net_personal_amount,
                advance_amount, accounting_basis, category, time, title, segment_text,
                pay_method, merchant, note, direction, relation_type, relation_group_id,
                confidence, created_at
            FROM records
            WHERE id IN ({placeholders})
            ORDER BY id DESC
            """,
            tuple(record_ids),
        )
        return [dict(row) for row in cursor.fetchall()]


def get_last_record() -> Optional[Dict[str, Any]]:
    """Return the latest record as a dict, or None if table is empty."""
    with _get_connection() as conn:
        cursor = conn.execute(
            """
            SELECT
                id, source_id, group_key, amount, cash_flow_amount, net_personal_amount,
                advance_amount, accounting_basis, category, time, title, segment_text,
                pay_method, merchant, note, direction, relation_type, relation_group_id,
                confidence, created_at
            FROM records
            ORDER BY id DESC
            LIMIT 1
            """
        )
        row = cursor.fetchone()
        return dict(row) if row else None


def update_record(record_id: int, amount: float) -> bool:
    """Update record amount by ID and return True if row exists."""
    with _get_connection() as conn:
        cursor = conn.execute(
            """
            UPDATE records
            SET amount = ?, net_personal_amount = ?, cash_flow_amount = COALESCE(cash_flow_amount, ?)
            WHERE id = ?
            """,
            (amount, amount, amount, record_id),
        )
        conn.commit()
        return cursor.rowcount > 0


def delete_record(record_id: int) -> bool:
    """Delete record by ID and return True if row exists."""
    with _get_connection() as conn:
        cursor = conn.execute("DELETE FROM records WHERE id = ?", (record_id,))
        conn.commit()
        return cursor.rowcount > 0


def delete_records_by_date(date_text: str) -> int:
    """Delete all records for a date and return deleted row count."""
    with _get_connection() as conn:
        cursor = conn.execute("DELETE FROM records WHERE time = ?", (date_text,))
        conn.commit()
        return int(cursor.rowcount)


if __name__ == "__main__":
    """Initialize database when this module is run directly."""
    init_db()
    print(f"Database initialized at: {DB_PATH}")
