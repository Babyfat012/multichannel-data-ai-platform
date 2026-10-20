"""Read-only, bounded-memory audit of a known Walmart source-ID collision."""

from __future__ import annotations

import duckdb


def main() -> None:
    """Inspect source-to-label keys and report their overlap for bridge design."""
    connection = duckdb.connect()
    connection.execute("SET memory_limit='256MB'")
    connection.execute("SET threads=1")
    path = "/data/raw/ground-truth/order_line_keys/walmart/*.parquet"
    rows = connection.execute(
        """SELECT order_line_id, channel_order_id, channel_line_id
           FROM read_parquet(?)
           WHERE channel_order_id = '1678172477013'
           ORDER BY channel_line_id, order_line_id""",
        [path],
    ).fetchall()
    for row in rows:
        print(*row)


if __name__ == "__main__":
    main()
