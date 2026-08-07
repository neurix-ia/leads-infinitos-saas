#!/usr/bin/env python3
"""
Sobe campaigns.daily_limit para 30 em campanhas Scale ativas com teto < 30.

Uso (no container /app):
  python3 scripts/bump_scale_campaign_daily_limits.py --dry-run
  python3 scripts/bump_scale_campaign_daily_limits.py
  python3 scripts/bump_scale_campaign_daily_limits.py --user-id 22
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Bump Scale active campaigns daily_limit to 30 when below 30"
    )
    parser.add_argument("--user-id", type=int, help="Só este user_id (opcional)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Lista campanhas elegíveis sem UPDATE",
    )
    parser.add_argument(
        "--target",
        type=int,
        default=30,
        help="Novo daily_limit (default 30; teto por campanha)",
    )
    args = parser.parse_args()
    target = max(5, min(int(args.target), 30))

    from psycopg2.extras import RealDictCursor

    from app import get_db_connection

    conn = get_db_connection()
    try:
        params: list = [target]
        user_clause = ""
        if args.user_id is not None:
            user_clause = " AND c.user_id = %s "
            params.append(int(args.user_id))

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT c.id AS campaign_id, c.user_id, c.name, c.status, c.daily_limit,
                       l.license_type
                FROM campaigns c
                JOIN licenses l ON l.user_id = c.user_id
                  AND l.status = 'active'
                  AND l.expires_at > NOW()
                  AND LOWER(TRIM(l.license_type)) = 'scale'
                WHERE c.status IN ('running', 'pending')
                  AND COALESCE(c.daily_limit, 0) < %s
                  {user_clause}
                ORDER BY c.user_id ASC, c.id ASC
                """,
                tuple(params),
            )
            rows = [dict(r) for r in (cur.fetchall() or [])]

        print(
            json.dumps(
                {
                    "dry_run": bool(args.dry_run),
                    "target": target,
                    "count": len(rows),
                    "campaigns": rows,
                },
                ensure_ascii=False,
                indent=2,
                default=str,
            )
        )
        if args.dry_run or not rows:
            return 0

        ids = [int(r["campaign_id"]) for r in rows]
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE campaigns
                SET daily_limit = %s
                WHERE id = ANY(%s)
                """,
                (target, ids),
            )
            updated = cur.rowcount
        conn.commit()
        print(json.dumps({"updated": int(updated or 0)}, ensure_ascii=False))
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
