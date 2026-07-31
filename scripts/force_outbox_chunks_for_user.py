#!/usr/bin/env python3
"""
Force chunk (outbox initial) para TODAS as campanhas Uazapi running/pending de um user.

Uso (no container /app):
  python scripts/force_outbox_chunks_for_user.py --user-id 10
  python scripts/force_outbox_chunks_for_user.py --user-id 10 --dry-run

Ideal para cron diário às 08:05 BRT como rede de segurança além do auto-force do worker.
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
    parser = argparse.ArgumentParser(description="Force outbox initial chunks for a user")
    parser.add_argument("--user-id", type=int, required=True)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Lista campanhas elegíveis sem enfileirar",
    )
    args = parser.parse_args()

    from app import get_db_connection
    from worker_message_outbox import (
        _count_eligible_pending_leads_without_initial_outbox,
        _count_initial_outbox_in_flight,
        _list_uazapi_campaigns_needing_initial,
        force_initial_outbox_batches_for_user,
    )

    conn = get_db_connection()
    try:
        camps = _list_uazapi_campaigns_needing_initial(conn, user_id=args.user_id)
        preview = []
        for c in camps:
            cid = int(c["id"])
            preview.append(
                {
                    "campaign_id": cid,
                    "name": c.get("name"),
                    "status": c.get("status"),
                    "eligible": _count_eligible_pending_leads_without_initial_outbox(
                        conn, cid
                    ),
                    "in_flight": _count_initial_outbox_in_flight(conn, cid),
                }
            )
        print(json.dumps({"user_id": args.user_id, "campaigns": preview}, ensure_ascii=False, indent=2))
        if args.dry_run:
            return 0
        result = force_initial_outbox_batches_for_user(conn, args.user_id, force=True)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return 0 if int(result.get("rows_enqueued") or 0) >= 0 else 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
