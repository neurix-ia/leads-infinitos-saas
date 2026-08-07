#!/usr/bin/env python3
"""
Force chunk (outbox initial) para campanhas Uazapi running/pending.

Uso (no container /app):
  python3 scripts/force_outbox_chunks_for_user.py --user-id 10
  python3 scripts/force_outbox_chunks_for_user.py --user-id 10 --dry-run
  python3 scripts/force_outbox_chunks_for_user.py --all-users
  python3 scripts/force_outbox_chunks_for_user.py --all-users --dry-run
  python3 scripts/force_outbox_chunks_for_user.py --user-id 22 --no-recover

Cron diário (VPS em UTC → 11:05 = 08:05 BRT, seg–sex) — TODOS os users + recover:
  5 11 * * 1-5 docker exec leads_infinitos_web python3 /app/scripts/force_outbox_chunks_for_user.py --all-users >> /var/log/force_outbox_all.log 2>&1

NÃO use mais --user-id 10 sozinho no cron (deixava outros users sem rede de segurança).
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
        description="Force outbox initial chunks for one user or all users"
    )
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--user-id", type=int, help="Só este user_id")
    scope.add_argument(
        "--all-users",
        action="store_true",
        help="Todas as campanhas Uazapi elegíveis de todos os users",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Lista campanhas elegíveis sem enfileirar",
    )
    parser.add_argument(
        "--no-recover",
        action="store_true",
        help="Não reativa pending vencidos / waiting_instance antes do force",
    )
    args = parser.parse_args()

    from app import get_db_connection
    from worker_message_outbox import (
        _count_eligible_pending_leads_without_initial_outbox,
        _count_initial_outbox_in_flight,
        _list_uazapi_campaigns_needing_initial,
        force_initial_outbox_batches_for_user,
    )

    user_id = None if args.all_users else int(args.user_id)
    recover = not bool(args.no_recover)
    conn = get_db_connection()
    try:
        camps = _list_uazapi_campaigns_needing_initial(conn, user_id=user_id)
        preview = []
        for c in camps:
            cid = int(c["id"])
            preview.append(
                {
                    "campaign_id": cid,
                    "user_id": c.get("user_id"),
                    "name": c.get("name"),
                    "status": c.get("status"),
                    "daily_limit": c.get("daily_limit"),
                    "eligible": _count_eligible_pending_leads_without_initial_outbox(
                        conn, cid
                    ),
                    "in_flight": _count_initial_outbox_in_flight(conn, cid),
                }
            )
        print(
            json.dumps(
                {
                    "scope": "all_users" if args.all_users else f"user:{user_id}",
                    "recover": recover,
                    "campaigns": preview,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        if args.dry_run:
            return 0
        result = force_initial_outbox_batches_for_user(
            conn, user_id, force=True, recover=recover
        )
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return 0 if int(result.get("rows_enqueued") or 0) >= 0 else 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
