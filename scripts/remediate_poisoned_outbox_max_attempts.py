#!/usr/bin/env python3
"""
Marca outboxes envenenados (muitas tentativas retry_scheduled) como failed.

Uso operacional pós-deploy da correção de max attempts (campanha 273 / outbox 356):

  python scripts/remediate_poisoned_outbox_max_attempts.py --campaign-id 273 --dry-run
  python scripts/remediate_poisoned_outbox_max_attempts.py --campaign-id 273 --outbox-id 356
  python scripts/remediate_poisoned_outbox_max_attempts.py --min-retries 8

Por padrão usa UAZAPI_OUTBOX_MAX_ATTEMPTS (default 8) como limiar.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _max_attempts_default() -> int:
    return max(1, int(os.environ.get("UAZAPI_OUTBOX_MAX_ATTEMPTS", "8")))


def _find_poisoned(conn, *, campaign_id: int | None, outbox_id: int | None, min_retries: int):
    from psycopg2.extras import RealDictCursor

    clauses = [
        "o.status IN ('pending', 'waiting_instance', 'sending')",
        "a.retry_count >= %s",
    ]
    params: list = [min_retries]
    if campaign_id is not None:
        clauses.append("o.campaign_id = %s")
        params.append(campaign_id)
    if outbox_id is not None:
        clauses.append("o.id = %s")
        params.append(outbox_id)

    sql = f"""
        SELECT o.id AS outbox_id, o.campaign_id, o.campaign_lead_id, o.status,
               cl.csv_row_order, cl.phone,
               a.retry_count,
               a.last_outcome, a.last_http_status
        FROM campaign_message_outbox o
        JOIN campaign_leads cl ON cl.id = o.campaign_lead_id
        JOIN (
            SELECT outbox_id,
                   COUNT(*) FILTER (WHERE outcome = 'retry_scheduled') AS retry_count,
                   MAX(outcome) FILTER (WHERE attempt_no = (
                       SELECT MAX(attempt_no) FROM campaign_send_attempts c2
                       WHERE c2.outbox_id = campaign_send_attempts.outbox_id
                   )) AS last_outcome,
                   MAX(http_status) FILTER (WHERE attempt_no = (
                       SELECT MAX(attempt_no) FROM campaign_send_attempts c3
                       WHERE c3.outbox_id = campaign_send_attempts.outbox_id
                   )) AS last_http_status
            FROM campaign_send_attempts
            GROUP BY outbox_id
        ) a ON a.outbox_id = o.id
        WHERE {" AND ".join(clauses)}
        ORDER BY a.retry_count DESC, o.id
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, tuple(params))
        return cur.fetchall() or []


def _remediate(conn, rows, *, dry_run: bool) -> int:
    from psycopg2.extras import RealDictCursor

    if not rows:
        return 0
    if dry_run:
        return len(rows)

    n = 0
    for r in rows:
        oid = int(r["outbox_id"])
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT COALESCE(MAX(attempt_no), 0) AS m FROM campaign_send_attempts WHERE outbox_id = %s",
                (oid,),
            )
            attempt_no = int((cur.fetchone() or {}).get("m") or 0) + 1
            cur.execute(
                """
                INSERT INTO campaign_send_attempts
                    (outbox_id, attempt_no, http_status, uazapi_response, outcome, latency_ms, finished_at)
                VALUES (%s, %s, %s, %s, 'failed_max_attempts', 0, NOW())
                """,
                (
                    oid,
                    attempt_no,
                    r.get("last_http_status"),
                    json.dumps(
                        {
                            "remediation": "manual_failed_max_attempts",
                            "prior_retry_count": int(r.get("retry_count") or 0),
                            "ts": datetime.now(timezone.utc).isoformat(),
                        }
                    ),
                ),
            )
            cur.execute(
                """
                UPDATE campaign_message_outbox
                SET status = 'failed', updated_at = NOW()
                WHERE id = %s AND status IN ('pending', 'waiting_instance', 'sending')
                """,
                (oid,),
            )
            if cur.rowcount:
                n += 1
        conn.commit()
    return n


def main() -> None:
    parser = argparse.ArgumentParser(description="Remedia outboxes com retry infinito.")
    parser.add_argument("--campaign-id", type=int, default=None)
    parser.add_argument("--outbox-id", type=int, default=None)
    parser.add_argument(
        "--min-retries",
        type=int,
        default=None,
        help="Mínimo de retry_scheduled para marcar failed (default: UAZAPI_OUTBOX_MAX_ATTEMPTS)",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    min_retries = args.min_retries if args.min_retries is not None else _max_attempts_default()

    from app import get_db_connection

    conn = get_db_connection()
    try:
        rows = _find_poisoned(
            conn,
            campaign_id=args.campaign_id,
            outbox_id=args.outbox_id,
            min_retries=min_retries,
        )
        if not rows:
            print(f"Nenhum outbox com >= {min_retries} retry_scheduled encontrado.")
            return
        print(f"Encontrados {len(rows)} outbox(s) elegíveis (min_retries={min_retries}):")
        for r in rows:
            print(
                f"  outbox_id={r['outbox_id']} campaign_id={r['campaign_id']} "
                f"csv_row={r.get('csv_row_order')} phone={r.get('phone')} "
                f"status={r['status']} retries={r['retry_count']}"
            )
        if args.dry_run:
            print("\n[DRY-RUN] Nada alterado.")
            return
        confirm = input("\nConfirma remediação? (s/N): ").strip().lower()
        if confirm != "s":
            print("Cancelado.")
            return
        n = _remediate(conn, rows, dry_run=False)
        print(f"✓ {n} outbox(s) marcados como failed (failed_max_attempts).")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
