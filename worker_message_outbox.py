"""
Worker — fila Postgres ``campaign_message_outbox`` (envio unitário Uazapi).

ADR-5: (A) claim curto em transação → COMMIT → (B) HTTP sem tx → (C) persistência tentativa + estado.
Task 4 tech-spec: throttle antes do claim; §6.1 contagens só após HTTP 200 na fase (C).
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta
from typing import Any, Optional

import psycopg2
from psycopg2.extras import RealDictCursor

from services.uazapi import UazapiService
from utils.campaign_dispatch_audit import append_dispatch_audit_event
from utils.config import SUPER_ADMIN_EMAILS, USE_MESSAGE_OUTBOX
from utils.campaign_send_policy import uazapi_initial_chunk_distribution_limits
from utils.limits import (
    check_initial_chunk_daily_quota_for_campaign,
    get_user_daily_limit,
    initial_chunk_quota_snapshot,
)
from utils.next_valid_uazapi_send import (
    is_campaign_send_window,
    next_valid_send_after_current_window_utc_naive,
    next_valid_send_utc_naive,
)
from utils.outbox_prometheus import observe_campaign_outbox_send_attempt
from utils.uazapi_outbox_errors import classify_outbox_send_failure

try:
    uazapi_service = UazapiService()
except ImportError:
    uazapi_service = None

logger = logging.getLogger(__name__)


def get_db_connection():
    conn = psycopg2.connect(
        host=os.environ.get("DB_HOST", "localhost"),
        database=os.environ.get("DB_NAME", "leads_infinitos"),
        user=os.environ.get("DB_USER", "postgres"),
        password=os.environ.get("DB_PASSWORD"),
        port=os.environ.get("DB_PORT", "5432"),
    )
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
    return conn


# Última instância servida no tick outbox (round-robin entre instâncias quando ``rotation_mode == round_robin``).
_last_outbox_instance_id: Optional[int] = None
# Throttle de logs de skip do agendamento automático (campaign_id, reason) → monotonic.
_outbox_schedule_skip_logged_at: dict[tuple[int, str], float] = {}

STAGE_TO_STEP_NUMBER = {"initial": 1, "follow1": 2, "follow2": 3, "breakup": 4}

# Cadência outbox: (stage, lead.current_step na coluna Kanban, step_priority, campaign_steps.step_number)
_CADENCE_OUTBOX_STAGES: tuple[tuple[str, int, int, int], ...] = (
    ("follow1", 2, 1, 2),
    ("follow2", 3, 2, 3),
    ("breakup", 4, 3, 4),
)

_REAPER_MINUTES = int(os.environ.get("UAZAPI_OUTBOX_SENDING_REAPER_MINUTES", "20"))
_OUTBOX_STALE_PENDING_TTL_MINUTES = int(
    os.environ.get("UAZAPI_OUTBOX_STALE_PENDING_TTL_MINUTES", "30")
)
_OUTBOX_RETRY_BASE_SEC = int(os.environ.get("UAZAPI_OUTBOX_RETRY_BASE_SECONDS", "30"))
_OUTBOX_RETRY_MAX_SEC = int(os.environ.get("UAZAPI_OUTBOX_RETRY_MAX_SECONDS", "3600"))
_OUTBOX_MAX_ATTEMPTS = max(
    1,
    int(os.environ.get("UAZAPI_OUTBOX_MAX_ATTEMPTS", "8")),
)


def _outbox_retry_backoff_seconds(attempt_no: int) -> int:
    """Backoff exponencial por número da tentativa (1-based), com teto."""
    exp = max(0, attempt_no - 1)
    raw = _OUTBOX_RETRY_BASE_SEC * (2**exp)
    return min(_OUTBOX_RETRY_MAX_SEC, raw)


def _result_json_for_classify(
    response_body: Optional[str], audit_response_body: Any
) -> Any:
    if isinstance(audit_response_body, dict):
        return audit_response_body
    if isinstance(response_body, str) and response_body.strip():
        try:
            parsed = json.loads(response_body)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass
    return None


def _normalize_uazapi_send_result(result_json: Any) -> dict[str, Any]:
    """
    Separa sucesso vs falha estruturada Uazapi para persistência e classificação.
    Dict com ``uazapi_request_failed`` nunca é tratado como sucesso (truthy).
    """
    if result_json is None:
        legacy_failure = {
            "uazapi_request_failed": True,
            "reason": "empty_response",
            "error_body": {"message": "Uazapi returned no response"},
        }
        return {
            "success": False,
            "http_status": None,
            "response_body": json.dumps(legacy_failure, ensure_ascii=False),
            "audit_response_body": legacy_failure,
            "result_json_classify": legacy_failure,
            "exception_class_name": None,
        }
    if not isinstance(result_json, dict):
        body = json.dumps({"raw": str(result_json)[:2000]}, ensure_ascii=False)
        return {
            "success": False,
            "http_status": None,
            "response_body": body,
            "audit_response_body": str(result_json)[:4000],
            "result_json_classify": None,
            "exception_class_name": None,
        }
    if result_json.get("uazapi_request_failed"):
        http_status: Optional[int] = None
        raw_status = result_json.get("http_status")
        if raw_status is not None:
            try:
                http_status = int(raw_status)
            except (TypeError, ValueError):
                http_status = None
        response_body = json.dumps(result_json, ensure_ascii=False)
        return {
            "success": False,
            "http_status": http_status,
            "response_body": response_body,
            "audit_response_body": result_json,
            "result_json_classify": result_json,
            "exception_class_name": result_json.get("exception"),
        }
    response_body = json.dumps(result_json, ensure_ascii=False)
    return {
        "success": True,
        "http_status": 200,
        "response_body": response_body,
        "audit_response_body": result_json,
        "result_json_classify": result_json,
        "exception_class_name": None,
    }


def _error_summary_from_send_failure(
    *,
    http_status: Optional[int],
    response_body: Optional[str],
    result_json: Any,
    failure_kind: Optional[str],
) -> str:
    """Resumo curto para auditoria JSONL (sem token)."""
    parts: list[str] = []
    if failure_kind:
        parts.append(str(failure_kind))
    if http_status is not None:
        parts.append(f"HTTP {http_status}")
    if isinstance(result_json, dict):
        reason = result_json.get("reason")
        if reason:
            parts.append(str(reason))
        eb = result_json.get("error_body")
        if isinstance(eb, dict):
            msg = eb.get("message") or eb.get("error") or eb.get("Message")
            if msg:
                parts.append(str(msg)[:200])
        elif isinstance(eb, str) and eb.strip():
            parts.append(eb.strip()[:200])
        exc = result_json.get("exception") or result_json.get("exception_message")
        if exc:
            parts.append(str(exc)[:120])
    elif response_body and response_body not in _INTERNAL_TERMINAL_MARKERS:
        parts.append(str(response_body)[:200])
    return " · ".join(parts) if parts else "send failed"


_INTERNAL_TERMINAL_MARKERS = frozenset({"missing_phone", "missing_message_template"})


def _count_retry_scheduled_attempts(conn, outbox_id: int) -> int:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS n
            FROM campaign_send_attempts
            WHERE outbox_id = %s AND outcome = 'retry_scheduled'
            """,
            (outbox_id,),
        )
        row = cur.fetchone() or {}
    return int(row.get("n") or 0)


def _parse_message_template(raw: Any) -> list[str]:
    if not raw or not str(raw).strip():
        return []
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            msgs = [str(x).strip() for x in parsed if str(x).strip()]
            return msgs if msgs else []
        if isinstance(parsed, str) and parsed.strip():
            return [parsed.strip()]
    except Exception:
        if isinstance(raw, str) and raw.strip():
            return [raw.strip()]
    return []


def _normalize_phone_e164_br(phone: Any) -> Optional[str]:
    if not phone:
        return None
    clean = re.sub(r"\D", "", str(phone))
    if len(clean) <= 11 and not clean.startswith("55"):
        clean = "55" + clean
    return clean if len(clean) >= 12 else clean or None


def _is_media_path_safe(media_path: str, user_id: int) -> bool:
    if not media_path or not user_id:
        return False
    if ".." in media_path:
        return False
    prefix = f"storage/{user_id}/"
    return bool(media_path.startswith(prefix))


def _truncate(s: Optional[str], n: int = 1800) -> str:
    if not s:
        return ""
    s = str(s)
    return s if len(s) <= n else s[:n] + "…"


def _log_outbox_attempt_event(
    *,
    campaign_id: int,
    outbox_id: int,
    instance_id: int,
    latency_ms: int,
    outcome: str,
    http_status: Optional[int] = None,
) -> None:
    """
    Um evento por tentativa (Task 11); JSON numa linha — sem PII (F11).
    """
    payload: dict[str, Any] = {
        "event": "campaign_outbox_send_attempt",
        "campaign_id": campaign_id,
        "outbox_id": outbox_id,
        "instance_id": instance_id,
        "latency_ms": latency_ms,
        "outcome": outcome,
    }
    if http_status is not None:
        payload["http_status"] = http_status
    logger.info(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    observe_campaign_outbox_send_attempt(outcome, latency_ms)


def _campaign_dict_from_row(c: dict) -> dict:
    return {
        "send_hour_start": c.get("send_hour_start"),
        "send_hour_end": c.get("send_hour_end"),
        "send_saturday": c.get("send_saturday"),
        "send_sunday": c.get("send_sunday"),
    }


def _user_is_superadmin(conn, user_id: int) -> bool:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT email FROM users WHERE id = %s", (user_id,))
        row = cur.fetchone()
    return bool(row and row.get("email") in SUPER_ADMIN_EMAILS)


def _outbox_csv_sort_key(row: dict) -> tuple:
    ordv = row.get("csv_row_order")
    try:
        ord_i = int(ordv) if ordv is not None else int(row.get("campaign_lead_id") or 0)
    except (TypeError, ValueError):
        ord_i = int(row.get("campaign_lead_id") or 0)
    nr = row.get("next_run_at")
    nr_key = 0
    if nr is not None:
        try:
            nr_key = int(nr.timestamp()) if hasattr(nr, "timestamp") else 0
        except Exception:
            nr_key = 0
    return (
        int(row.get("step_priority") or 0),
        ord_i,
        int(row.get("campaign_lead_id") or 0),
        nr_key,
        int(row.get("id") or 0),
    )


def _rotation_mode_round_robin(row: dict) -> bool:
    rm = (row.get("rotation_mode") or "single").strip().lower()
    return rm == "round_robin"


def _outbox_rr_instance_tie_key(row: dict) -> tuple:
    """
    Critério de empate para rotação entre instâncias (Task 4 / ADR-O3):
    mesma campanha, passo, posição CSV na fila e janela ``next_run_at`` — exclui
    ``instance_id`` e ``id`` da outbox para permitir mais de uma linha elegível.
    """
    t = _outbox_csv_sort_key(row)
    return (int(row.get("campaign_id") or 0), t[0], t[1], t[3])


def _pick_instance_round_robin(front: list[dict]) -> dict:
    """Entre linhas empatadas na ordenação RR, alterna ``instance_id`` (estado global)."""
    global _last_outbox_instance_id
    if len(front) == 1:
        return front[0]
    inst_ids = sorted({int(c["instance_id"]) for c in front})
    if len(inst_ids) == 1:
        return min(front, key=lambda x: int(x["id"]))
    last = _last_outbox_instance_id
    if last is None or last not in inst_ids:
        target = inst_ids[0]
    else:
        idx = inst_ids.index(last)
        target = inst_ids[(idx + 1) % len(inst_ids)]
    matching = [c for c in front if int(c["instance_id"]) == target]
    return min(matching, key=lambda x: int(x["id"]))


def _choose_outbox_row(candidates: list[dict]) -> dict:
    """
    ADR-O3 / Task 4: ordem estrita por ``_outbox_csv_sort_key`` (csv_row_order + desempates).
    Com ``rotation_mode == round_robin``, entre linhas empatadas no critério acima **e**
    no empate de instância (``_outbox_rr_instance_tie_key``), alterna ``instance_id``.
    """
    candidates.sort(key=_outbox_csv_sort_key)
    head = candidates[0]
    hk = _outbox_csv_sort_key(head)
    strict_front = [c for c in candidates if _outbox_csv_sort_key(c) == hk]
    if len(strict_front) > 1 and _rotation_mode_round_robin(head):
        return _pick_instance_round_robin(strict_front)
    rk = _outbox_rr_instance_tie_key(head)
    rr_front = [c for c in candidates if _outbox_rr_instance_tie_key(c) == rk]
    if len(rr_front) > 1 and _rotation_mode_round_robin(head):
        return _pick_instance_round_robin(rr_front)
    return head


def _log_outbox_schedule_skip(campaign_id: int, reason: str) -> None:
    """Evita spam: no máximo um log por (campanha, motivo) a cada 10 min."""
    key = (int(campaign_id), reason)
    now = time.monotonic()
    if now - _outbox_schedule_skip_logged_at.get(key, 0.0) < 600:
        return
    _outbox_schedule_skip_logged_at[key] = now
    logger.info(
        json.dumps(
            {
                "event": "outbox_schedule_skip",
                "campaign_id": campaign_id,
                "reason": reason,
            },
            ensure_ascii=False,
        )
    )


def _log_outbox_daily_quota_reached(campaign_id: int, quota: dict) -> None:
    logger.info(
        json.dumps(
            {
                "event": "outbox_daily_quota_reached",
                "campaign_id": int(campaign_id),
                "sent_campaign_today": quota.get("sent_campaign_today"),
                "campaign_cap": quota.get("campaign_cap"),
                "remaining_slots": quota.get("remaining_slots"),
            },
            ensure_ascii=False,
        )
    )


def _defer_pending_initial_rows_to_next_window(
    conn, campaign_id: int, camp_win: dict, *, reason: str = "daily_quota"
) -> int:
    """Reprograma initial ``pending`` para a próxima janela BRT válida (rollover diário)."""
    now_utc = datetime.utcnow()
    try:
        # Cota esgotada dentro da janela: NÃO usar next_valid(agora)=agora (fica em loop
        # e bloqueia o prime do dia seguinte). Salta para depois do fim da janela actual.
        next_at = next_valid_send_after_current_window_utc_naive(camp_win, now_utc)
    except ValueError:
        return 0
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, next_run_at FROM campaign_message_outbox
            WHERE campaign_id = %s
              AND LOWER(TRIM(stage)) = 'initial'
              AND status = 'pending'
              AND next_run_at <= NOW()
            """,
            (int(campaign_id),),
        )
        rows = cur.fetchall() or []
    deferred = 0
    for row in rows:
        oid = int(row["id"])
        prev = row.get("next_run_at")
        if prev is not None and prev >= next_at:
            continue
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE campaign_message_outbox
                SET next_run_at = %s, updated_at = NOW()
                WHERE id = %s AND status IN ('pending', 'waiting_instance')
                """,
                (next_at, oid),
            )
            if cur.rowcount:
                deferred += 1
                logger.info(
                    json.dumps(
                        {
                            "event": "outbox_next_day_deferred",
                            "campaign_id": int(campaign_id),
                            "outbox_id": oid,
                            "reason": reason,
                            "next_run_at": next_at.isoformat(),
                        },
                        ensure_ascii=False,
                    )
                )
    return deferred


def _recover_stale_pending_initial_outbox(conn, *, max_rows: int = 100) -> int:
    """
    No início do tick: initial ``pending`` com ``next_run_at`` vencido em campanhas
    activas, fora da janela ou sem cota — reprograma para ``next_valid_send_utc_naive``.
    """
    if max_rows <= 0:
        return 0
    now_utc = datetime.utcnow()
    ttl_min = max(1, _OUTBOX_STALE_PENDING_TTL_MINUTES)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT o.id, o.campaign_id, o.next_run_at,
                   c.send_hour_start, c.send_hour_end, c.send_saturday, c.send_sunday
            FROM campaign_message_outbox o
            JOIN campaigns c ON c.id = o.campaign_id
            WHERE LOWER(TRIM(o.stage)) = 'initial'
              AND o.status = 'pending'
              AND o.next_run_at <= NOW()
              AND o.updated_at < (NOW() - (%s * INTERVAL '1 minute'))
              AND c.status IN ('running', 'pending')
            ORDER BY o.next_run_at ASC, o.id ASC
            LIMIT %s
            """,
            (ttl_min, max_rows),
        )
        rows = cur.fetchall() or []

    recovered = 0
    for row in rows:
        row = dict(row)
        cid = int(row["campaign_id"])
        camp_win = _campaign_dict_from_row(row)
        if is_campaign_send_window(camp_win) and check_initial_chunk_daily_quota_for_campaign(
            cid
        ):
            continue
        try:
            nv = next_valid_send_utc_naive(camp_win, now_utc, margin_minutes=0)
        except ValueError:
            continue
        prev = row.get("next_run_at")
        if prev is not None and nv <= prev:
            continue
        oid = int(row["id"])
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE campaign_message_outbox
                SET next_run_at = %s, updated_at = NOW()
                WHERE id = %s AND status IN ('pending', 'waiting_instance')
                """,
                (nv, oid),
            )
            if cur.rowcount:
                recovered += 1
                logger.info(
                    json.dumps(
                        {
                            "event": "outbox_stale_pending_recovered",
                            "campaign_id": cid,
                            "outbox_id": oid,
                            "next_run_at": nv.isoformat(),
                        },
                        ensure_ascii=False,
                    )
                )
    return recovered


_INITIAL_OUTBOX_BLOCKING_STATUSES = ("pending", "waiting_instance")
_INITIAL_OUTBOX_ROW_EXISTS_STATUSES = (
    "pending",
    "sending",
    "sent",
    "waiting_instance",
    "failed",
)


def _count_eligible_pending_leads_without_initial_outbox(conn, campaign_id: int) -> int:
    """
    Leads initial pendentes no CSV sem linha outbox initial activa (incl. ``failed``).
    Usado para não bloquear novo lote quando um único outbox está em backoff.
    """
    cid = int(campaign_id)
    placeholders = ", ".join("%s" for _ in _INITIAL_OUTBOX_ROW_EXISTS_STATUSES)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT COUNT(*)::int AS n FROM campaign_leads cl
            WHERE cl.campaign_id = %s
              AND cl.status = 'pending'
              AND cl.current_step = 1
              AND COALESCE(cl.removed_from_funnel, FALSE) = FALSE
              AND COALESCE(cl.cadence_status, 'active') NOT IN ('converted', 'lost')
              AND NOT EXISTS (
                  SELECT 1 FROM campaign_message_outbox o
                  WHERE o.campaign_lead_id = cl.id
                    AND LOWER(TRIM(o.stage)) = 'initial'
                    AND o.status IN ({placeholders})
              )
            """,
            (cid, *_INITIAL_OUTBOX_ROW_EXISTS_STATUSES),
        )
        row = cur.fetchone() or {}
    return int(row.get("n") or 0)


def _reconcile_initial_outbox_in_flight(
    conn, campaign_id: int, camp_win: dict, quota: dict, *, force: bool
) -> tuple[bool, Optional[str]]:
    """
    ``sending`` bloqueia curto; ``pending`` futuro aguarda; ``pending`` vencido/stale
    é recuperado para a próxima janela em vez de travar o dia seguinte.

    Se existirem leads pendentes **sem** linha outbox initial, não bloqueia novo lote
    só porque outro outbox está em backoff (``next_run_at`` futuro) — evita que um lead
    envenenado impeça enfileirar o restante do CSV.
    """
    if force:
        return True, None

    cid = int(campaign_id)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM campaign_message_outbox
            WHERE campaign_id = %s AND LOWER(TRIM(stage)) = 'initial'
              AND status = 'sending'
            LIMIT 1
            """,
            (cid,),
        )
        if cur.fetchone():
            _log_outbox_schedule_skip(cid, "initial_outbox_sending")
            return False, "initial_outbox_sending"

    now_utc = datetime.utcnow()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        pending_placeholders = ", ".join("%s" for _ in _INITIAL_OUTBOX_BLOCKING_STATUSES)
        cur.execute(
            f"""
            SELECT id, next_run_at FROM campaign_message_outbox
            WHERE campaign_id = %s AND LOWER(TRIM(stage)) = 'initial'
              AND status IN ({pending_placeholders})
            ORDER BY next_run_at ASC NULLS FIRST, id ASC
            """,
            (cid, *_INITIAL_OUTBOX_BLOCKING_STATUSES),
        )
        pending_rows = cur.fetchall() or []

    if not pending_rows:
        return True, None

    eligible_without_outbox = _count_eligible_pending_leads_without_initial_outbox(
        conn, cid
    )

    has_future = False
    has_active_due = False
    for pr in pending_rows:
        pr = dict(pr)
        nr = pr.get("next_run_at")
        if nr is not None and nr > now_utc:
            has_future = True
            continue
        if is_campaign_send_window(camp_win) and quota.get("allows_more"):
            has_active_due = True
            continue
        try:
            nv = next_valid_send_utc_naive(camp_win, now_utc, margin_minutes=0)
        except ValueError:
            has_active_due = True
            continue
        oid = int(pr["id"])
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE campaign_message_outbox
                SET next_run_at = %s, updated_at = NOW()
                WHERE id = %s AND status IN ('pending', 'waiting_instance')
                """,
                (nv, oid),
            )
            if cur.rowcount:
                logger.info(
                    json.dumps(
                        {
                            "event": "outbox_stale_pending_recovered",
                            "campaign_id": cid,
                            "outbox_id": oid,
                            "next_run_at": nv.isoformat(),
                            "context": "schedule_reconcile",
                        },
                        ensure_ascii=False,
                    )
                )

    if has_future:
        if eligible_without_outbox > 0:
            return True, None
        _log_outbox_schedule_skip(cid, "initial_outbox_pending_scheduled")
        return False, "initial_outbox_pending_scheduled"
    if has_active_due:
        if eligible_without_outbox > 0:
            return True, None
        _log_outbox_schedule_skip(cid, "initial_outbox_pending_active")
        return False, "initial_outbox_pending_active"
    return True, None


def _count_initial_outbox_in_flight(conn, campaign_id: int) -> int:
    """Linhas initial ainda na fila (não enviadas / não falhadas)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::int AS n
            FROM campaign_message_outbox
            WHERE campaign_id = %s
              AND LOWER(TRIM(stage)) = 'initial'
              AND status IN ('pending', 'sending', 'waiting_instance')
            """,
            (int(campaign_id),),
        )
        row = cur.fetchone()
    return int((row[0] if row else 0) or 0)


def _maybe_reset_stale_sent_today(conn, campaign_id: int, quota: dict) -> None:
    """Zera ``campaigns.sent_today`` quando a cota BRT do dia já está em 0 (contador stale)."""
    if int(quota.get("sent_campaign_today") or 0) != 0:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE campaigns
            SET sent_today = 0
            WHERE id = %s AND COALESCE(sent_today, 0) <> 0
            """,
            (int(campaign_id),),
        )


def _load_uazapi_instances_for_campaign(conn, campaign_id: int) -> list:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT i.id AS instance_id
            FROM campaign_instances ci
            JOIN instances i ON i.id = ci.instance_id
            WHERE ci.campaign_id = %s
              AND COALESCE(i.api_provider, 'megaapi') = 'uazapi'
              AND i.apikey IS NOT NULL
              AND TRIM(i.apikey) <> ''
            ORDER BY i.id ASC
            """,
            (int(campaign_id),),
        )
        return list(cur.fetchall() or [])


def _select_eligible_leads_for_initial_outbox(
    conn, campaign_id: int, batch_limit: int
) -> list:
    if batch_limit <= 0:
        return []
    exists_placeholders = ", ".join("%s" for _ in _INITIAL_OUTBOX_ROW_EXISTS_STATUSES)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT cl.id
            FROM campaign_leads cl
            WHERE cl.campaign_id = %s
              AND cl.status = 'pending'
              AND cl.current_step = 1
              AND COALESCE(cl.removed_from_funnel, FALSE) = FALSE
              AND COALESCE(cl.cadence_status, 'active') NOT IN ('converted', 'lost')
              AND NOT EXISTS (
                  SELECT 1 FROM campaign_message_outbox o
                  WHERE o.campaign_lead_id = cl.id
                    AND LOWER(TRIM(o.stage)) = 'initial'
                    AND o.status IN ({exists_placeholders})
              )
            ORDER BY COALESCE(cl.send_batch, 999) ASC,
                     COALESCE(cl.csv_row_order, cl.id) ASC,
                     cl.id ASC
            LIMIT %s
            """,
            (int(campaign_id), *_INITIAL_OUTBOX_ROW_EXISTS_STATUSES, int(batch_limit)),
        )
        return list(cur.fetchall() or [])


def _campaign_daily_batch_limit(campaign: dict, n_instances: int) -> int:
    uid = int(campaign.get("user_id") or 0)
    daily_limit = int(campaign.get("daily_limit") or 0)
    if daily_limit <= 0:
        daily_limit = int(get_user_daily_limit(uid) or 30)
    _, total_limit = uazapi_initial_chunk_distribution_limits(
        daily_limit, max(1, int(n_instances))
    )
    return int(total_limit)


def _persist_initial_outbox_batch(
    conn,
    *,
    campaign_id: int,
    leads: list,
    instances: list,
    rotation_mode: str,
    next_run_at_val,
    now_utc: datetime,
    enqueue_reason: str,
) -> int:
    """INSERT outbox initial para ``leads``. Retorna quantas linhas tentou gravar."""
    cid = int(campaign_id)
    n_inst = len(instances)
    if n_inst <= 0 or not leads:
        return 0
    rot = (rotation_mode or "single").strip().lower()
    if rot not in ("single", "round_robin"):
        rot = "single"
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE campaign_stage_sends
            SET status = 'failed',
                last_materialize_error = COALESCE(
                    NULLIF(TRIM(last_materialize_error), ''),
                    'outbox: legacy chunk auto-failed (campanha usa fila outbox)'
                ),
                updated_at = NOW()
            WHERE campaign_id = %s AND stage = 'initial'
              AND status IN ('scheduled', 'running', 'partial', 'queued', 'waiting_reconnect')
              AND COALESCE(success_count, 0) + COALESCE(failed_count, 0) = 0
            """,
            (cid,),
        )
        for i, lr in enumerate(leads):
            lead_id = int(lr["id"])
            if rot == "round_robin":
                inst = instances[i % n_inst]
            else:
                inst = instances[0]
            instance_id = int(inst["instance_id"])
            idempotency_key = f"campaign-{cid}-lead-{lead_id}-initial"
            payload_summary = json.dumps(
                {
                    "stage": "initial",
                    "enqueue": enqueue_reason,
                    "rotation_mode": rot,
                },
                ensure_ascii=False,
            )
            queued_at_val = now_utc + timedelta(
                seconds=i // 1_000_000, microseconds=i % 1_000_000
            )
            cur.execute(
                """
                INSERT INTO campaign_message_outbox (
                    campaign_id, campaign_lead_id, instance_id,
                    stage, step_priority, status, queued_at,
                    next_run_at, idempotency_key, payload_summary
                )
                VALUES (
                    %s, %s, %s, 'initial', 0, 'pending',
                    %s, %s, %s, %s::jsonb
                )
                ON CONFLICT (campaign_lead_id, stage) DO NOTHING
                """,
                (
                    cid,
                    lead_id,
                    instance_id,
                    queued_at_val,
                    next_run_at_val,
                    idempotency_key,
                    payload_summary,
                ),
            )
        lead_ids_out = [int(lr["id"]) for lr in leads]
        cur.execute(
            """
            UPDATE campaign_leads
            SET current_step = 1
            WHERE campaign_id = %s AND id = ANY(%s)
            """,
            (cid, lead_ids_out),
        )
        cur.execute(
            "UPDATE campaigns SET status = 'running' WHERE id = %s",
            (cid,),
        )
    return len(leads)


def _prime_next_window_initial_outbox_batch(
    conn, campaign: dict, camp_win: dict
) -> int:
    """
    Cota do dia esgotada → completa 1 lote (daily_limit) para a próxima abertura de
    janela BRT. Se já existir parte do lote pré-agendada, só completa o restante.
    """
    cid = int(campaign["id"])
    instances = _load_uazapi_instances_for_campaign(conn, cid)
    if not instances:
        return 0
    batch_limit = _campaign_daily_batch_limit(campaign, len(instances))
    now_utc = datetime.utcnow()
    try:
        next_run_at_val = next_valid_send_after_current_window_utc_naive(
            camp_win, now_utc
        )
    except ValueError:
        _log_outbox_schedule_skip(cid, "no_valid_send_window")
        return 0

    # Já agendados para a próxima janela (ou depois) contam no lote do dia seguinte.
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::int AS n
            FROM campaign_message_outbox
            WHERE campaign_id = %s
              AND LOWER(TRIM(stage)) = 'initial'
              AND status IN ('pending', 'sending', 'waiting_instance')
              AND next_run_at >= %s
            """,
            (cid, next_run_at_val),
        )
        row = cur.fetchone()
    already_for_next = int((row[0] if row else 0) or 0)
    need = batch_limit - already_for_next
    if need <= 0:
        return 0

    leads = _select_eligible_leads_for_initial_outbox(conn, cid, need)
    if not leads:
        return 0
    rotation_mode = (campaign.get("rotation_mode") or "single").strip().lower()
    n = _persist_initial_outbox_batch(
        conn,
        campaign_id=cid,
        leads=leads,
        instances=instances,
        rotation_mode=rotation_mode,
        next_run_at_val=next_run_at_val,
        now_utc=now_utc,
        enqueue_reason="prime_next_window",
    )
    logger.info(
        json.dumps(
            {
                "event": "outbox_primed_next_window",
                "campaign_id": cid,
                "rows_enqueued": n,
                "already_for_next": already_for_next,
                "next_run_at": next_run_at_val.isoformat(),
            },
            ensure_ascii=False,
        )
    )
    return n


def schedule_next_initial_outbox_batch(
    conn, campaign: dict, *, force: bool = False
) -> tuple[int, Optional[str]]:
    """
    Ponto único de enfileiramento do lote ``initial`` em ``campaign_message_outbox``.
    Substitui ``campaign_stage_sends`` em ``schedule_next_initial_chunk``.

    Gates: outbox habilitado, campanha Uazapi, status running/pending, leads step-1
    pendentes (e demais checagens de cota/janela/in-flight).

    ``force=True`` (admin / Forçar chunk): enfileira imediatamente, como antes.
    Automático: fora da janela BRT agenda ``next_run_at`` no próximo slot válido em vez de
    ignorar a campanha até o dia seguinte.

    Se a cota diária esgotou e não há outbox in-flight, pré-enfileira o próximo lote
    com ``next_run_at`` na próxima janela (evita depender de force chunk no dia seguinte).
    Com cota livre e dentro do horário, enfileira para disparo no mesmo dia.
    """
    if not USE_MESSAGE_OUTBOX:
        return 0, "outbox_disabled"

    cid = int(campaign["id"])
    uid = int(campaign.get("user_id") or 0)
    if not uid:
        return 0, "no_user_id"
    if not campaign.get("use_uazapi_sender"):
        return 0, "not_uazapi_sender"
    camp_status = (campaign.get("status") or "").strip().lower()
    if camp_status not in ("running", "pending"):
        return 0, "campaign_not_active"

    camp_win = {
        "send_hour_start": campaign.get("send_hour_start"),
        "send_hour_end": campaign.get("send_hour_end"),
        "send_saturday": campaign.get("send_saturday"),
        "send_sunday": campaign.get("send_sunday"),
    }
    quota = initial_chunk_quota_snapshot(cid)
    remaining_slots = int(quota.get("remaining_slots") or 0)
    allows_more = bool(quota.get("allows_more")) and remaining_slots > 0

    if not allows_more:
        _log_outbox_schedule_skip(cid, "daily_quota_exceeded")
        _log_outbox_daily_quota_reached(cid, quota)
        _defer_pending_initial_rows_to_next_window(conn, cid, camp_win, reason="daily_quota")
        try:
            n_primed = _prime_next_window_initial_outbox_batch(conn, campaign, camp_win)
            conn.commit()
        except Exception:
            conn.rollback()
            logger.exception(
                "schedule_next_initial_outbox_batch prime failed campaign_id=%s", cid
            )
            return 0, "daily_quota_exceeded"
        if n_primed > 0:
            return n_primed, "primed_next_window"
        return 0, "daily_quota_exceeded"

    _maybe_reset_stale_sent_today(conn, cid, quota)

    # Evita segundo lote no mesmo dia BRT quando já existe fila (ex.: pré-enfileirado
    # para a próxima janela). Ainda permite enfileirar se só há poucas linhas em backoff
    # (lead envenenado) e sobra cota — ver ``_reconcile_initial_outbox_in_flight``.
    if not force:
        in_flight_n = _count_initial_outbox_in_flight(conn, cid)
        if in_flight_n >= remaining_slots:
            _log_outbox_schedule_skip(cid, "initial_outbox_already_queued")
            conn.commit()
            return 0, "initial_outbox_already_queued"

    ok_in_flight, in_flight_reason = _reconcile_initial_outbox_in_flight(
        conn, cid, camp_win, quota, force=force
    )
    if not ok_in_flight:
        conn.commit()
        return 0, in_flight_reason

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS cnt FROM campaign_leads
            WHERE campaign_id = %s
              AND status = 'pending'
              AND current_step = 1
              AND COALESCE(removed_from_funnel, FALSE) = FALSE
              AND COALESCE(cadence_status, 'active') NOT IN ('converted', 'lost')
            """,
            (cid,),
        )
        pending_row = cur.fetchone()
    if not pending_row or int(pending_row[0] or 0) <= 0:
        return 0, "no_pending_leads"

    instances = _load_uazapi_instances_for_campaign(conn, cid)
    if not instances:
        return 0, "no_uazapi_instance"

    total_limit = _campaign_daily_batch_limit(campaign, len(instances))
    batch_limit = min(total_limit, remaining_slots)

    rotation_mode = (campaign.get("rotation_mode") or "single").strip().lower()
    leads = _select_eligible_leads_for_initial_outbox(conn, cid, batch_limit)
    if not leads:
        _log_outbox_schedule_skip(cid, "no_eligible_pending_leads")
        return 0, "no_eligible_pending_leads"

    now_utc = datetime.utcnow()
    if force or is_campaign_send_window(camp_win):
        next_run_at_val = now_utc
    else:
        try:
            next_run_at_val = next_valid_send_utc_naive(camp_win, now_utc, margin_minutes=0)
        except ValueError:
            _log_outbox_schedule_skip(cid, "no_valid_send_window")
            return 0, "no_valid_send_window"
        logger.info(
            json.dumps(
                {
                    "event": "outbox_next_day_deferred",
                    "campaign_id": cid,
                    "reason": "outside_send_window",
                    "next_run_at": next_run_at_val.isoformat(),
                },
                ensure_ascii=False,
            )
        )

    try:
        n = _persist_initial_outbox_batch(
            conn,
            campaign_id=cid,
            leads=leads,
            instances=instances,
            rotation_mode=rotation_mode,
            next_run_at_val=next_run_at_val,
            now_utc=now_utc,
            enqueue_reason="schedule_next_initial_outbox_batch",
        )
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception(
            "schedule_next_initial_outbox_batch failed campaign_id=%s", cid
        )
        return 0, "persist_failed"

    logger.info(
        json.dumps(
            {
                "event": "schedule_next_initial_outbox_batch",
                "campaign_id": cid,
                "rows_enqueued": n,
            },
            ensure_ascii=False,
        )
    )
    return n, None


def diagnose_initial_outbox_enqueue(conn, campaign_id: int) -> dict:
    """Contagens para admin quando enfileirar outbox falha."""
    from psycopg2.extras import RealDictCursor

    cid = int(campaign_id)
    quota = initial_chunk_quota_snapshot(cid)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT status, COUNT(*)::int AS n
            FROM campaign_message_outbox
            WHERE campaign_id = %s AND LOWER(TRIM(stage)) = 'initial'
            GROUP BY status
            """,
            (cid,),
        )
        outbox_by_status = {
            r["status"]: int(r["n"]) for r in (cur.fetchall() or [])
        }
        cur.execute(
            """
            SELECT COUNT(*)::int AS n FROM campaign_leads
            WHERE campaign_id = %s AND status = 'pending' AND current_step = 1
              AND COALESCE(removed_from_funnel, FALSE) = FALSE
            """,
            (cid,),
        )
        row = cur.fetchone()
        pending_step1 = int((row or {}).get("n") or 0)
        eligible = _count_eligible_pending_leads_without_initial_outbox(conn, cid)
    return {
        "quota": quota,
        "outbox_initial_by_status": outbox_by_status,
        "pending_leads_step1": pending_step1,
        "eligible_without_outbox_row": eligible,
    }


def _load_campaign_row_for_outbox_schedule(
    conn, campaign_id: int
) -> Optional[dict]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, name, user_id, status, enable_cadence,
                   send_hour_start, send_hour_end, send_saturday, send_sunday,
                   use_uazapi_sender, scheduled_start, rotation_mode, daily_limit
            FROM campaigns
            WHERE id = %s
            """,
            (campaign_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def _try_schedule_next_initial_outbox_after_batch(conn, campaign_id: int) -> None:
    """
    Quando o lote initial atual termina (sem pending/sending na outbox), tenta enfileirar
    o próximo lote no mesmo tick — no dia seguinte a cota diária já liberou.
    """
    if not USE_MESSAGE_OUTBOX:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM campaign_message_outbox
            WHERE campaign_id = %s AND LOWER(TRIM(stage)) = 'initial'
              AND status IN ('pending', 'sending', 'waiting_instance')
            LIMIT 1
            """,
            (campaign_id,),
        )
        if cur.fetchone():
            return
    camp = _load_campaign_row_for_outbox_schedule(conn, campaign_id)
    if not camp or (camp.get("status") or "") not in ("running", "pending"):
        return
    n, _reason = schedule_next_initial_outbox_batch(conn, camp)
    if n > 0:
        print(
            f"  📤 [Initial Outbox] Campaign '{camp.get('name')}': "
            f"próximo lote enfileirado ({n}) após conclusão do lote anterior"
        )


def maybe_schedule_outbox_initial_batches(conn) -> None:
    """
    Varre campanhas Uazapi ``running``/``pending`` com leads initial pendentes e tenta
    enfileirar o próximo lote. Chamado a cada tick do cadence (day-2+ e campanhas sem
    linhas prévias na outbox).
    """
    if not USE_MESSAGE_OUTBOX:
        return
    try:
        # Tick anterior pode ter deixado a conexão em estado abortado.
        conn.rollback()
    except Exception:
        pass
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT c.id, c.name, c.user_id, c.status, c.enable_cadence,
                   c.send_hour_start, c.send_hour_end, c.send_saturday, c.send_sunday,
                   c.use_uazapi_sender, c.scheduled_start, c.rotation_mode, c.daily_limit
            FROM campaigns c
            WHERE c.status IN ('running', 'pending')
              AND COALESCE(c.use_uazapi_sender, FALSE) = TRUE
              AND EXISTS (
                  SELECT 1 FROM campaign_leads cl
                  WHERE cl.campaign_id = c.id
                    AND cl.status = 'pending'
                    AND cl.current_step = 1
                    AND COALESCE(cl.removed_from_funnel, FALSE) = FALSE
                    AND COALESCE(cl.cadence_status, 'active') NOT IN ('converted', 'lost')
                  LIMIT 1
              )
            ORDER BY c.id ASC
            """
        )
        campaigns = cur.fetchall() or []
    for camp in campaigns:
        camp = dict(camp)
        try:
            n, reason = schedule_next_initial_outbox_batch(conn, camp)
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            logger.exception(
                "maybe_schedule_outbox_initial_batches failed campaign_id=%s",
                camp.get("id"),
            )
            continue
        if n > 0:
            print(
                f"  📤 [Initial Outbox] Campaign '{camp.get('name')}': "
                f"enfileirados {n} envio(s) initial (outbox /send/text)"
                + (f" reason={reason}" if reason else "")
            )
        elif reason and reason not in (
            "daily_quota_exceeded",
            "initial_outbox_already_queued",
            "initial_outbox_pending_scheduled",
            "initial_outbox_pending_active",
            "initial_outbox_sending",
        ):
            _log_outbox_schedule_skip(int(camp["id"]), str(reason))


def enqueue_missing_cadence_outbox_rows(conn) -> None:
    """
    Para campanhas Uazapi com fila outbox e ``enable_cadence``: enfileira follow1/follow2/breakup
    quando o lead já passou pelo buffer (snoozed) e ``snooze_until`` expirou.
    Ordem de inserção = ``csv_row_order`` (mesma hierarquia visual do Kanban).
    """
    if not USE_MESSAGE_OUTBOX:
        return
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT c.id AS campaign_id, c.user_id,
                   COALESCE(NULLIF(TRIM(c.rotation_mode), ''), 'single') AS rotation_mode
            FROM campaigns c
            WHERE c.status IN ('running', 'pending')
              AND COALESCE(c.enable_cadence, FALSE) = TRUE
              AND COALESCE(c.use_uazapi_sender, FALSE) = TRUE
              AND EXISTS (SELECT 1 FROM campaign_message_outbox o WHERE o.campaign_id = c.id LIMIT 1)
            """
        )
        campaigns = cur.fetchall() or []

    base_utc = datetime.utcnow()
    for camp in campaigns:
        cid = int(camp["campaign_id"])
        uid = int(camp["user_id"])
        rotation_mode = (camp.get("rotation_mode") or "single").strip().lower()
        if rotation_mode not in ("single", "round_robin"):
            rotation_mode = "single"

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT i.id AS instance_id, i.apikey
                FROM campaign_instances ci
                JOIN instances i ON i.id = ci.instance_id
                WHERE ci.campaign_id = %s
                  AND COALESCE(i.api_provider, 'megaapi') = 'uazapi'
                  AND i.apikey IS NOT NULL
                  AND TRIM(i.apikey) <> ''
                ORDER BY i.id ASC
                """,
                (cid,),
            )
            instances = cur.fetchall() or []
        if not instances:
            continue
        n_inst = len(instances)

        for stage, lead_step, step_priority, msg_step in _CADENCE_OUTBOX_STAGES:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT 1 FROM campaign_steps
                    WHERE campaign_id = %s AND step_number = %s
                    LIMIT 1
                    """,
                    (cid, msg_step),
                )
                if not cur.fetchone():
                    continue

                cur.execute(
                    """
                    SELECT cl.id, COALESCE(cl.csv_row_order, cl.id) AS ord_key
                    FROM campaign_leads cl
                    WHERE cl.campaign_id = %s
                      AND cl.current_step = %s
                      AND cl.cadence_status = 'snoozed'
                      AND cl.snooze_until IS NOT NULL
                      AND cl.snooze_until <= NOW()
                      AND COALESCE(cl.removed_from_funnel, FALSE) = FALSE
                      AND COALESCE(cl.cadence_status, '') NOT IN ('converted', 'lost')
                      AND NOT EXISTS (
                          SELECT 1 FROM campaign_message_outbox o
                          WHERE o.campaign_lead_id = cl.id
                            AND LOWER(TRIM(o.stage)) = LOWER(%s)
                            AND o.status IN ('pending', 'sending', 'sent')
                      )
                    ORDER BY COALESCE(cl.csv_row_order, cl.id) ASC, cl.id ASC
                    """,
                    (cid, lead_step, stage),
                )
                leads_to_queue = cur.fetchall() or []

            if not leads_to_queue:
                continue

            try:
                with conn.cursor() as cur:
                    for i, lr in enumerate(leads_to_queue):
                        lead_id = int(lr["id"])
                        if rotation_mode == "round_robin":
                            inst = instances[i % n_inst]
                        else:
                            inst = instances[0]
                        instance_id = int(inst["instance_id"])
                        idempotency_key = f"campaign-{cid}-lead-{lead_id}-{stage}"
                        payload_summary = json.dumps(
                            {
                                "stage": stage,
                                "enqueue": "cadence_followup_auto",
                                "rotation_mode": rotation_mode,
                            },
                            ensure_ascii=False,
                        )
                        queued_at = base_utc + timedelta(
                            seconds=i // 1_000_000, microseconds=i % 1_000_000
                        )
                        cur.execute(
                            """
                            INSERT INTO campaign_message_outbox (
                                campaign_id, campaign_lead_id, instance_id,
                                stage, step_priority, status, queued_at,
                                next_run_at, idempotency_key, payload_summary
                            )
                            VALUES (
                                %s, %s, %s, %s, %s, 'pending', %s,
                                NOW(), %s, %s::jsonb
                            )
                            ON CONFLICT (campaign_lead_id, stage) DO NOTHING
                            """,
                            (
                                cid,
                                lead_id,
                                instance_id,
                                stage,
                                step_priority,
                                queued_at,
                                idempotency_key,
                                payload_summary,
                            ),
                        )
                conn.commit()
            except Exception:
                conn.rollback()
                logger.exception(
                    "enqueue_missing_cadence_outbox_rows failed campaign_id=%s stage=%s",
                    cid,
                    stage,
                )


_STAGE_STEP_PRIORITY = {"initial": 0, "follow1": 1, "follow2": 2, "breakup": 3}


def enqueue_outbox_rows_for_leads(
    conn,
    *,
    campaign_id: int,
    stage: str,
    lead_ids: list[int],
    instance_id: int,
    enqueue_source: str,
    next_run_at: Optional[datetime] = None,
    supersede_send_id: Optional[int] = None,
) -> int:
    """
    Enfileira leads na outbox com idempotency_key estável (legado → outbox / materialize).
    Retorna número de linhas inseridas (ON CONFLICT ignorado).
    """
    if not USE_MESSAGE_OUTBOX or not lead_ids:
        return 0
    cid = int(campaign_id)
    st = (stage or "initial").strip().lower()
    step_priority = _STAGE_STEP_PRIORITY.get(st, 0)
    iid = int(instance_id)
    now_utc = datetime.utcnow()
    nr = next_run_at if next_run_at is not None else now_utc
    enqueued = 0
    with conn.cursor() as cur:
        for i, lead_id in enumerate(lead_ids):
            lid = int(lead_id)
            idempotency_key = f"campaign-{cid}-lead-{lid}-{st}"
            payload_summary = json.dumps(
                {"stage": st, "enqueue": enqueue_source},
                ensure_ascii=False,
            )
            queued_at_val = now_utc + timedelta(
                seconds=i // 1_000_000, microseconds=i % 1_000_000
            )
            cur.execute(
                """
                INSERT INTO campaign_message_outbox (
                    campaign_id, campaign_lead_id, instance_id,
                    stage, step_priority, status, queued_at,
                    next_run_at, idempotency_key, payload_summary
                )
                VALUES (
                    %s, %s, %s, %s, %s, 'pending',
                    %s, %s, %s, %s::jsonb
                )
                ON CONFLICT (campaign_lead_id, stage) DO NOTHING
                """,
                (
                    cid,
                    lid,
                    iid,
                    st,
                    step_priority,
                    queued_at_val,
                    nr,
                    idempotency_key,
                    payload_summary,
                ),
            )
            if cur.rowcount:
                enqueued += 1
        if supersede_send_id is not None:
            cur.execute(
                """
                UPDATE campaign_stage_sends
                SET status = 'failed',
                    last_materialize_error = COALESCE(
                        NULLIF(TRIM(last_materialize_error), ''),
                        'outbox: legacy materialize migrated'
                    ),
                    updated_at = NOW()
                WHERE id = %s
                  AND uazapi_folder_id IS NULL
                """,
                (int(supersede_send_id),),
            )
    return enqueued


def supersede_legacy_follow_stage_sends(
    conn, campaign_id: int, stage: str, scheduled_for
) -> int:
    """Marca sends follow agendados (sem pasta) como superseded — cadência via outbox."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE campaign_stage_sends
            SET status = 'failed',
                last_materialize_error = COALESCE(
                    NULLIF(TRIM(last_materialize_error), ''),
                    'outbox: follow-up via enqueue_missing_cadence_outbox_rows'
                ),
                updated_at = NOW()
            WHERE campaign_id = %s
              AND stage = %s
              AND scheduled_for = %s
              AND status IN ('scheduled', 'waiting_reconnect')
              AND uazapi_folder_id IS NULL
            """,
            (int(campaign_id), stage, scheduled_for),
        )
        return cur.rowcount or 0


def _chunk_size_from_stage_send_row(row: dict) -> int:
    lj = row.get("lead_ids")
    if isinstance(lj, list):
        return len(lj)
    if isinstance(lj, str):
        try:
            parsed = json.loads(lj)
            if isinstance(parsed, list):
                return len(parsed)
        except Exception:
            pass
    try:
        return max(0, int(row.get("planned_count") or 0))
    except (TypeError, ValueError):
        return 0


def _legacy_initial_outbox_offset(conn, campaign_id: int) -> int:
    """Posições consumidas por chunks initial legados (mesma regra do script de migração)."""
    from psycopg2.extras import RealDictCursor

    offset = 0
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT status, planned_count, success_count, failed_count, lead_ids
            FROM campaign_stage_sends
            WHERE campaign_id = %s AND stage = 'initial'
            ORDER BY id ASC
            """,
            (int(campaign_id),),
        )
        rows = cur.fetchall() or []
    for row in rows:
        st = (row.get("status") or "").lower()
        n = _chunk_size_from_stage_send_row(row)
        sc = int(row.get("success_count") or 0)
        fc = int(row.get("failed_count") or 0)
        if st in ("scheduled", "failed", "queued"):
            offset += n
        elif st == "done":
            offset += sc if sc > 0 else n
        elif st in ("running", "partial"):
            touched = sc + fc
            offset += touched if touched > 0 else n
    return offset


def reconcile_in_flight_legacy_initial_to_outbox(
    conn, *, max_campaigns: int = 10
) -> int:
    """
    Campanhas com chunks initial legados em voo: após sync, enfileira leads pendentes
  restantes na outbox (idempotency_key por lead+stage).
    """
    if not USE_MESSAGE_OUTBOX or max_campaigns <= 0:
        return 0
    from psycopg2.extras import RealDictCursor

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT DISTINCT css.campaign_id
            FROM campaign_stage_sends css
            JOIN campaigns c ON c.id = css.campaign_id
            WHERE css.stage = 'initial'
              AND css.status IN ('running', 'partial', 'scheduled', 'queued')
              AND c.status IN ('running', 'pending')
              AND COALESCE(c.use_uazapi_sender, FALSE) = TRUE
            ORDER BY css.campaign_id ASC
            LIMIT %s
            """,
            (max_campaigns,),
        )
        campaign_ids = [int(r["campaign_id"]) for r in (cur.fetchall() or [])]

    total = 0
    for cid in campaign_ids:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT cl.id
                FROM campaign_leads cl
                WHERE cl.campaign_id = %s
                  AND cl.status = 'pending'
                  AND cl.current_step = 1
                  AND COALESCE(cl.removed_from_funnel, FALSE) = FALSE
                  AND COALESCE(cl.cadence_status, 'active') NOT IN ('converted', 'lost')
                  AND NOT EXISTS (
                      SELECT 1 FROM campaign_message_outbox o
                      WHERE o.campaign_lead_id = cl.id
                        AND LOWER(TRIM(o.stage)) = 'initial'
                        AND o.status IN ('pending', 'sending', 'sent')
                  )
                ORDER BY COALESCE(cl.send_batch, 999) ASC,
                         COALESCE(cl.csv_row_order, cl.id) ASC,
                         cl.id ASC
                """,
                (cid,),
            )
            pending = [int(r["id"]) for r in (cur.fetchall() or [])]
        if not pending:
            continue

        offset = _legacy_initial_outbox_offset(conn, cid)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id
                FROM campaign_leads
                WHERE campaign_id = %s
                  AND COALESCE(removed_from_funnel, FALSE) = FALSE
                ORDER BY COALESCE(send_batch, 999) ASC,
                         COALESCE(csv_row_order, id) ASC,
                         id ASC
                """,
                (cid,),
            )
            ordered = [int(r["id"]) for r in (cur.fetchall() or [])]
        to_enqueue = [lid for lid in ordered[offset:] if lid in set(pending)]
        if not to_enqueue:
            continue

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT i.id AS instance_id
                FROM campaign_instances ci
                JOIN instances i ON i.id = ci.instance_id
                WHERE ci.campaign_id = %s
                  AND COALESCE(i.api_provider, 'megaapi') = 'uazapi'
                  AND i.apikey IS NOT NULL
                  AND TRIM(i.apikey) <> ''
                ORDER BY i.id ASC
                LIMIT 1
                """,
                (cid,),
            )
            inst = cur.fetchone()
        if not inst:
            continue
        n = enqueue_outbox_rows_for_leads(
            conn,
            campaign_id=cid,
            stage="initial",
            lead_ids=to_enqueue,
            instance_id=int(inst["instance_id"]),
            enqueue_source="reconcile_in_flight_legacy_initial",
        )
        if n:
            total += n
            logger.info(
                json.dumps(
                    {
                        "event": "reconcile_in_flight_legacy_initial",
                        "campaign_id": cid,
                        "rows_enqueued": n,
                    },
                    ensure_ascii=False,
                )
            )
    return total


def _defer_row_next_window(
    conn,
    outbox_id: int,
    campaign_dict: dict,
    *,
    campaign_id: Optional[int] = None,
    reason: str = "outside_send_window",
) -> None:
    try:
        nv = next_valid_send_utc_naive(campaign_dict, datetime.utcnow(), margin_minutes=0)
    except Exception:
        nv = datetime.utcnow() + timedelta(minutes=30)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE campaign_message_outbox
            SET next_run_at = %s, updated_at = NOW()
            WHERE id = %s
            """,
            (nv, outbox_id),
        )
    logger.info(
        json.dumps(
            {
                "event": "outbox_next_day_deferred",
                "campaign_id": campaign_id,
                "outbox_id": outbox_id,
                "reason": reason,
                "next_run_at": nv.isoformat(),
            },
            ensure_ascii=False,
        )
    )


def _defer_initial_row_daily_quota(conn, row: dict) -> None:
    """Adia envio initial para a próxima janela BRT quando a cota diária esgotou."""
    row = dict(row)
    cd = _campaign_dict_from_row(row)
    cid = int(row["campaign_id"])
    oid = int(row["id"])
    try:
        nv = next_valid_send_after_current_window_utc_naive(cd, datetime.utcnow())
    except ValueError:
        _defer_row_retry_later(conn, oid, minutes=60)
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE campaign_message_outbox
            SET next_run_at = %s, updated_at = NOW()
            WHERE id = %s AND status = 'pending'
            """,
            (nv, oid),
        )
    quota = initial_chunk_quota_snapshot(cid)
    _log_outbox_daily_quota_reached(cid, quota)
    logger.info(
        json.dumps(
            {
                "event": "outbox_next_day_deferred",
                "campaign_id": cid,
                "outbox_id": oid,
                "reason": "daily_quota",
                "next_run_at": nv.isoformat(),
            },
            ensure_ascii=False,
        )
    )


def _defer_row_retry_later(conn, outbox_id: int, minutes: int = 45) -> None:
    nv = datetime.utcnow() + timedelta(minutes=minutes)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE campaign_message_outbox
            SET next_run_at = %s, updated_at = NOW()
            WHERE id = %s
            """,
            (nv, outbox_id),
        )


def _reaper_stale_sending(conn) -> None:
    if _REAPER_MINUTES <= 0:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE campaign_message_outbox
            SET status = 'pending',
                updated_at = NOW()
            WHERE status = 'sending'
              AND updated_at < (NOW() - (%s * INTERVAL '1 minute'))
            """,
            (_REAPER_MINUTES,),
        )


def _passes_throttle_initial(c: dict) -> bool:
    if (c.get("stage") or "").lower() != "initial":
        return True
    return check_initial_chunk_daily_quota_for_campaign(int(c["campaign_id"]))


def _load_step_row(conn, campaign_id: int, step_number: int) -> dict:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT step_number, message_template, media_path, media_type
            FROM campaign_steps
            WHERE campaign_id = %s AND step_number = %s
            LIMIT 1
            """,
            (campaign_id, step_number),
        )
        row = cur.fetchone()
    return dict(row) if row else {}


def _pick_message_text(conn, campaign_id: int, step_number: int) -> str:
    row = _load_step_row(conn, campaign_id, step_number)
    raw = row.get("message_template") or ""
    msgs = _parse_message_template(raw)
    if msgs:
        return random.choice(msgs)
    if step_number == 1:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT message_template FROM campaigns WHERE id = %s LIMIT 1",
                (campaign_id,),
            )
            crow = cur.fetchone() or {}
        msgs = _parse_message_template(crow.get("message_template") or "")
        if msgs:
            return random.choice(msgs)
    return ""


def _apply_lead_success(
    cur,
    *,
    campaign_id: int,
    lead_id: int,
    stage: str,
    instance_name: str,
    enable_cadence: bool,
) -> None:
    st = (stage or "initial").lower()
    step_n = STAGE_TO_STEP_NUMBER.get(st, 1)

    if st == "initial":
        if enable_cadence:
            cur.execute(
                """
                UPDATE campaign_leads
                SET status = 'sent',
                    sent_at = NOW(),
                    last_sent_stage = 'initial',
                    sent_by_instance = %s,
                    last_message_sent_at = NOW(),
                    current_step = 2,
                    cadence_status = 'monitoring'
                WHERE id = %s AND campaign_id = %s
                """,
                (instance_name, lead_id, campaign_id),
            )
        else:
            cur.execute(
                """
                UPDATE campaign_leads
                SET status = 'sent',
                    sent_at = NOW(),
                    last_sent_stage = 'initial',
                    sent_by_instance = %s,
                    last_message_sent_at = NOW()
                WHERE id = %s AND campaign_id = %s
                """,
                (instance_name, lead_id, campaign_id),
            )
        return

    next_step = min(step_n + 1, 32)
    cur.execute(
        """
        UPDATE campaign_leads
        SET status = 'sent',
            sent_at = NOW(),
            last_sent_stage = %s,
            sent_by_instance = %s,
            last_message_sent_at = NOW(),
            current_step = %s,
            cadence_status = 'monitoring'
        WHERE id = %s AND campaign_id = %s
        """,
        (st, instance_name, next_step, lead_id, campaign_id),
    )


def process_message_outbox_tick(conn) -> None:
    """
    Um envio por tick (global). Feature flag ``USE_MESSAGE_OUTBOX`` (utils.config).
    """
    global _last_outbox_instance_id

    if not USE_MESSAGE_OUTBOX:
        return
    if not uazapi_service:
        return

    _reaper_stale_sending(conn)
    _recover_stale_pending_initial_outbox(conn)
    conn.commit()

    enqueue_missing_cadence_outbox_rows(conn)
    reconcile_in_flight_legacy_initial_to_outbox(conn)
    conn.commit()

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT o.id, o.campaign_id, o.campaign_lead_id, o.instance_id, o.stage, o.step_priority,
                   o.queued_at, o.next_run_at, o.idempotency_key, o.payload_summary,
                   c.user_id, c.enable_cadence, c.status AS campaign_status,
                   COALESCE(NULLIF(TRIM(c.rotation_mode), ''), 'single') AS rotation_mode,
                   c.send_hour_start, c.send_hour_end, c.send_saturday, c.send_sunday,
                   c.scheduled_start, c.outbox_delay_min_seconds, c.outbox_delay_max_seconds,
                   cl.phone, cl.name AS lead_name, cl.csv_row_order,
                   i.apikey, i.name AS instance_name,
                   COALESCE(i.api_provider, 'megaapi') AS api_provider
            FROM campaign_message_outbox o
            JOIN campaigns c ON c.id = o.campaign_id
            JOIN campaign_leads cl ON cl.id = o.campaign_lead_id
            JOIN instances i ON i.id = o.instance_id
            WHERE o.status IN ('pending', 'waiting_instance')
              AND o.next_run_at <= NOW()
              AND c.status IN ('running', 'pending')
              AND (c.scheduled_start IS NULL OR c.scheduled_start <= NOW())
            ORDER BY o.step_priority ASC,
                     COALESCE(cl.csv_row_order, cl.id) ASC,
                     cl.id ASC,
                     o.next_run_at ASC,
                     o.id ASC
            LIMIT 300
            """
        )
        rows = cur.fetchall() or []

    if not rows:
        return

    in_window: list[dict] = []
    for r in rows:
        r = dict(r)
        cd = _campaign_dict_from_row(r)
        if is_campaign_send_window(cd):
            in_window.append(r)

    if not in_window:
        first = dict(rows[0])
        _defer_row_next_window(
            conn,
            int(first["id"]),
            _campaign_dict_from_row(first),
            campaign_id=int(first["campaign_id"]),
            reason="outside_send_window",
        )
        conn.commit()
        return

    candidates: list[dict] = []
    for r in in_window:
        if r.get("api_provider") != "uazapi" or not (r.get("apikey") or "").strip():
            continue
        if not _passes_throttle_initial(r):
            _defer_initial_row_daily_quota(conn, r)
            conn.commit()
            return
        candidates.append(r)

    if not candidates:
        return

    chosen = _choose_outbox_row(candidates)

    outbox_id = int(chosen["id"])
    campaign_id = int(chosen["campaign_id"])
    lead_id = int(chosen["campaign_lead_id"])
    user_id = int(chosen["user_id"])
    stage = (chosen.get("stage") or "initial").lower()
    step_number = STAGE_TO_STEP_NUMBER.get(stage, 1)
    track_id = (chosen.get("idempotency_key") or "").strip() or f"outbox-{outbox_id}"
    track_source = "campaign_message_outbox"

    # --- (A) Claim curto ---
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, status FROM campaign_message_outbox
                WHERE id = %s AND status IN ('pending', 'waiting_instance')
                FOR UPDATE SKIP LOCKED
                """,
                (outbox_id,),
            )
            lk = cur.fetchone()
            if not lk:
                conn.rollback()
                return
            cur.execute(
                """
                UPDATE campaign_message_outbox
                SET status = 'sending', updated_at = NOW()
                WHERE id = %s AND status IN ('pending', 'waiting_instance')
                """,
                (outbox_id,),
            )
            if cur.rowcount != 1:
                conn.rollback()
                return
        conn.commit()
    except Exception:
        conn.rollback()
        return

    _last_outbox_instance_id = int(chosen["instance_id"])

    phone_num = _normalize_phone_e164_br(chosen.get("phone"))
    token = (chosen.get("apikey") or "").strip()
    message = _pick_message_text(conn, campaign_id, step_number)
    lead_name = (chosen.get("lead_name") or "Visitante").strip()
    message = (
        message.replace("{{nome}}", lead_name)
        .replace("{{name}}", lead_name)
        .replace("{nome}", lead_name)
        .replace("{name}", lead_name)
    )

    step_row = _load_step_row(conn, campaign_id, step_number)
    media_path = step_row.get("media_path") or ""
    media_type = (step_row.get("media_type") or "image").lower()
    is_sa = _user_is_superadmin(conn, user_id)

    if not phone_num:
        latency_ms = 0
        _persist_outcome(
            conn,
            outbox_id=outbox_id,
            campaign_id=campaign_id,
            lead_id=lead_id,
            chosen=chosen,
            http_status=None,
            response_body="missing_phone",
            outcome="failed",
            latency_ms=latency_ms,
            success=False,
            track_from_response=None,
            audit_request={"kind": "none", "reason": "missing_phone"},
            audit_response_body=None,
        )
        return

    started = time.monotonic()
    http_status: Optional[int] = None
    response_body: Optional[str] = None
    result_json: Optional[dict] = None

    audit_request: dict[str, Any]
    # --- (B) HTTP fora de transação ---
    if (
        media_path
        and is_sa
        and _is_media_path_safe(media_path, user_id)
        and os.path.exists(media_path)
    ):
        audit_request = {
            "kind": "media",
            "number": phone_num,
            "media_type": media_type if media_type in ("image", "video") else "image",
            "media_file": os.path.basename(media_path) or media_path,
            "caption_preview": (message or "")[:400],
            "track_id": track_id,
            "track_source": track_source,
        }
        result_json = uazapi_service.send_media_campaign(
            token,
            phone_num or "",
            media_type if media_type in ("image", "video") else "image",
            media_path,
            caption=message or "",
            track_id=track_id,
            track_source=track_source,
        )
        normalized = _normalize_uazapi_send_result(result_json)
        http_status = normalized["http_status"]
        response_body = normalized["response_body"]
        audit_response_body = normalized["audit_response_body"]
    else:
        if not message.strip():
            latency_ms = int((time.monotonic() - started) * 1000)
            _persist_outcome(
                conn,
                outbox_id=outbox_id,
                campaign_id=campaign_id,
                lead_id=lead_id,
                chosen=chosen,
                http_status=None,
                response_body="missing_message_template",
                outcome="failed",
                latency_ms=latency_ms,
                success=False,
                track_from_response=None,
                audit_request={
                    "kind": "text",
                    "reason": "missing_message_template",
                    "number": phone_num,
                    "step_number": step_number,
                },
                audit_response_body=None,
            )
            return
        audit_request = {
            "kind": "text",
            "number": phone_num,
            "text_preview": message[:500],
            "track_id": track_id,
            "track_source": track_source,
        }
        result_json = uazapi_service.send_text_idempotent(
            token,
            phone_num or "",
            message,
            track_id=track_id,
            track_source=track_source,
        )
        normalized = _normalize_uazapi_send_result(result_json)
        http_status = normalized["http_status"]
        response_body = normalized["response_body"]
        audit_response_body = normalized["audit_response_body"]

    latency_ms = int((time.monotonic() - started) * 1000)
    success = normalized["success"]

    _persist_outcome(
        conn,
        outbox_id=outbox_id,
        campaign_id=campaign_id,
        lead_id=lead_id,
        chosen=chosen,
        http_status=http_status,
        response_body=response_body,
        outcome="sent" if success else "failed",
        latency_ms=latency_ms,
        success=success,
        track_from_response=normalized["result_json_classify"] if success else None,
        audit_request=audit_request,
        audit_response_body=audit_response_body,
    )


def _persist_outcome(
    conn,
    *,
    outbox_id: int,
    campaign_id: int,
    lead_id: int,
    chosen: dict,
    http_status: Optional[int],
    response_body: Optional[str],
    outcome: str,
    latency_ms: int,
    success: bool,
    track_from_response: Any,
    audit_request: Optional[dict] = None,
    audit_response_body: Any = None,
) -> None:
    """Fase (C): tentativa + estado; §6.1 só promove lead/envio se ``success``.
    Falhas classificadas como transitórias voltam a ``pending`` com ``next_run_at`` em backoff;
    só ``terminal`` fecha a linha como ``failed``."""
    stage = (chosen.get("stage") or "initial").lower()
    enable_cadence = bool(chosen.get("enable_cadence"))
    instance_name = chosen.get("instance_name") or ""
    track_stored = None
    if isinstance(track_from_response, dict):
        track_stored = (
            track_from_response.get("messageId")
            or track_from_response.get("message_id")
            or track_from_response.get("id")
        )
        if track_stored is not None:
            track_stored = str(track_stored)
    if not track_stored:
        track_stored = (chosen.get("idempotency_key") or "") or None

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT COALESCE(MAX(attempt_no), 0) AS m FROM campaign_send_attempts WHERE outbox_id = %s",
            (outbox_id,),
        )
        attempt_no = int((cur.fetchone() or {}).get("m") or 0) + 1

    result_json_classify = _result_json_for_classify(response_body, audit_response_body)
    failure_kind: Optional[str] = None
    next_retry_at_iso: Optional[str] = None
    if success:
        attempt_row_outcome = outcome
    else:
        kind = classify_outbox_send_failure(
            http_status,
            response_body,
            result_json_classify,
            None,
        )
        failure_kind = kind
        if kind == "terminal":
            attempt_row_outcome = "failed_terminal"
        elif kind == "instance_unreachable":
            attempt_row_outcome = "retry_scheduled"
        else:
            prior_retries = _count_retry_scheduled_attempts(conn, outbox_id)
            if prior_retries >= _OUTBOX_MAX_ATTEMPTS - 1:
                attempt_row_outcome = "failed_max_attempts"
            else:
                attempt_row_outcome = "retry_scheduled"

    error_summary = (
        None
        if success
        else _error_summary_from_send_failure(
            http_status=http_status,
            response_body=response_body,
            result_json=result_json_classify,
            failure_kind=failure_kind,
        )
    )

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO campaign_send_attempts
                    (outbox_id, attempt_no, http_status, uazapi_response, outcome, latency_ms, finished_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                """,
                (
                    outbox_id,
                    attempt_no,
                    http_status,
                    _truncate(response_body),
                    attempt_row_outcome,
                    latency_ms,
                ),
            )

            if success:
                cur.execute(
                    """
                    UPDATE campaign_message_outbox
                    SET status = 'sent',
                        uazapi_track_id = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (track_stored, outbox_id),
                )
                _apply_lead_success(
                    cur,
                    campaign_id=campaign_id,
                    lead_id=lead_id,
                    stage=stage,
                    instance_name=instance_name,
                    enable_cadence=enable_cadence,
                )
                cur.execute(
                    """
                    UPDATE campaigns
                    SET sent_today = COALESCE(sent_today, 0) + 1
                    WHERE id = %s
                    """,
                    (campaign_id,),
                )

                dmin = int(chosen.get("outbox_delay_min_seconds") or 600)
                dmax = int(chosen.get("outbox_delay_max_seconds") or 900)
                lo, hi = min(dmin, dmax), max(dmin, dmax)
                delta_sec = random.randint(lo, hi)
                cur.execute(
                    """
                    UPDATE campaign_message_outbox
                    SET next_run_at = GREATEST(next_run_at, NOW() + (%s * INTERVAL '1 second')),
                        updated_at = NOW()
                    WHERE campaign_id = %s
                      AND status = 'pending'
                      AND id <> %s
                    """,
                    (delta_sec, campaign_id, outbox_id),
                )
            else:
                if attempt_row_outcome in ("failed_terminal", "failed_max_attempts"):
                    cur.execute(
                        """
                        UPDATE campaign_message_outbox
                        SET status = 'failed',
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (outbox_id,),
                    )
                else:
                    delay_sec = _outbox_retry_backoff_seconds(attempt_no)
                    outbox_status = (
                        "waiting_instance"
                        if failure_kind == "instance_unreachable"
                        else "pending"
                    )
                    next_retry_at_iso = (
                        datetime.utcnow() + timedelta(seconds=delay_sec)
                    ).isoformat() + "Z"
                    cur.execute(
                        """
                        UPDATE campaign_message_outbox
                        SET status = %s,
                            next_run_at = NOW() + (%s * INTERVAL '1 second'),
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (outbox_status, delay_sec, outbox_id),
                    )

        conn.commit()
        _log_outbox_attempt_event(
            campaign_id=campaign_id,
            outbox_id=outbox_id,
            instance_id=int(chosen.get("instance_id") or 0),
            latency_ms=latency_ms,
            outcome=attempt_row_outcome,
            http_status=http_status,
        )
        try:
            uid_audit = int(chosen.get("user_id") or 0)
            if uid_audit:
                append_dispatch_audit_event(
                    user_id=uid_audit,
                    campaign_id=campaign_id,
                    event={
                        "campaign_id": campaign_id,
                        "lead_id": lead_id,
                        "csv_row_order": chosen.get("csv_row_order"),
                        "outbox_id": outbox_id,
                        "stage": stage,
                        "attempt_no": attempt_no,
                        "instance_id": int(chosen.get("instance_id") or 0),
                        "outcome": attempt_row_outcome,
                        "latency_ms": latency_ms,
                        "http_status": http_status,
                        "failure_kind": failure_kind,
                        "error_summary": error_summary,
                        "max_attempts": _OUTBOX_MAX_ATTEMPTS,
                        "next_retry_at": next_retry_at_iso,
                        "request": audit_request or {},
                        "response": audit_response_body
                        if audit_response_body is not None
                        else _truncate(response_body, 4000),
                    },
                )
        except Exception:
            logger.exception(
                "append_dispatch_audit_event failed campaign_id=%s outbox_id=%s",
                campaign_id,
                outbox_id,
            )
        if success and stage == "initial":
            _try_schedule_next_initial_outbox_after_batch(conn, campaign_id)
    except Exception as e:
        conn.rollback()
        observe_campaign_outbox_send_attempt("persist_failed", latency_ms)
        logger.error(
            "%s",
            json.dumps(
                {
                    "event": "campaign_outbox_send_attempt",
                    "campaign_id": campaign_id,
                    "outbox_id": outbox_id,
                    "instance_id": int(chosen.get("instance_id") or 0),
                    "latency_ms": latency_ms,
                    "outcome": "persist_failed",
                    "http_status": http_status,
                    "error_class": type(e).__name__,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE campaign_message_outbox
                    SET status = 'pending', updated_at = NOW()
                    WHERE id = %s AND status = 'sending'
                    """,
                    (outbox_id,),
                )
            conn.commit()
        except Exception:
            conn.rollback()


if __name__ == "__main__":
    c = get_db_connection()
    try:
        process_message_outbox_tick(c)
    finally:
        c.close()
