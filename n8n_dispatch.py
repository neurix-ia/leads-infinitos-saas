"""
Disparo via n8n: o Leads Infinitos fala o mesmo contrato do CRM Neurix com o workflow
``Disparador CRM Simple`` (n8n-neurix-1, webhook ``disparador-crm-start``).

Ligado por ``USE_N8N_DISPATCH=1``. Substitui o motor local (outbox/chunks) para campanhas
disparadas por aqui; o legado continua intocado para o resto.

Modelo: ``campaign_dispatches`` = uma etapa de uma campanha entregue ao n8n (é o ``campaign_id``
que o n8n enxerga); ``campaign_sends`` = um target por lead/etapa, com mensagem já sorteada e
instância já atribuída (round-robin na criação).

Endpoints n8n → LI (header ``X-CRM-API-Key`` = ``N8N_DISPATCH_API_KEY``):
  GET  /api/n8n/tools/dispatch-targets?campaign_id&status=pending&limit=1
  POST /api/n8n/tools/dispatch-execution      {campaign_id, execution_id}
  POST /api/n8n/tools/dispatch-target-status  {target_id, status, error}
  POST /api/n8n/tools/dispatch-complete       {campaign_id}
Endpoint UI → LI (login): POST /api/campaigns/<id>/n8n-dispatch/<step>
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

import requests
from flask import Blueprint, jsonify, request
from flask_login import current_user, login_required
from psycopg2.extras import RealDictCursor

from utils.config import (
    N8N_DISPATCH_API_KEY,
    N8N_DISPATCH_WEBHOOK_URL,
    PUBLIC_BASE_URL,
)
from utils.n8n_dispatch_gate import (
    STAGE_LABELS,
    STEP_TO_STAGE,
    api_key_matches,
    assign_round_robin,
    campaign_daily_cap,
    campaign_delays_seconds,
    compute_dispatch_gate,
    pick_variation,
)

logger = logging.getLogger(__name__)

n8n_dispatch_bp = Blueprint("n8n_dispatch", __name__)

# Injectado por app.py (evita import circular com o monólito).
_get_db_connection = None


def configure(get_db_connection) -> None:
    global _get_db_connection
    _get_db_connection = get_db_connection


def _conn():
    if _get_db_connection is None:
        raise RuntimeError("n8n_dispatch.configure(get_db_connection) não foi chamado")
    return _get_db_connection()


# ───────────────────────────── schema ─────────────────────────────

def ensure_schema(cur) -> None:
    """Chamado de ``init_db``. Idempotente."""
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS campaign_dispatches (
            id SERIAL PRIMARY KEY,
            campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
            step INTEGER NOT NULL,
            stage TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'scheduled'
                CHECK (status IN ('scheduled', 'running', 'done', 'failed', 'cancelled')),
            n8n_execution_id TEXT,
            total INTEGER NOT NULL DEFAULT 0,
            sent INTEGER NOT NULL DEFAULT 0,
            failed INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMP,
            finished_at TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_campaign_dispatches_campaign
            ON campaign_dispatches (campaign_id, step);

        CREATE TABLE IF NOT EXISTS campaign_sends (
            id SERIAL PRIMARY KEY,
            dispatch_id INTEGER NOT NULL REFERENCES campaign_dispatches(id) ON DELETE CASCADE,
            campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
            campaign_lead_id INTEGER NOT NULL REFERENCES campaign_leads(id) ON DELETE CASCADE,
            instance_id INTEGER REFERENCES instances(id) ON DELETE SET NULL,
            step INTEGER NOT NULL,
            stage TEXT NOT NULL,
            message TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'sent', 'failed', 'cancelled')),
            error TEXT,
            sent_at TIMESTAMP,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (campaign_lead_id, step)
        );
        CREATE INDEX IF NOT EXISTS idx_campaign_sends_dispatch_status
            ON campaign_sends (dispatch_id, status);
        CREATE INDEX IF NOT EXISTS idx_campaign_sends_campaign_sent_at
            ON campaign_sends (campaign_id, sent_at);
        """
    )


# ───────────────────────────── helpers ─────────────────────────────

def _require_api_key():
    provided = request.headers.get("X-CRM-API-Key")
    if not api_key_matches(provided, N8N_DISPATCH_API_KEY):
        return jsonify({"error": "unauthorized"}), 401
    return None


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _parse_message_template(raw: Any) -> list[str]:
    if not raw or not str(raw).strip():
        return []
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [str(x).strip() for x in parsed if str(x).strip()]
        if isinstance(parsed, str) and parsed.strip():
            return [parsed.strip()]
    except Exception:
        if isinstance(raw, str) and raw.strip():
            return [raw.strip()]
    return []


def _load_step_messages(cur, campaign_id: int, step: int) -> list[str]:
    cur.execute(
        "SELECT message_template FROM campaign_steps WHERE campaign_id = %s AND step_number = %s LIMIT 1",
        (campaign_id, step),
    )
    row = cur.fetchone() or {}
    msgs = _parse_message_template(row.get("message_template"))
    if msgs or step != 1:
        return msgs
    cur.execute("SELECT message_template FROM campaigns WHERE id = %s", (campaign_id,))
    row = cur.fetchone() or {}
    return _parse_message_template(row.get("message_template"))


def _load_campaign(cur, campaign_id: int) -> Optional[dict]:
    cur.execute(
        """
        SELECT id, user_id, name, status, daily_limit, rotation_mode,
               send_hour_start, send_hour_end, send_saturday, send_sunday,
               outbox_delay_min_seconds, outbox_delay_max_seconds
        FROM campaigns WHERE id = %s
        """,
        (campaign_id,),
    )
    return cur.fetchone()


def _load_uazapi_instances(cur, campaign_id: int) -> list[dict]:
    cur.execute(
        """
        SELECT i.id, i.apikey
        FROM campaign_instances ci
        JOIN instances i ON i.id = ci.instance_id
        WHERE ci.campaign_id = %s
          AND COALESCE(i.api_provider, 'megaapi') = 'uazapi'
          AND COALESCE(TRIM(i.apikey), '') <> ''
        ORDER BY i.id ASC
        """,
        (campaign_id,),
    )
    return list(cur.fetchall() or [])


def _sent_today_count(cur, campaign_id: int) -> int:
    """Envios confirmados hoje (BRT) na campanha, todas as etapas. Só a fila n8n conta aqui."""
    cur.execute(
        """
        SELECT COUNT(*) AS n FROM campaign_sends
        WHERE campaign_id = %s AND status = 'sent'
          AND date(sent_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')
              = date(NOW() AT TIME ZONE 'America/Sao_Paulo')
        """,
        (campaign_id,),
    )
    return int((cur.fetchone() or {}).get("n") or 0)


def _refresh_counters(cur, dispatch_id: int) -> dict:
    cur.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE status = 'sent') AS sent,
            COUNT(*) FILTER (WHERE status = 'failed') AS failed,
            COUNT(*) FILTER (WHERE status = 'pending') AS pending,
            COUNT(*) AS total
        FROM campaign_sends WHERE dispatch_id = %s
        """,
        (dispatch_id,),
    )
    c = cur.fetchone() or {}
    counters = {k: int(c.get(k) or 0) for k in ("sent", "failed", "pending", "total")}
    cur.execute(
        "UPDATE campaign_dispatches SET sent = %s, failed = %s, total = %s WHERE id = %s",
        (counters["sent"], counters["failed"], counters["total"], dispatch_id),
    )
    return counters


def _eligible_lead_ids(cur, campaign_id: int, step: int) -> list[int]:
    """
    Etapa 1: leads pendentes. Etapa N>1: quem recebeu a etapa N-1 por esta fila e ainda não
    recebeu a N, não saiu do funil e não foi parado pela cadência.
    """
    if step == 1:
        cur.execute(
            """
            SELECT cl.id FROM campaign_leads cl
            WHERE cl.campaign_id = %s
              AND cl.status IN ('pending', 'waiting_instance')
              AND COALESCE(cl.removed_from_funnel, FALSE) = FALSE
              AND NOT EXISTS (SELECT 1 FROM campaign_sends s WHERE s.campaign_lead_id = cl.id AND s.step = 1)
            ORDER BY COALESCE(cl.csv_row_order, cl.id), cl.id
            """,
            (campaign_id,),
        )
    else:
        cur.execute(
            """
            SELECT cl.id FROM campaign_leads cl
            JOIN campaign_sends prev ON prev.campaign_lead_id = cl.id AND prev.step = %s AND prev.status = 'sent'
            WHERE cl.campaign_id = %s
              AND COALESCE(cl.removed_from_funnel, FALSE) = FALSE
              AND COALESCE(cl.cadence_status, 'pending') <> 'stopped'
              AND cl.status <> 'replied'
              AND NOT EXISTS (SELECT 1 FROM campaign_sends s WHERE s.campaign_lead_id = cl.id AND s.step = %s)
            ORDER BY COALESCE(cl.csv_row_order, cl.id), cl.id
            """,
            (step - 1, campaign_id, step),
        )
    return [int(r["id"]) for r in (cur.fetchall() or [])]


def _trigger_n8n(payload: dict) -> tuple[bool, Optional[str]]:
    url = (N8N_DISPATCH_WEBHOOK_URL or "").strip()
    if not url:
        return False, "N8N_DISPATCH_WEBHOOK_URL não configurada"
    try:
        resp = requests.post(url, json=payload, timeout=30)
        if resp.status_code >= 400:
            return False, f"n8n webhook HTTP {resp.status_code}"
        return True, None
    except Exception as exc:  # noqa: BLE001
        logger.exception("Falha ao chamar webhook n8n: %s", exc)
        return False, str(exc)


# ───────────────────────────── start (UI → LI → n8n) ─────────────────────────────

def start_dispatch(campaign_id: int, step: int, *, user_id: Optional[int] = None) -> tuple[dict, int]:
    """Cria o dispatch da etapa, materializa targets e aciona o n8n. Retorna (json, http_status)."""
    if step not in STEP_TO_STAGE:
        return {"error": "step inválido (1–4)"}, 400
    stage = STEP_TO_STAGE[step]
    conn = _conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            campaign = _load_campaign(cur, campaign_id)
            if not campaign or (user_id is not None and int(campaign["user_id"]) != int(user_id)):
                return {"error": "campanha não encontrada"}, 404

            cur.execute(
                "SELECT id FROM campaign_dispatches WHERE campaign_id = %s AND step = %s AND status IN ('scheduled','running') LIMIT 1",
                (campaign_id, step),
            )
            if cur.fetchone():
                return {"error": f"já existe disparo em andamento para a etapa {step}"}, 409

            instances = _load_uazapi_instances(cur, campaign_id)
            if not instances:
                return {"error": "campanha sem instância Uazapi com token"}, 400
            messages = _load_step_messages(cur, campaign_id, step)
            if not messages:
                return {"error": f"etapa {step} sem mensagem"}, 400
            lead_ids = _eligible_lead_ids(cur, campaign_id, step)
            if not lead_ids:
                return {"error": "nenhum lead elegível para esta etapa", "total": 0}, 400

            round_robin = (campaign.get("rotation_mode") or "single") == "round_robin"
            inst_ids = assign_round_robin([int(i["id"]) for i in instances], len(lead_ids), round_robin)
            token_by_inst = {int(i["id"]): i["apikey"] for i in instances}

            gate = compute_dispatch_gate(
                campaign,
                sent_today=_sent_today_count(cur, campaign_id),
                campaign_paused=(campaign.get("status") == "paused"),
            )
            start_now = gate["wait_seconds"] <= 0
            now = _utc_now_naive()
            cur.execute(
                """
                INSERT INTO campaign_dispatches (campaign_id, step, stage, status, total, started_at)
                VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
                """,
                (campaign_id, step, stage, "running" if start_now else "scheduled", len(lead_ids), now if start_now else None),
            )
            dispatch_id = int(cur.fetchone()["id"])
            rows = [
                (dispatch_id, campaign_id, lid, iid, step, stage, pick_variation(messages))
                for lid, iid in zip(lead_ids, inst_ids)
            ]
            cur.executemany(
                """
                INSERT INTO campaign_sends (dispatch_id, campaign_id, campaign_lead_id, instance_id, step, stage, message)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                rows,
            )
            if campaign.get("status") in ("pending", None):
                cur.execute("UPDATE campaigns SET status = 'running' WHERE id = %s", (campaign_id,))
        conn.commit()
    finally:
        conn.close()

    lo, hi = campaign_delays_seconds(campaign)
    first_inst, first_msg = rows[0][3], rows[0][6]
    payload = {
        "campaign_id": str(dispatch_id),
        "instance_token": token_by_inst.get(first_inst, ""),
        "message": first_msg,
        "min_delay": lo,
        "max_delay": hi,
        "campaign_status": "running" if start_now else "scheduled",
        "schedule_enabled": True,
        "weekdays": _weekdays(campaign),
        "window_start": f"{int(campaign.get('send_hour_start') or 8):02d}:00",
        "window_end": f"{int(campaign.get('send_hour_end') or 20):02d}:00",
        "wait_seconds": gate["wait_seconds"],
        "wait_reason": gate["wait_reason"],
        "resume_at": gate["resume_at"],
        "daily_limit": gate["daily_limit"],
        "daily_remaining": gate["daily_remaining"],
        "quota_exhausted": gate["quota_exhausted"],
        "crm_base_url": (PUBLIC_BASE_URL or "").rstrip("/"),
        "source": "leads-infinitos",
    }
    ok, err = _trigger_n8n(payload)
    return {
        "dispatch_id": dispatch_id,
        "campaign_id": campaign_id,
        "step": step,
        "stage": stage,
        "stage_label": STAGE_LABELS[step],
        "total": len(rows),
        "status": payload["campaign_status"],
        "n8n_triggered": ok,
        "n8n_error": err,
    }, (200 if ok else 502)


def _weekdays(campaign: dict) -> list[int]:
    """0=domingo … 6=sábado (mesma convenção do CRM / Date#getDay)."""
    days = [1, 2, 3, 4, 5]
    if campaign.get("send_saturday"):
        days.append(6)
    if campaign.get("send_sunday"):
        days.insert(0, 0)
    return days


@n8n_dispatch_bp.route("/api/campaigns/<int:campaign_id>/n8n-dispatch/<int:step>", methods=["POST"])
@login_required
def api_start_n8n_dispatch(campaign_id: int, step: int):
    body, code = start_dispatch(campaign_id, step, user_id=int(current_user.id))
    return jsonify(body), code


# ───────────────────────────── n8n → LI ─────────────────────────────

@n8n_dispatch_bp.route("/api/n8n/tools/dispatch-targets", methods=["GET"])
def n8n_dispatch_targets():
    err = _require_api_key()
    if err:
        return err
    try:
        dispatch_id = int(request.args.get("campaign_id") or 0)
    except ValueError:
        dispatch_id = 0
    status_filter = (request.args.get("status") or "pending").strip()
    try:
        limit = max(1, min(int(request.args.get("limit") or 1), 500))
    except ValueError:
        limit = 1

    conn = _conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM campaign_dispatches WHERE id = %s", (dispatch_id,))
            dispatch = cur.fetchone()
            if not dispatch:
                return jsonify({"detail": "Campanha não encontrada."}), 404
            campaign = _load_campaign(cur, int(dispatch["campaign_id"])) or {}
            cancelled = dispatch["status"] == "cancelled"

            gate = compute_dispatch_gate(
                campaign,
                sent_today=_sent_today_count(cur, int(campaign["id"])),
                campaign_paused=(campaign.get("status") == "paused"),
            )
            wait_seconds = 0 if cancelled else gate["wait_seconds"]
            can_send = not cancelled and wait_seconds <= 0

            cur.execute(
                "SELECT 1 FROM campaign_sends WHERE dispatch_id = %s AND status = %s LIMIT 1",
                (dispatch_id, status_filter),
            )
            has_pending = bool(cur.fetchone())
            dispatch_status = dispatch["status"]
            if not cancelled and has_pending:
                now = _utc_now_naive()
                if can_send and dispatch_status == "scheduled":
                    cur.execute(
                        "UPDATE campaign_dispatches SET status = 'running', started_at = COALESCE(started_at, %s) WHERE id = %s",
                        (now, dispatch_id),
                    )
                    dispatch_status = "running"
                elif not can_send and dispatch_status == "running":
                    cur.execute("UPDATE campaign_dispatches SET status = 'scheduled' WHERE id = %s", (dispatch_id,))
                    dispatch_status = "scheduled"

            targets: list[dict] = []
            if can_send:
                take = min(limit, max(0, gate["daily_remaining"]))
                if take > 0:
                    cur.execute(
                        """
                        SELECT s.id, s.message, i.apikey,
                               COALESCE(NULLIF(TRIM(cl.name), ''), cl.phone) AS name, cl.phone
                        FROM campaign_sends s
                        JOIN campaign_leads cl ON cl.id = s.campaign_lead_id
                        LEFT JOIN instances i ON i.id = s.instance_id
                        WHERE s.dispatch_id = %s AND s.status = %s
                        ORDER BY COALESCE(cl.csv_row_order, cl.id), s.id
                        LIMIT %s
                        """,
                        (dispatch_id, status_filter, take),
                    )
                    for r in cur.fetchall() or []:
                        targets.append(
                            {
                                "target_id": str(r["id"]),
                                "name": r.get("name") or "",
                                "phone_e164": _phone_e164(r.get("phone")),
                                "message": r.get("message") or "",
                                "instance_token": r.get("apikey") or "",
                            }
                        )
        conn.commit()
    finally:
        conn.close()

    lo, hi = campaign_delays_seconds(campaign)
    first = targets[0] if targets else {}
    return jsonify(
        {
            "campaign_id": str(dispatch_id),
            # Top-level = primeiro target: o nó Expand Targets do n8n lê daqui (limit=1).
            "message": first.get("message", ""),
            "instance_token": first.get("instance_token", ""),
            "min_delay": lo,
            "max_delay": hi,
            "cancelled": cancelled,
            "campaign_status": dispatch_status,
            "schedule_enabled": True,
            "weekdays": _weekdays(campaign),
            "window_start": f"{int(campaign.get('send_hour_start') or 8):02d}:00",
            "window_end": f"{int(campaign.get('send_hour_end') or 20):02d}:00",
            "within_window": gate["within_window"],
            "wait_seconds": wait_seconds,
            "wait_reason": "cancelled" if cancelled else gate["wait_reason"],
            "resume_at": None if cancelled else gate["resume_at"],
            "daily_limit": gate["daily_limit"],
            "daily_remaining": gate["daily_remaining"],
            "quota_exhausted": gate["quota_exhausted"],
            "targets": targets,
        }
    )


def _phone_e164(phone: Any) -> str:
    digits = "".join(ch for ch in str(phone or "") if ch.isdigit())
    if digits and not digits.startswith("55") and len(digits) <= 11:
        digits = "55" + digits
    return digits


@n8n_dispatch_bp.route("/api/n8n/tools/dispatch-execution", methods=["POST"])
def n8n_dispatch_execution():
    err = _require_api_key()
    if err:
        return err
    body = request.get_json(silent=True) or {}
    try:
        dispatch_id = int(body.get("campaign_id") or 0)
    except ValueError:
        dispatch_id = 0
    execution_id = str(body.get("execution_id") or "").strip()
    if not execution_id:
        return jsonify({"detail": "execution_id obrigatório."}), 400
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE campaign_dispatches SET n8n_execution_id = %s WHERE id = %s",
                (execution_id, dispatch_id),
            )
            if cur.rowcount == 0:
                return jsonify({"detail": "Campanha não encontrada."}), 404
        conn.commit()
    finally:
        conn.close()
    return jsonify({"ok": True, "campaign_id": str(dispatch_id), "execution_id": execution_id})


@n8n_dispatch_bp.route("/api/n8n/tools/dispatch-target-status", methods=["POST"])
def n8n_dispatch_target_status():
    err = _require_api_key()
    if err:
        return err
    body = request.get_json(silent=True) or {}
    new_status = str(body.get("status") or "").strip()
    if new_status not in ("sent", "failed", "pending"):
        return jsonify({"detail": "status inválido."}), 400
    try:
        target_id = int(body.get("target_id") or 0)
    except ValueError:
        target_id = 0
    error_text = body.get("error")
    error_text = str(error_text)[:1800] if error_text else None

    conn = _conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id, dispatch_id, campaign_id, campaign_lead_id, instance_id, step, stage FROM campaign_sends WHERE id = %s",
                (target_id,),
            )
            send = cur.fetchone()
            if not send:
                return jsonify({"detail": "Target não encontrado."}), 404
            now = _utc_now_naive()
            cur.execute(
                "UPDATE campaign_sends SET status = %s, error = %s, sent_at = %s WHERE id = %s",
                (new_status, error_text, now if new_status == "sent" else None, target_id),
            )
            if new_status == "sent":
                cur.execute(
                    """
                    UPDATE campaign_leads
                    SET status = 'sent',
                        sent_at = COALESCE(sent_at, %s),
                        current_step = GREATEST(COALESCE(current_step, 1), %s),
                        last_sent_stage = %s,
                        last_sent_instance_id = %s,
                        last_message_sent_at = %s,
                        log = CONCAT(COALESCE(log, ''), %s)
                    WHERE id = %s
                    """,
                    (
                        now, int(send["step"]), send["stage"], send["instance_id"], now,
                        f"\n[n8n] {send['stage']} enviado {now.isoformat()}Z",
                        int(send["campaign_lead_id"]),
                    ),
                )
            elif new_status == "failed" and int(send["step"]) == 1:
                cur.execute(
                    "UPDATE campaign_leads SET status = 'failed', log = CONCAT(COALESCE(log, ''), %s) WHERE id = %s",
                    (f"\n[n8n] falha: {error_text or 'send failed'}", int(send["campaign_lead_id"])),
                )
            counters = _refresh_counters(cur, int(send["dispatch_id"]))
        conn.commit()
    finally:
        conn.close()
    return jsonify({"ok": True, "campaign_id": str(send["dispatch_id"]), **counters})


@n8n_dispatch_bp.route("/api/n8n/tools/dispatch-complete", methods=["POST"])
def n8n_dispatch_complete():
    err = _require_api_key()
    if err:
        return err
    body = request.get_json(silent=True) or {}
    try:
        dispatch_id = int(body.get("campaign_id") or 0)
    except ValueError:
        dispatch_id = 0
    conn = _conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, status FROM campaign_dispatches WHERE id = %s", (dispatch_id,))
            dispatch = cur.fetchone()
            if not dispatch:
                return jsonify({"detail": "Campanha não encontrada."}), 404
            counters = _refresh_counters(cur, dispatch_id)
            if dispatch["status"] in ("cancelled", "scheduled") or counters["pending"] > 0:
                conn.commit()
                return jsonify({"ok": True, "skipped": True, "status": dispatch["status"], **counters})
            done_status = "done" if counters["sent"] > 0 or counters["failed"] == 0 else "failed"
            cur.execute(
                "UPDATE campaign_dispatches SET status = %s, finished_at = %s WHERE id = %s",
                (done_status, _utc_now_naive(), dispatch_id),
            )
        conn.commit()
    finally:
        conn.close()
    return jsonify({"ok": True, "skipped": False, "status": done_status, **counters})
