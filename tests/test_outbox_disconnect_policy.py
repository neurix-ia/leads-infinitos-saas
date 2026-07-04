# -*- coding: utf-8 -*-
"""
Task 8 (tech-spec desconexão): política outbox — classificação de falhas e exclusão do tick
quando a campanha está pausada (incl. pausa sistema por instância).
"""

from __future__ import annotations

import importlib
import json
import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

_REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_TESTS = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _REPO)
sys.path.insert(0, _TESTS)

import campaign_test_data as ctd

from utils.uazapi_outbox_errors import classify_outbox_send_failure


@pytest.fixture
def db_conn():
    from app import get_db_connection

    try:
        conn = get_db_connection()
    except Exception as exc:
        pytest.skip(f"PostgreSQL não disponível para testes de integração: {exc}")
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture
def ensure_target_user(db_conn):
    from psycopg2.extras import RealDictCursor

    email = "outbox_disconnect_policy@example.com"
    with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id FROM users WHERE email = %s", (email,))
        row = cur.fetchone()
        if row:
            return row["id"]
        cur.execute(
            "INSERT INTO users (email, password_hash, is_admin) VALUES (%s, %s, false) RETURNING id",
            (email, "fakehash"),
        )
        uid = cur.fetchone()["id"]
        db_conn.commit()
        return uid


@pytest.fixture
def ensure_uazapi_instance(db_conn, ensure_target_user):
    return ctd.first_connected_uazapi_instance_id(db_conn, ensure_target_user)


def _reload_outbox_modules(monkeypatch, enabled: bool):
    if enabled:
        monkeypatch.setenv("USE_MESSAGE_OUTBOX", "1")
    else:
        monkeypatch.delenv("USE_MESSAGE_OUTBOX", raising=False)
    import utils.config as cfg

    importlib.reload(cfg)
    import app as app_mod

    importlib.reload(app_mod)
    import worker_message_outbox as wmo

    importlib.reload(wmo)
    return app_mod, wmo


def _insert_campaign_with_leads(
    db_conn,
    *,
    user_id: int,
    instance_id: int,
    n_leads: int,
    name: str,
    sent_today: int = 0,
):
    from psycopg2.extras import RealDictCursor

    msg = json.dumps(["Olá {nome}, teste desconexão outbox."])
    with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO campaigns (
                user_id, name, message_template, status,
                use_uazapi_sender, rotation_mode,
                send_hour_start, send_hour_end, send_saturday, send_sunday,
                outbox_delay_min_seconds, outbox_delay_max_seconds,
                sent_today, enable_cadence
            )
            VALUES (
                %s, %s, %s, 'running',
                true, 'single',
                %s, %s, true, true,
                5, 10,
                %s, false
            )
            RETURNING id
            """,
            (
                user_id,
                name,
                msg,
                ctd.DEFAULT_TEST_SEND_HOUR_START,
                ctd.DEFAULT_TEST_SEND_HOUR_END,
                sent_today,
            ),
        )
        cid = cur.fetchone()["id"]
        cur.execute(
            "INSERT INTO campaign_instances (campaign_id, instance_id) VALUES (%s, %s)",
            (cid, instance_id),
        )
        cur.execute(
            """
            INSERT INTO campaign_steps (campaign_id, step_number, step_label, message_template)
            VALUES (%s, 1, 'Inicial', %s::text)
            """,
            (cid, msg),
        )
        lead_ids = []
        for i in range(n_leads):
            cur.execute(
                """
                INSERT INTO campaign_leads (campaign_id, phone, name, status)
                VALUES (%s, %s, %s, 'pending')
                RETURNING id
                """,
                (cid, f"551188888{i:04d}", f"Lead {i}"),
            )
            lead_ids.append(int(cur.fetchone()["id"]))
        db_conn.commit()
    return cid, lead_ids


@pytest.mark.parametrize(
    "http_status,body,result_json,exc_cls,expected",
    [
        (503, '{"error":"x"}', None, None, "retry_backoff"),
        (502, None, None, None, "retry_backoff"),
        (504, None, None, None, "retry_backoff"),
        (408, None, None, None, "retry_backoff"),
        (429, None, None, None, "retry_backoff"),
        (500, "{}", None, None, "retry_backoff"),
        (400, '{"message":"bad"}', None, None, "terminal"),
        (404, None, None, None, "terminal"),
        (None, "", None, None, "instance_unreachable"),
        (None, '{"error":"no session"}', None, None, "instance_unreachable"),
        (200, '{"error":"disconnected"}', None, None, "instance_unreachable"),
        (None, "missing_phone", None, None, "terminal"),
        (None, "missing_message_template", None, None, "terminal"),
        (
            None,
            None,
            {
                "uazapi_request_failed": True,
                "http_status": 503,
                "error_body": {"message": "down"},
            },
            None,
            "retry_backoff",
        ),
        (
            None,
            None,
            {
                "uazapi_request_failed": True,
                "http_status": 500,
                "error_body": {"error": "No session"},
            },
            None,
            "instance_unreachable",
        ),
    ],
)
def test_classify_outbox_send_failure_matrix(http_status, body, result_json, exc_cls, expected):
    assert classify_outbox_send_failure(http_status, body, result_json, exc_cls) == expected


def test_classify_timeout_exception_retry():
    assert classify_outbox_send_failure(None, "", None, TimeoutError) == "retry_backoff"


def test_classify_connection_error_unreachable():
    assert classify_outbox_send_failure(None, "", None, ConnectionError) == "instance_unreachable"


def test_classify_requests_timeout_subclass():
    import requests

    assert classify_outbox_send_failure(None, "", None, requests.exceptions.ReadTimeout) == "retry_backoff"


def test_classify_requests_connection_error_subclass():
    import requests

    assert (
        classify_outbox_send_failure(None, "", None, requests.exceptions.ConnectionError)
        == "instance_unreachable"
    )


def _chosen_minimal(*, user_id: int, instance_id: int):
    return {
        "stage": "initial",
        "enable_cadence": False,
        "instance_name": "inst-test",
        "outbox_delay_min_seconds": 5,
        "outbox_delay_max_seconds": 10,
        "user_id": user_id,
        "instance_id": instance_id,
        "csv_row_order": 1,
    }


def test_persist_outcome_transient_503_keeps_pending_not_failed(
    db_conn, ensure_target_user, ensure_uazapi_instance, monkeypatch
):
    """Falha HTTP transitória (503): outbox permanece elegível a retry, não ``failed`` terminal."""
    _reload_outbox_modules(monkeypatch, True)
    uid = ensure_target_user
    iid = ensure_uazapi_instance
    cid, leads = _insert_campaign_with_leads(
        db_conn, user_id=uid, instance_id=iid, n_leads=1, name="disconnect 503 persist"
    )
    from psycopg2.extras import RealDictCursor

    with db_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO campaign_message_outbox (
                campaign_id, campaign_lead_id, instance_id,
                stage, step_priority, status, queued_at, next_run_at,
                idempotency_key, payload_summary
            )
            VALUES (%s, %s, %s, 'initial', 0, 'sending', NOW(), NOW(),
                    %s, '{}'::jsonb)
            RETURNING id
            """,
            (cid, leads[0], iid, f"campaign-{cid}-lead-{leads[0]}-initial"),
        )
        oid = int(cur.fetchone()[0])
        db_conn.commit()

    import worker_message_outbox as wmo

    wmo._persist_outcome(
        db_conn,
        outbox_id=oid,
        campaign_id=cid,
        lead_id=leads[0],
        chosen=_chosen_minimal(user_id=uid, instance_id=iid),
        http_status=503,
        response_body=json.dumps({"message": "Service Unavailable"}),
        outcome="failed",
        latency_ms=12,
        success=False,
        track_from_response=None,
        audit_request={"kind": "text"},
        audit_response_body=None,
    )

    with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT status FROM campaign_message_outbox WHERE id = %s", (oid,))
        assert cur.fetchone()["status"] == "pending"
        cur.execute(
            "SELECT outcome FROM campaign_send_attempts WHERE outbox_id = %s ORDER BY attempt_no DESC LIMIT 1",
            (oid,),
        )
        assert cur.fetchone()["outcome"] == "retry_scheduled"

    monkeypatch.delenv("USE_MESSAGE_OUTBOX", raising=False)
    importlib.reload(__import__("utils.config", fromlist=["cfg"]))
    importlib.reload(__import__("worker_message_outbox", fromlist=["wmo"]))


def test_persist_outcome_terminal_400_marks_failed(db_conn, ensure_target_user, ensure_uazapi_instance, monkeypatch):
    _reload_outbox_modules(monkeypatch, True)
    uid = ensure_target_user
    iid = ensure_uazapi_instance
    cid, leads = _insert_campaign_with_leads(
        db_conn, user_id=uid, instance_id=iid, n_leads=1, name="disconnect 400 persist"
    )
    from psycopg2.extras import RealDictCursor

    with db_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO campaign_message_outbox (
                campaign_id, campaign_lead_id, instance_id,
                stage, step_priority, status, queued_at, next_run_at,
                idempotency_key, payload_summary
            )
            VALUES (%s, %s, %s, 'initial', 0, 'sending', NOW(), NOW(),
                    %s, '{}'::jsonb)
            RETURNING id
            """,
            (cid, leads[0], iid, f"campaign-{cid}-lead-{leads[0]}-initial-b"),
        )
        oid = int(cur.fetchone()[0])
        db_conn.commit()

    import worker_message_outbox as wmo

    wmo._persist_outcome(
        db_conn,
        outbox_id=oid,
        campaign_id=cid,
        lead_id=leads[0],
        chosen=_chosen_minimal(user_id=uid, instance_id=iid),
        http_status=400,
        response_body=json.dumps({"error": "bad request"}),
        outcome="failed",
        latency_ms=5,
        success=False,
        track_from_response=None,
        audit_request={"kind": "text"},
        audit_response_body=None,
    )

    with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT status FROM campaign_message_outbox WHERE id = %s", (oid,))
        assert cur.fetchone()["status"] == "failed"
        cur.execute(
            "SELECT outcome FROM campaign_send_attempts WHERE outbox_id = %s ORDER BY attempt_no DESC LIMIT 1",
            (oid,),
        )
        assert cur.fetchone()["outcome"] == "failed_terminal"

    monkeypatch.delenv("USE_MESSAGE_OUTBOX", raising=False)
    importlib.reload(__import__("utils.config", fromlist=["cfg"]))
    importlib.reload(__import__("worker_message_outbox", fromlist=["wmo"]))


def test_system_paused_campaign_skipped_by_outbox_tick_select(
    db_conn, ensure_target_user, ensure_uazapi_instance, monkeypatch
):
    """Campanha ``paused`` com ``pause_origin=system`` (desconexão) não entra no SELECT do tick."""
    _reload_outbox_modules(monkeypatch, True)
    uid = ensure_target_user
    iid = ensure_uazapi_instance
    cid, leads = _insert_campaign_with_leads(
        db_conn, user_id=uid, instance_id=iid, n_leads=1, name="system pause outbox tick"
    )
    from psycopg2.extras import RealDictCursor

    with db_conn.cursor() as cur:
        cur.execute(
            """
            UPDATE campaigns
            SET status = 'paused',
                pause_origin = 'system',
                pause_reason_code = 'instance_disconnected',
                system_paused_at = NOW()
            WHERE id = %s
            """,
            (cid,),
        )
        cur.execute(
            """
            INSERT INTO campaign_message_outbox (
                campaign_id, campaign_lead_id, instance_id,
                stage, step_priority, status, queued_at, next_run_at,
                idempotency_key, payload_summary
            )
            VALUES (%s, %s, %s, 'initial', 0, 'pending', NOW(), NOW(),
                    %s, '{}'::jsonb)
            """,
            (cid, leads[0], iid, f"campaign-{cid}-lead-{leads[0]}-initial-sys"),
        )
        db_conn.commit()

    with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT id FROM campaign_message_outbox WHERE campaign_id = %s ORDER BY id DESC LIMIT 1",
            (cid,),
        )
        oid = int(cur.fetchone()["id"])

    import worker_message_outbox as wmo

    with patch.object(wmo, "is_campaign_send_window", return_value=True):
        with patch.object(
            wmo.uazapi_service,
            "send_text_idempotent",
            return_value={"messageId": "should-not-run"},
        ):
            with patch("utils.limits.check_initial_chunk_daily_quota_for_campaign", return_value=True):
                wmo.process_message_outbox_tick(db_conn)

    with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT status FROM campaign_message_outbox WHERE id = %s", (oid,))
        assert cur.fetchone()["status"] == "pending"
        cur.execute("SELECT COUNT(*) AS n FROM campaign_send_attempts WHERE outbox_id = %s", (oid,))
        assert int(cur.fetchone()["n"]) == 0

    monkeypatch.delenv("USE_MESSAGE_OUTBOX", raising=False)
    importlib.reload(__import__("utils.config", fromlist=["cfg"]))
    importlib.reload(__import__("worker_message_outbox", fromlist=["wmo"]))


def test_outbox_tick_sql_excludes_non_running_campaign_statuses():
    """Garante que o SELECT principal do tick continua a filtrar só campanhas activas."""
    src = Path(_REPO, "worker_message_outbox.py").read_text(encoding="utf-8")
    assert "AND c.status IN ('running', 'pending')" in src
    assert "waiting_instance" in src


def test_normalize_uazapi_send_result_structured_failure_not_success():
    import worker_message_outbox as wmo

    failure = {
        "uazapi_request_failed": True,
        "http_status": 400,
        "error_body": {"message": "bad request"},
    }
    norm = wmo._normalize_uazapi_send_result(failure)
    assert norm["success"] is False
    assert norm["http_status"] == 400
    assert norm["result_json_classify"]["uazapi_request_failed"] is True


def test_normalize_uazapi_send_result_none_legacy_not_success():
    import worker_message_outbox as wmo

    norm = wmo._normalize_uazapi_send_result(None)
    assert norm["success"] is False
    assert norm["http_status"] is None
    assert norm["result_json_classify"]["reason"] == "empty_response"


def test_persist_outcome_structured_400_marks_failed_terminal(
    db_conn, ensure_target_user, ensure_uazapi_instance, monkeypatch
):
    _reload_outbox_modules(monkeypatch, True)
    uid = ensure_target_user
    iid = ensure_uazapi_instance
    cid, leads = _insert_campaign_with_leads(
        db_conn, user_id=uid, instance_id=iid, n_leads=1, name="structured 400"
    )
    from psycopg2.extras import RealDictCursor

    with db_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO campaign_message_outbox (
                campaign_id, campaign_lead_id, instance_id,
                stage, step_priority, status, queued_at, next_run_at,
                idempotency_key, payload_summary
            )
            VALUES (%s, %s, %s, 'initial', 0, 'sending', NOW(), NOW(),
                    %s, '{}'::jsonb)
            RETURNING id
            """,
            (cid, leads[0], iid, f"campaign-{cid}-lead-{leads[0]}-struct400"),
        )
        oid = int(cur.fetchone()[0])
        db_conn.commit()

    import worker_message_outbox as wmo

    structured = {
        "uazapi_request_failed": True,
        "http_status": 400,
        "error_body": {"error": "invalid number"},
    }
    norm = wmo._normalize_uazapi_send_result(structured)
    wmo._persist_outcome(
        db_conn,
        outbox_id=oid,
        campaign_id=cid,
        lead_id=leads[0],
        chosen=_chosen_minimal(user_id=uid, instance_id=iid),
        http_status=norm["http_status"],
        response_body=norm["response_body"],
        outcome="failed",
        latency_ms=3,
        success=False,
        track_from_response=None,
        audit_request={"kind": "text"},
        audit_response_body=norm["audit_response_body"],
    )

    with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT status FROM campaign_message_outbox WHERE id = %s", (oid,))
        assert cur.fetchone()["status"] == "failed"
        cur.execute(
            "SELECT outcome FROM campaign_send_attempts WHERE outbox_id = %s ORDER BY attempt_no DESC LIMIT 1",
            (oid,),
        )
        assert cur.fetchone()["outcome"] == "failed_terminal"


def test_persist_outcome_max_attempts_marks_failed_max_attempts(
    db_conn, ensure_target_user, ensure_uazapi_instance, monkeypatch
):
    monkeypatch.setenv("UAZAPI_OUTBOX_MAX_ATTEMPTS", "3")
    _reload_outbox_modules(monkeypatch, True)
    uid = ensure_target_user
    iid = ensure_uazapi_instance
    cid, leads = _insert_campaign_with_leads(
        db_conn, user_id=uid, instance_id=iid, n_leads=1, name="max attempts"
    )
    from psycopg2.extras import RealDictCursor

    with db_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO campaign_message_outbox (
                campaign_id, campaign_lead_id, instance_id,
                stage, step_priority, status, queued_at, next_run_at,
                idempotency_key, payload_summary
            )
            VALUES (%s, %s, %s, 'initial', 0, 'sending', NOW(), NOW(),
                    %s, '{}'::jsonb)
            RETURNING id
            """,
            (cid, leads[0], iid, f"campaign-{cid}-lead-{leads[0]}-maxatt"),
        )
        oid = int(cur.fetchone()[0])
        for n in (1, 2):
            cur.execute(
                """
                INSERT INTO campaign_send_attempts
                    (outbox_id, attempt_no, http_status, uazapi_response, outcome, latency_ms, finished_at)
                VALUES (%s, %s, 503, '{}', 'retry_scheduled', 1, NOW())
                """,
                (oid, n),
            )
        db_conn.commit()

    import worker_message_outbox as wmo

    wmo._persist_outcome(
        db_conn,
        outbox_id=oid,
        campaign_id=cid,
        lead_id=leads[0],
        chosen=_chosen_minimal(user_id=uid, instance_id=iid),
        http_status=503,
        response_body=json.dumps({"message": "Service Unavailable"}),
        outcome="failed",
        latency_ms=5,
        success=False,
        track_from_response=None,
        audit_request={"kind": "text"},
        audit_response_body={"uazapi_request_failed": True, "http_status": 503},
    )

    with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT status FROM campaign_message_outbox WHERE id = %s", (oid,))
        assert cur.fetchone()["status"] == "failed"
        cur.execute(
            "SELECT outcome FROM campaign_send_attempts WHERE outbox_id = %s ORDER BY attempt_no DESC LIMIT 1",
            (oid,),
        )
        assert cur.fetchone()["outcome"] == "failed_max_attempts"


def test_persist_outcome_instance_unreachable_sets_waiting_instance(
    db_conn, ensure_target_user, ensure_uazapi_instance, monkeypatch
):
    _reload_outbox_modules(monkeypatch, True)
    uid = ensure_target_user
    iid = ensure_uazapi_instance
    cid, leads = _insert_campaign_with_leads(
        db_conn, user_id=uid, instance_id=iid, n_leads=1, name="instance unreachable"
    )
    from psycopg2.extras import RealDictCursor

    with db_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO campaign_message_outbox (
                campaign_id, campaign_lead_id, instance_id,
                stage, step_priority, status, queued_at, next_run_at,
                idempotency_key, payload_summary
            )
            VALUES (%s, %s, %s, 'initial', 0, 'sending', NOW(), NOW(),
                    %s, '{}'::jsonb)
            RETURNING id
            """,
            (cid, leads[0], iid, f"campaign-{cid}-lead-{leads[0]}-unreach"),
        )
        oid = int(cur.fetchone()[0])
        db_conn.commit()

    import worker_message_outbox as wmo

    norm = wmo._normalize_uazapi_send_result(None)
    wmo._persist_outcome(
        db_conn,
        outbox_id=oid,
        campaign_id=cid,
        lead_id=leads[0],
        chosen=_chosen_minimal(user_id=uid, instance_id=iid),
        http_status=norm["http_status"],
        response_body=norm["response_body"],
        outcome="failed",
        latency_ms=4,
        success=False,
        track_from_response=None,
        audit_request={"kind": "text"},
        audit_response_body=norm["audit_response_body"],
    )

    with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT status FROM campaign_message_outbox WHERE id = %s", (oid,))
        assert cur.fetchone()["status"] == "waiting_instance"
        cur.execute(
            "SELECT outcome FROM campaign_send_attempts WHERE outbox_id = %s ORDER BY attempt_no DESC LIMIT 1",
            (oid,),
        )
        assert cur.fetchone()["outcome"] == "retry_scheduled"


def test_persist_outcome_audit_contains_failure_kind(
    db_conn, ensure_target_user, ensure_uazapi_instance, monkeypatch, tmp_path
):
    monkeypatch.setenv("STORAGE_DIR", str(tmp_path))
    _reload_outbox_modules(monkeypatch, True)
    uid = ensure_target_user
    iid = ensure_uazapi_instance
    cid, leads = _insert_campaign_with_leads(
        db_conn, user_id=uid, instance_id=iid, n_leads=1, name="audit failure_kind"
    )
    from psycopg2.extras import RealDictCursor

    with db_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO campaign_message_outbox (
                campaign_id, campaign_lead_id, instance_id,
                stage, step_priority, status, queued_at, next_run_at,
                idempotency_key, payload_summary
            )
            VALUES (%s, %s, %s, 'initial', 0, 'sending', NOW(), NOW(),
                    %s, '{}'::jsonb)
            RETURNING id
            """,
            (cid, leads[0], iid, f"campaign-{cid}-lead-{leads[0]}-audit"),
        )
        oid = int(cur.fetchone()[0])
        db_conn.commit()

    import worker_message_outbox as wmo

    structured = {
        "uazapi_request_failed": True,
        "http_status": 503,
        "error_body": {"message": "down"},
    }
    norm = wmo._normalize_uazapi_send_result(structured)
    wmo._persist_outcome(
        db_conn,
        outbox_id=oid,
        campaign_id=cid,
        lead_id=leads[0],
        chosen=_chosen_minimal(user_id=uid, instance_id=iid),
        http_status=norm["http_status"],
        response_body=norm["response_body"],
        outcome="failed",
        latency_ms=6,
        success=False,
        track_from_response=None,
        audit_request={"kind": "text"},
        audit_response_body=norm["audit_response_body"],
    )

    import utils.campaign_dispatch_audit as cda

    path = cda.dispatch_audit_jsonl_path(uid, cid, ensure_parent=False)
    lines = path.read_text(encoding="utf-8").strip().splitlines()
    assert lines
    ev = json.loads(lines[-1])
    assert ev["failure_kind"] == "retry_backoff"
    assert ev.get("error_summary")
    assert ev.get("max_attempts") == wmo._OUTBOX_MAX_ATTEMPTS


def test_outbox_tick_structured_uazapi_failure_not_marked_sent(
    db_conn, ensure_target_user, ensure_uazapi_instance, monkeypatch
):
    """Dict de erro estruturado (truthy) não deve ser interpretado como sucesso no tick."""
    _reload_outbox_modules(monkeypatch, True)
    uid = ensure_target_user
    iid = ensure_uazapi_instance
    cid, leads = _insert_campaign_with_leads(
        db_conn, user_id=uid, instance_id=iid, n_leads=1, name="tick structured fail"
    )
    from psycopg2.extras import RealDictCursor

    with db_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO campaign_message_outbox (
                campaign_id, campaign_lead_id, instance_id,
                stage, step_priority, status, queued_at, next_run_at,
                idempotency_key, payload_summary
            )
            VALUES (%s, %s, %s, 'initial', 0, 'pending', NOW(), NOW(),
                    %s, '{}'::jsonb)
            RETURNING id
            """,
            (cid, leads[0], iid, f"campaign-{cid}-lead-{leads[0]}-tickfail"),
        )
        oid = int(cur.fetchone()[0])
        db_conn.commit()

    import worker_message_outbox as wmo

    structured_fail = {
        "uazapi_request_failed": True,
        "http_status": 503,
        "error_body": {"message": "Service Unavailable"},
    }
    with patch.object(wmo, "is_campaign_send_window", return_value=True):
        with patch.object(
            wmo.uazapi_service,
            "send_text_idempotent",
            return_value=structured_fail,
        ):
            with patch("utils.limits.check_initial_chunk_daily_quota_for_campaign", return_value=True):
                wmo.process_message_outbox_tick(db_conn)

    with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT status FROM campaign_message_outbox WHERE id = %s", (oid,))
        st = cur.fetchone()["status"]
        assert st in ("pending", "waiting_instance")
        cur.execute(
            "SELECT outcome, http_status FROM campaign_send_attempts WHERE outbox_id = %s ORDER BY attempt_no DESC LIMIT 1",
            (oid,),
        )
        row = cur.fetchone()
        assert row["outcome"] == "retry_scheduled"
        assert row["http_status"] == 503

    monkeypatch.delenv("USE_MESSAGE_OUTBOX", raising=False)
    importlib.reload(__import__("utils.config", fromlist=["cfg"]))
    importlib.reload(__import__("worker_message_outbox", fromlist=["wmo"]))
