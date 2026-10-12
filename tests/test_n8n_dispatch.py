"""Disparo via n8n: regras puras (gate, cota, round-robin, auth) e contrato HTTP com BD falsa."""

import random
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from utils.n8n_dispatch_gate import (
    api_key_matches,
    assign_round_robin,
    campaign_daily_cap,
    campaign_delays_seconds,
    compute_dispatch_gate,
    pick_variation,
)

CAMP = {"send_hour_start": 8, "send_hour_end": 20, "send_saturday": False, "send_sunday": False, "daily_limit": 30}


def _utc(y, m, d, hh, mm=0):
    # BRT = UTC-3 (sem DST em 2026). 10:00 BRT = 13:00 UTC.
    return datetime(y, m, d, hh, mm, tzinfo=timezone.utc)


def test_gate_within_window_and_quota_free():
    g = compute_dispatch_gate(CAMP, sent_today=5, now_utc=_utc(2026, 10, 14, 13))  # qua 10:00 BRT
    assert g["wait_seconds"] == 0 and g["wait_reason"] is None
    assert g["daily_remaining"] == 25 and g["within_window"] is True


def test_gate_outside_window_waits_until_window_start():
    g = compute_dispatch_gate(CAMP, sent_today=0, now_utc=_utc(2026, 10, 14, 2))  # qua 23:00 BRT (dia 13)
    assert g["wait_reason"] == "window" and g["wait_seconds"] > 0
    assert g["resume_at"].startswith("2026-10-14T11:00")  # 08:00 BRT


def test_gate_weekend_blocked_skips_to_monday():
    g = compute_dispatch_gate(CAMP, sent_today=0, now_utc=_utc(2026, 10, 17, 13))  # sáb 10:00 BRT
    assert g["wait_reason"] == "window"
    assert g["resume_at"].startswith("2026-10-19T11:00")


def test_gate_quota_exhausted_waits_for_next_day_window():
    g = compute_dispatch_gate(CAMP, sent_today=30, now_utc=_utc(2026, 10, 14, 13))
    assert g["wait_reason"] == "quota" and g["quota_exhausted"] is True
    assert g["daily_remaining"] == 0
    assert g["resume_at"].startswith("2026-10-15T11:00")


def test_gate_paused_rechecks_soon():
    g = compute_dispatch_gate(CAMP, sent_today=0, campaign_paused=True, now_utc=_utc(2026, 10, 14, 13))
    assert g["wait_reason"] == "paused" and 0 < g["wait_seconds"] <= 600


def test_daily_cap_never_exceeds_30():
    assert campaign_daily_cap({"daily_limit": 60}) == 30
    assert campaign_daily_cap({"daily_limit": 10}) == 10
    assert campaign_daily_cap({"daily_limit": 0}) == 30
    assert campaign_daily_cap({}) == 30


def test_delays_default_and_override():
    assert campaign_delays_seconds({}) == (180, 540)
    assert campaign_delays_seconds({"outbox_delay_min_seconds": 600, "outbox_delay_max_seconds": 60}) == (60, 600)


def test_round_robin_assignment():
    assert assign_round_robin([7, 9], 5, True) == [7, 9, 7, 9, 7]
    assert assign_round_robin([7, 9], 3, False) == [7, 7, 7]
    assert assign_round_robin([], 3, True) == []


def test_pick_variation_keeps_nome_placeholder():
    rng = random.Random(1)
    msg = pick_variation(["Oi {{nome}}", "  ", "Olá {{nome}}"], rng)
    assert "{{nome}}" in msg
    assert pick_variation([]) == ""


def test_api_key_requires_secret():
    assert api_key_matches("abc", "abc")
    assert not api_key_matches("abc", "abd")
    assert not api_key_matches("", "")
    assert not api_key_matches("x", None)


# ───────────── contrato HTTP com BD falsa ─────────────

class _FakeCursor:
    """Devolve respostas em sequência; grava SQL executado."""

    def __init__(self, results):
        self._results = list(results)
        self.executed = []
        self.rowcount = 1

    def execute(self, sql, params=None):
        self.executed.append((" ".join(sql.split()), params))

    def executemany(self, sql, rows):
        self.executed.append((" ".join(sql.split()), list(rows)))

    def fetchone(self):
        return self._results.pop(0) if self._results else None

    def fetchall(self):
        return self._results.pop(0) if self._results else []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _app_with(monkeypatch, cursor, api_key="k"):
    import n8n_dispatch
    from flask import Flask

    monkeypatch.setattr(n8n_dispatch, "N8N_DISPATCH_API_KEY", api_key)
    conn = MagicMock()
    conn.cursor.return_value = cursor
    n8n_dispatch.configure(lambda: conn)
    app = Flask(__name__)
    app.register_blueprint(n8n_dispatch.n8n_dispatch_bp)
    return app.test_client(), conn


def test_targets_requires_api_key(monkeypatch):
    client, _ = _app_with(monkeypatch, _FakeCursor([]))
    assert client.get("/api/n8n/tools/dispatch-targets?campaign_id=1").status_code == 401
    assert client.get("/api/n8n/tools/dispatch-targets?campaign_id=1", headers={"X-CRM-API-Key": "nope"}).status_code == 401


def test_targets_404_for_unknown_dispatch(monkeypatch):
    client, _ = _app_with(monkeypatch, _FakeCursor([None]))
    r = client.get("/api/n8n/tools/dispatch-targets?campaign_id=999", headers={"X-CRM-API-Key": "k"})
    assert r.status_code == 404 and r.get_json()["detail"] == "Campanha não encontrada."


def test_targets_returns_first_target_top_level(monkeypatch):
    dispatch = {"id": 5, "campaign_id": 1, "step": 1, "stage": "initial", "status": "running"}
    campaign = dict(CAMP, id=1, user_id=1, status="running", rotation_mode="single")
    rows = [{"id": 11, "message": "Oi {{nome}}", "apikey": "tok", "name": "Ana", "phone": "41999990000"}]
    cur = _FakeCursor([dispatch, campaign, {"n": 3}, {"x": 1}, rows])
    client, conn = _app_with(monkeypatch, cur)
    r = client.get("/api/n8n/tools/dispatch-targets?campaign_id=5&limit=1", headers={"X-CRM-API-Key": "k"})
    body = r.get_json()
    assert r.status_code == 200
    if body["wait_seconds"] == 0:  # depende da hora em que o teste roda
        assert body["targets"][0] == {
            "target_id": "11", "name": "Ana", "phone_e164": "5541999990000",
            "message": "Oi {{nome}}", "instance_token": "tok",
        }
        assert body["message"] == "Oi {{nome}}" and body["instance_token"] == "tok"
    else:
        assert body["targets"] == [] and body["wait_reason"] in ("window", "quota")
    assert body["campaign_id"] == "5" and body["daily_limit"] == 30
    conn.commit.assert_called()


def test_target_status_sent_updates_lead(monkeypatch):
    send = {"id": 11, "dispatch_id": 5, "campaign_id": 1, "campaign_lead_id": 77, "instance_id": 3, "step": 2, "stage": "follow1"}
    cur = _FakeCursor([send, {"sent": 1, "failed": 0, "pending": 0, "total": 1}])
    client, _ = _app_with(monkeypatch, cur)
    r = client.post("/api/n8n/tools/dispatch-target-status", json={"target_id": "11", "status": "sent"}, headers={"X-CRM-API-Key": "k"})
    assert r.status_code == 200 and r.get_json()["sent"] == 1
    lead_updates = [s for s, _ in cur.executed if "UPDATE campaign_leads" in s]
    assert len(lead_updates) == 1 and "last_sent_stage" in lead_updates[0]


def test_complete_marks_done_only_without_pending(monkeypatch):
    cur = _FakeCursor([{"id": 5, "status": "running"}, {"sent": 2, "failed": 0, "pending": 1, "total": 3}])
    client, _ = _app_with(monkeypatch, cur)
    r = client.post("/api/n8n/tools/dispatch-complete", json={"campaign_id": "5"}, headers={"X-CRM-API-Key": "k"})
    assert r.get_json()["skipped"] is True

    cur = _FakeCursor([{"id": 5, "status": "running"}, {"sent": 3, "failed": 0, "pending": 0, "total": 3}])
    client, _ = _app_with(monkeypatch, cur)
    r = client.post("/api/n8n/tools/dispatch-complete", json={"campaign_id": "5"}, headers={"X-CRM-API-Key": "k"})
    body = r.get_json()
    assert body["skipped"] is False and body["status"] == "done"
