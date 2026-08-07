# -*- coding: utf-8 -*-
"""Auto force chunk diário (catch-up matinal BRT) + force por user."""

from __future__ import annotations

import importlib
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytz

BRT = pytz.timezone("America/Sao_Paulo")


def _reload_outbox(monkeypatch, enabled: bool = True):
    if enabled:
        monkeypatch.setenv("USE_MESSAGE_OUTBOX", "1")
    else:
        monkeypatch.delenv("USE_MESSAGE_OUTBOX", raising=False)
    monkeypatch.setenv("OUTBOX_AUTO_FORCE_CATCHUP_HOURS", "6")
    monkeypatch.setenv("OUTBOX_AUTO_FORCE_PRIORITY_USER_IDS", "10")
    import utils.config as cfg

    importlib.reload(cfg)
    import worker_message_outbox as wmo

    importlib.reload(wmo)
    return wmo


def _camp(**kwargs):
    base = {
        "id": 279,
        "name": "Test",
        "user_id": 10,
        "status": "running",
        "use_uazapi_sender": True,
        "daily_limit": 30,
        "rotation_mode": "single",
        "send_hour_start": 8,
        "send_hour_end": 20,
        "send_saturday": False,
        "send_sunday": False,
    }
    base.update(kwargs)
    return base


def _quota(remaining=30, allows=True):
    return {
        "allows_more": allows,
        "remaining_slots": remaining,
        "policy": "g3",
    }


def test_auto_force_runs_at_window_open_when_queue_empty(monkeypatch):
    wmo = _reload_outbox(monkeypatch, True)
    wmo._AUTO_FORCE_DAILY_DONE.clear()
    conn = MagicMock()
    now = BRT.localize(datetime(2026, 7, 22, 8, 5, 0))

    with patch.object(wmo, "recover_stale_initial_outbox_for_campaign", return_value={}):
        with patch.object(wmo, "_list_uazapi_campaigns_needing_initial", return_value=[_camp()]):
            with patch.object(wmo, "_count_initial_outbox_in_flight", return_value=0):
                with patch.object(
                    wmo, "_count_eligible_pending_leads_without_initial_outbox", return_value=100
                ):
                    with patch.object(
                        wmo,
                        "schedule_next_initial_outbox_batch",
                        return_value=(30, None),
                    ) as sched:
                        n = wmo.maybe_auto_force_daily_initial_batches(conn, now_brazil=now)
    assert n == 30
    sched.assert_called_once()
    assert sched.call_args.kwargs.get("force") is True
    assert (279, "2026-07-22") in wmo._AUTO_FORCE_DAILY_DONE


def test_auto_force_still_runs_mid_morning_catchup(monkeypatch):
    """Catch-up até start+6h — 10:00 ainda força se fila vazia."""
    wmo = _reload_outbox(monkeypatch, True)
    wmo._AUTO_FORCE_DAILY_DONE.clear()
    conn = MagicMock()
    now = BRT.localize(datetime(2026, 7, 22, 10, 0, 0))

    with patch.object(wmo, "recover_stale_initial_outbox_for_campaign", return_value={}):
        with patch.object(wmo, "_list_uazapi_campaigns_needing_initial", return_value=[_camp()]):
            with patch.object(wmo, "_count_initial_outbox_in_flight", return_value=0):
                with patch.object(
                    wmo, "_count_eligible_pending_leads_without_initial_outbox", return_value=50
                ):
                    with patch.object(
                        wmo, "schedule_next_initial_outbox_batch", return_value=(30, None)
                    ) as sched:
                        n = wmo.maybe_auto_force_daily_initial_batches(conn, now_brazil=now)
    assert n == 30
    sched.assert_called_once()


def test_auto_force_skips_outside_catchup_window(monkeypatch):
    wmo = _reload_outbox(monkeypatch, True)
    wmo._AUTO_FORCE_DAILY_DONE.clear()
    conn = MagicMock()
    now = BRT.localize(datetime(2026, 7, 22, 15, 0, 0))  # após 8+6

    with patch.object(
        wmo, "_list_uazapi_campaigns_needing_initial", return_value=[_camp()]
    ):
        with patch.object(wmo, "schedule_next_initial_outbox_batch") as sched:
            n = wmo.maybe_auto_force_daily_initial_batches(conn, now_brazil=now)
    assert n == 0
    sched.assert_not_called()


def test_auto_force_retries_when_enqueue_returns_zero(monkeypatch):
    """Falha transitória não marca done — próximo tick pode tentar de novo."""
    wmo = _reload_outbox(monkeypatch, True)
    wmo._AUTO_FORCE_DAILY_DONE.clear()
    conn = MagicMock()
    now = BRT.localize(datetime(2026, 7, 22, 8, 10, 0))

    with patch.object(wmo, "recover_stale_initial_outbox_for_campaign", return_value={}):
        with patch.object(wmo, "_list_uazapi_campaigns_needing_initial", return_value=[_camp()]):
            with patch.object(wmo, "_count_initial_outbox_in_flight", return_value=0):
                with patch.object(
                    wmo, "_count_eligible_pending_leads_without_initial_outbox", return_value=100
                ):
                    with patch.object(
                        wmo,
                        "schedule_next_initial_outbox_batch",
                        return_value=(0, "persist_failed"),
                    ):
                        n = wmo.maybe_auto_force_daily_initial_batches(conn, now_brazil=now)
    assert n == 0
    assert (279, "2026-07-22") not in wmo._AUTO_FORCE_DAILY_DONE


def test_auto_force_skips_when_already_has_in_flight_and_no_eligible(monkeypatch):
    """In-flight sem elegíveis extras → marca done (fila já cobre o dia)."""
    wmo = _reload_outbox(monkeypatch, True)
    wmo._AUTO_FORCE_DAILY_DONE.clear()
    conn = MagicMock()
    now = BRT.localize(datetime(2026, 7, 22, 8, 10, 0))

    with patch.object(wmo, "recover_stale_initial_outbox_for_campaign", return_value={}):
        with patch.object(wmo, "_list_uazapi_campaigns_needing_initial", return_value=[_camp()]):
            with patch.object(wmo, "_count_initial_outbox_in_flight", return_value=30):
                with patch.object(
                    wmo, "_count_eligible_pending_leads_without_initial_outbox", return_value=0
                ):
                    with patch.object(wmo, "schedule_next_initial_outbox_batch") as sched:
                        n = wmo.maybe_auto_force_daily_initial_batches(conn, now_brazil=now)
    assert n == 0
    sched.assert_not_called()
    assert (279, "2026-07-22") in wmo._AUTO_FORCE_DAILY_DONE


def test_auto_force_still_runs_with_in_flight_when_eligible_remain(monkeypatch):
    """In-flight parcial + leads sem outbox → ainda force (não marca done cedo)."""
    wmo = _reload_outbox(monkeypatch, True)
    wmo._AUTO_FORCE_DAILY_DONE.clear()
    conn = MagicMock()
    now = BRT.localize(datetime(2026, 7, 22, 8, 10, 0))

    with patch.object(wmo, "recover_stale_initial_outbox_for_campaign", return_value={}):
        with patch.object(wmo, "_list_uazapi_campaigns_needing_initial", return_value=[_camp()]):
            with patch.object(wmo, "_count_initial_outbox_in_flight", return_value=5):
                with patch.object(
                    wmo, "_count_eligible_pending_leads_without_initial_outbox", return_value=80
                ):
                    with patch.object(
                        wmo, "schedule_next_initial_outbox_batch", return_value=(25, None)
                    ) as sched:
                        n = wmo.maybe_auto_force_daily_initial_batches(conn, now_brazil=now)
    assert n == 25
    sched.assert_called_once()
    assert sched.call_args.kwargs.get("force") is True
    assert (279, "2026-07-22") in wmo._AUTO_FORCE_DAILY_DONE


def test_auto_force_skips_weekend_when_disabled(monkeypatch):
    wmo = _reload_outbox(monkeypatch, True)
    wmo._AUTO_FORCE_DAILY_DONE.clear()
    conn = MagicMock()
    now = BRT.localize(datetime(2026, 7, 25, 8, 5, 0))  # sábado

    with patch.object(
        wmo,
        "_list_uazapi_campaigns_needing_initial",
        return_value=[_camp(send_saturday=False)],
    ):
        with patch.object(wmo, "schedule_next_initial_outbox_batch") as sched:
            n = wmo.maybe_auto_force_daily_initial_batches(conn, now_brazil=now)
    assert n == 0
    sched.assert_not_called()


def test_force_batches_for_user_enqueues_all(monkeypatch):
    wmo = _reload_outbox(monkeypatch, True)
    conn = MagicMock()
    camps = [_camp(id=279), _camp(id=280, name="Outra")]

    with patch.object(wmo, "recover_stale_initial_outbox_for_campaign", return_value={}):
        with patch.object(wmo, "_list_uazapi_campaigns_needing_initial", return_value=camps):
            with patch.object(
                wmo, "_count_eligible_pending_leads_without_initial_outbox", return_value=10
            ):
                with patch.object(wmo, "_count_initial_outbox_in_flight", return_value=0):
                    with patch.object(wmo, "_count_initial_outbox_sending", return_value=0):
                        with patch.object(
                            wmo, "initial_chunk_quota_snapshot", return_value=_quota(30)
                        ):
                            with patch.object(
                                wmo, "schedule_next_initial_outbox_batch", return_value=(5, None)
                            ) as sched:
                                out = wmo.force_initial_outbox_batches_for_user(conn, 10)
    assert out["rows_enqueued"] == 10
    assert out["scope"] == "user:10"
    assert sched.call_count == 2
    assert all(c["rows_enqueued"] == 5 for c in out["campaigns"])


def test_force_batches_despite_in_flight_when_eligible(monkeypatch):
    """force=True com elegíveis não aborta só porque há in_flight."""
    wmo = _reload_outbox(monkeypatch, True)
    conn = MagicMock()

    with patch.object(wmo, "recover_stale_initial_outbox_for_campaign", return_value={}):
        with patch.object(wmo, "_list_uazapi_campaigns_needing_initial", return_value=[_camp()]):
            with patch.object(
                wmo, "_count_eligible_pending_leads_without_initial_outbox", return_value=20
            ):
                with patch.object(wmo, "_count_initial_outbox_in_flight", return_value=5):
                    with patch.object(wmo, "_count_initial_outbox_sending", return_value=0):
                        with patch.object(
                            wmo, "initial_chunk_quota_snapshot", return_value=_quota(25)
                        ):
                            with patch.object(
                                wmo,
                                "schedule_next_initial_outbox_batch",
                                return_value=(20, None),
                            ) as sched:
                                out = wmo.force_initial_outbox_batches_for_user(
                                    conn, 10, force=True
                                )
    assert out["rows_enqueued"] == 20
    sched.assert_called_once()


def test_force_batches_for_all_users(monkeypatch):
    wmo = _reload_outbox(monkeypatch, True)
    conn = MagicMock()
    camps = [
        _camp(id=279, user_id=10),
        _camp(id=300, user_id=22, name="Outro user"),
    ]

    with patch.object(wmo, "recover_stale_initial_outbox_for_campaign", return_value={}):
        with patch.object(
            wmo, "_list_uazapi_campaigns_needing_initial", return_value=camps
        ) as listing:
            with patch.object(
                wmo, "_count_eligible_pending_leads_without_initial_outbox", return_value=3
            ):
                with patch.object(wmo, "_count_initial_outbox_in_flight", return_value=0):
                    with patch.object(wmo, "_count_initial_outbox_sending", return_value=0):
                        with patch.object(
                            wmo, "initial_chunk_quota_snapshot", return_value=_quota(30)
                        ):
                            with patch.object(
                                wmo,
                                "schedule_next_initial_outbox_batch",
                                return_value=(3, None),
                            ):
                                out = wmo.force_initial_outbox_batches_for_all_users(conn)
    assert listing.call_args.kwargs.get("user_id") is None
    assert out["scope"] == "all_users"
    assert out["user_id"] is None
    assert out["rows_enqueued"] == 6
    assert len(out["campaigns"]) == 2
