# -*- coding: utf-8 -*-
"""Auto force chunk diário no início da janela BRT."""

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


def test_auto_force_runs_at_window_open_when_queue_empty(monkeypatch):
    wmo = _reload_outbox(monkeypatch, True)
    wmo._AUTO_FORCE_DAILY_DONE.clear()
    conn = MagicMock()
    now = BRT.localize(datetime(2026, 7, 22, 8, 5, 0))  # qua 08:05

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


def test_auto_force_skips_outside_morning_window(monkeypatch):
    wmo = _reload_outbox(monkeypatch, True)
    wmo._AUTO_FORCE_DAILY_DONE.clear()
    conn = MagicMock()
    now = BRT.localize(datetime(2026, 7, 22, 10, 0, 0))

    with patch.object(
        wmo, "_list_uazapi_campaigns_needing_initial", return_value=[_camp()]
    ):
        with patch.object(wmo, "schedule_next_initial_outbox_batch") as sched:
            n = wmo.maybe_auto_force_daily_initial_batches(conn, now_brazil=now)
    assert n == 0
    sched.assert_not_called()
    assert (279, "2026-07-22") not in wmo._AUTO_FORCE_DAILY_DONE


def test_auto_force_skips_when_already_has_in_flight(monkeypatch):
    wmo = _reload_outbox(monkeypatch, True)
    wmo._AUTO_FORCE_DAILY_DONE.clear()
    conn = MagicMock()
    now = BRT.localize(datetime(2026, 7, 22, 8, 10, 0))

    with patch.object(wmo, "_list_uazapi_campaigns_needing_initial", return_value=[_camp()]):
        with patch.object(wmo, "_count_initial_outbox_in_flight", return_value=30):
            with patch.object(wmo, "schedule_next_initial_outbox_batch") as sched:
                n = wmo.maybe_auto_force_daily_initial_batches(conn, now_brazil=now)
    assert n == 0
    sched.assert_not_called()
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


def test_auto_force_once_per_day(monkeypatch):
    wmo = _reload_outbox(monkeypatch, True)
    wmo._AUTO_FORCE_DAILY_DONE.clear()
    wmo._AUTO_FORCE_DAILY_DONE.add((279, "2026-07-22"))
    conn = MagicMock()
    now = BRT.localize(datetime(2026, 7, 22, 8, 15, 0))

    with patch.object(wmo, "_list_uazapi_campaigns_needing_initial", return_value=[_camp()]):
        with patch.object(wmo, "schedule_next_initial_outbox_batch") as sched:
            n = wmo.maybe_auto_force_daily_initial_batches(conn, now_brazil=now)
    assert n == 0
    sched.assert_not_called()
