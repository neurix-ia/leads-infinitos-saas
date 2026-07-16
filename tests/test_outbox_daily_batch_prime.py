# -*- coding: utf-8 -*-
"""Pré-enfileiramento do lote initial do dia seguinte (cota esgotada)."""

from __future__ import annotations

import importlib
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest


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


def test_prime_skipped_when_in_flight_pending(monkeypatch):
    wmo = _reload_outbox(monkeypatch, True)
    conn = MagicMock()
    campaign = {
        "id": 279,
        "user_id": 10,
        "daily_limit": 30,
        "rotation_mode": "single",
        "use_uazapi_sender": True,
        "status": "running",
    }
    camp_win = {
        "send_hour_start": 8,
        "send_hour_end": 20,
        "send_saturday": False,
        "send_sunday": False,
    }
    with patch.object(wmo, "_count_initial_outbox_in_flight", return_value=3):
        with patch.object(wmo, "_load_uazapi_instances_for_campaign") as load_inst:
            n = wmo._prime_next_window_initial_outbox_batch(conn, campaign, camp_win)
    assert n == 0
    load_inst.assert_not_called()


def test_prime_enqueues_for_next_window_when_queue_empty(monkeypatch):
    wmo = _reload_outbox(monkeypatch, True)
    conn = MagicMock()
    campaign = {
        "id": 279,
        "user_id": 10,
        "daily_limit": 30,
        "rotation_mode": "single",
        "use_uazapi_sender": True,
        "status": "running",
    }
    camp_win = {
        "send_hour_start": 8,
        "send_hour_end": 20,
        "send_saturday": False,
        "send_sunday": False,
    }
    future = datetime(2026, 7, 17, 11, 0, 0)
    leads = [{"id": 1}, {"id": 2}, {"id": 3}]
    with patch.object(wmo, "_count_initial_outbox_in_flight", return_value=0):
        with patch.object(
            wmo,
            "_load_uazapi_instances_for_campaign",
            return_value=[{"instance_id": 135}],
        ):
            with patch.object(wmo, "_campaign_daily_batch_limit", return_value=30):
                with patch.object(
                    wmo, "_select_eligible_leads_for_initial_outbox", return_value=leads
                ):
                    with patch.object(
                        wmo,
                        "next_valid_send_after_current_window_utc_naive",
                        return_value=future,
                    ):
                        with patch.object(
                            wmo, "_persist_initial_outbox_batch", return_value=3
                        ) as persist:
                            n = wmo._prime_next_window_initial_outbox_batch(
                                conn, campaign, camp_win
                            )
    assert n == 3
    persist.assert_called_once()
    kwargs = persist.call_args.kwargs
    assert kwargs["next_run_at_val"] == future
    assert kwargs["enqueue_reason"] == "prime_next_window"
    assert kwargs["leads"] == leads


def test_schedule_primes_when_quota_exhausted(monkeypatch):
    wmo = _reload_outbox(monkeypatch, True)
    conn = MagicMock()
    campaign = {
        "id": 279,
        "user_id": 10,
        "daily_limit": 30,
        "rotation_mode": "single",
        "use_uazapi_sender": True,
        "status": "running",
        "send_hour_start": 8,
        "send_hour_end": 20,
        "send_saturday": False,
        "send_sunday": False,
    }
    with patch.object(
        wmo,
        "initial_chunk_quota_snapshot",
        return_value={
            "allows_more": False,
            "remaining_slots": 0,
            "sent_campaign_today": 30,
        },
    ):
        with patch.object(wmo, "_defer_pending_initial_rows_to_next_window", return_value=0):
            with patch.object(
                wmo, "_prime_next_window_initial_outbox_batch", return_value=30
            ) as prime:
                n, reason = wmo.schedule_next_initial_outbox_batch(conn, campaign)
    assert n == 30
    assert reason == "primed_next_window"
    prime.assert_called_once()
    conn.commit.assert_called()


def test_schedule_same_day_when_quota_allows_and_in_window(monkeypatch):
    """Rede de segurança: cota livre + na janela → next_run_at = agora (mesmo dia)."""
    wmo = _reload_outbox(monkeypatch, True)
    conn = MagicMock()
    campaign = {
        "id": 279,
        "user_id": 10,
        "daily_limit": 30,
        "rotation_mode": "single",
        "use_uazapi_sender": True,
        "status": "running",
        "send_hour_start": 8,
        "send_hour_end": 20,
        "send_saturday": False,
        "send_sunday": False,
    }
    leads = [{"id": 101}, {"id": 102}]
    # cursor for pending lead count
    cur = MagicMock()
    cur.fetchone.return_value = (655,)
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur

    with patch.object(
        wmo,
        "initial_chunk_quota_snapshot",
        return_value={
            "allows_more": True,
            "remaining_slots": 30,
            "sent_campaign_today": 0,
        },
    ):
        with patch.object(wmo, "_maybe_reset_stale_sent_today"):
            with patch.object(wmo, "_count_initial_outbox_in_flight", return_value=0):
                with patch.object(
                    wmo, "_reconcile_initial_outbox_in_flight", return_value=(True, None)
                ):
                    with patch.object(
                        wmo,
                        "_load_uazapi_instances_for_campaign",
                        return_value=[{"instance_id": 135}],
                    ):
                        with patch.object(
                            wmo, "_campaign_daily_batch_limit", return_value=30
                        ):
                            with patch.object(
                                wmo,
                                "_select_eligible_leads_for_initial_outbox",
                                return_value=leads,
                            ):
                                with patch.object(
                                    wmo, "is_campaign_send_window", return_value=True
                                ):
                                    with patch.object(
                                        wmo,
                                        "_persist_initial_outbox_batch",
                                        return_value=2,
                                    ) as persist:
                                        n, reason = wmo.schedule_next_initial_outbox_batch(
                                            conn, campaign
                                        )
    assert n == 2
    assert reason is None
    assert persist.call_args.kwargs["enqueue_reason"] == "schedule_next_initial_outbox_batch"
    assert persist.call_args.kwargs["next_run_at_val"] is not None


def test_schedule_skips_when_full_batch_already_queued(monkeypatch):
    """Lote já pré-enfileirado (30 pending) não gera segundo lote no dia seguinte."""
    wmo = _reload_outbox(monkeypatch, True)
    conn = MagicMock()
    campaign = {
        "id": 279,
        "user_id": 10,
        "daily_limit": 30,
        "use_uazapi_sender": True,
        "status": "running",
        "send_hour_start": 8,
        "send_hour_end": 20,
        "send_saturday": False,
        "send_sunday": False,
    }
    with patch.object(
        wmo,
        "initial_chunk_quota_snapshot",
        return_value={
            "allows_more": True,
            "remaining_slots": 30,
            "sent_campaign_today": 0,
        },
    ):
        with patch.object(wmo, "_maybe_reset_stale_sent_today"):
            with patch.object(wmo, "_count_initial_outbox_in_flight", return_value=30):
                with patch.object(wmo, "_reconcile_initial_outbox_in_flight") as recon:
                    n, reason = wmo.schedule_next_initial_outbox_batch(conn, campaign)
    assert n == 0
    assert reason == "initial_outbox_already_queued"
    recon.assert_not_called()
