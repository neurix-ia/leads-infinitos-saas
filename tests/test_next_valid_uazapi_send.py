"""Testes de ``next_valid_send_utc_naive`` / janela BRT (T3 / TD-9)."""

import unittest
from datetime import datetime

import pytz

from utils.next_valid_uazapi_send import (
    BRAZIL_TZ,
    is_campaign_send_window,
    next_valid_send_after_current_window_utc_naive,
    next_valid_send_utc_naive,
)


def _camp(**kwargs):
    base = {
        "send_hour_start": 8,
        "send_hour_end": 20,
        "send_saturday": False,
        "send_sunday": False,
    }
    base.update(kwargs)
    return base


class TestIsCampaignSendWindow(unittest.TestCase):
    def test_weekday_inside_window(self):
        # Segunda 2026-06-08 10:00 BRT
        br = BRAZIL_TZ.localize(datetime(2026, 6, 8, 10, 0, 0))
        self.assertTrue(is_campaign_send_window(_camp(), now_brazil=br))

    def test_saturday_blocked(self):
        br = BRAZIL_TZ.localize(datetime(2026, 6, 13, 10, 0, 0))  # sábado
        self.assertFalse(is_campaign_send_window(_camp(send_saturday=False), now_brazil=br))
        self.assertTrue(is_campaign_send_window(_camp(send_saturday=True), now_brazil=br))


class TestNextValidSendUtcNaive(unittest.TestCase):
    def test_same_instant_when_already_valid(self):
        # Seg 8 Jun 2026 10:00 BRT = 13:00 UTC
        utc_naive = datetime(2026, 6, 8, 13, 0, 0)
        out = next_valid_send_utc_naive(_camp(), utc_naive)
        self.assertEqual(out, utc_naive)

    def test_same_day_roll_to_send_hour_start(self):
        # Sex 12 Jun 2026 07:00 BRT = 10:00 UTC — antes da janela
        utc_naive = datetime(2026, 6, 12, 10, 0, 0)
        out = next_valid_send_utc_naive(_camp(), utc_naive)
        self.assertEqual(out, datetime(2026, 6, 12, 11, 0, 0))  # 08:00 BRT

    def test_skips_blocked_weekend(self):
        # Sáb 13 Jun 2026 12:00 BRT = 15:00 UTC
        utc_naive = datetime(2026, 6, 13, 15, 0, 0)
        out = next_valid_send_utc_naive(_camp(), utc_naive)
        # Seg 15 Jun 2026 08:00 BRT = 11:00 UTC
        self.assertEqual(out, datetime(2026, 6, 15, 11, 0, 0))

    def test_margin_minutes_applied_in_utc(self):
        # Seg 8 Jun 2026 08:00 BRT = 11:00 UTC; margem 120 min -> 10:00 BRT ainda na janela
        utc_naive = datetime(2026, 6, 8, 11, 0, 0)
        out = next_valid_send_utc_naive(_camp(), utc_naive, margin_minutes=120)
        self.assertEqual(out, datetime(2026, 6, 8, 13, 0, 0))

    def test_aware_datetime_normalized_to_utc(self):
        utc_naive = datetime(2026, 6, 8, 13, 0, 0)
        aware = pytz.UTC.localize(utc_naive)
        out = next_valid_send_utc_naive(_camp(), aware)
        self.assertEqual(out, utc_naive)

    def test_empty_window_raises(self):
        with self.assertRaises(ValueError):
            next_valid_send_utc_naive(
                _camp(send_hour_start=14, send_hour_end=14),
                datetime(2026, 6, 8, 13, 0, 0),
            )


class TestNextValidSendAfterCurrentWindow(unittest.TestCase):
    def test_inside_weekday_window_jumps_to_next_morning(self):
        # Seg 8 Jun 2026 15:00 BRT = 18:00 UTC → próximo = Ter 9 Jun 08:00 BRT = 11:00 UTC
        utc_naive = datetime(2026, 6, 8, 18, 0, 0)
        out = next_valid_send_after_current_window_utc_naive(_camp(), utc_naive)
        self.assertEqual(out, datetime(2026, 6, 9, 11, 0, 0))

    def test_friday_afternoon_skips_weekend(self):
        # Sex 12 Jun 2026 16:00 BRT = 19:00 UTC → Seg 15 Jun 08:00 BRT = 11:00 UTC
        utc_naive = datetime(2026, 6, 12, 19, 0, 0)
        out = next_valid_send_after_current_window_utc_naive(_camp(), utc_naive)
        self.assertEqual(out, datetime(2026, 6, 15, 11, 0, 0))

    def test_outside_window_same_as_next_valid(self):
        # Sex 12 Jun 2026 07:00 BRT = 10:00 UTC (antes das 8h)
        utc_naive = datetime(2026, 6, 12, 10, 0, 0)
        out = next_valid_send_after_current_window_utc_naive(_camp(), utc_naive)
        self.assertEqual(out, next_valid_send_utc_naive(_camp(), utc_naive))


if __name__ == "__main__":
    unittest.main()
