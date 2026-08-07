"""Política de planos: starter_trial + Scale 60 com teto por campanha 30."""

from utils.campaign_send_policy import initial_chunk_daily_quota_allows
from utils.limits import CAMPAIGN_DAILY_LIMIT_MAX, get_plan_policy


def test_get_plan_policy_starter_trial_limits():
    policy = get_plan_policy("starter_trial")
    assert policy["instance_limit"] == 1
    assert policy["monthly_extraction_limit"] == 100
    assert policy["daily_sends_per_instance_default"] == 10


def test_get_plan_policy_scale_is_60():
    policy = get_plan_policy("scale")
    assert policy["instance_limit"] == 4
    assert policy["monthly_extraction_limit"] == 4000
    assert policy["daily_sends_per_instance_default"] == 60
    assert CAMPAIGN_DAILY_LIMIT_MAX == 30


def test_g3_allows_two_campaigns_at_30_under_scale_plan():
    """Plano 60 não limita g3: cada campanha com daily_limit 30 é independente."""
    assert initial_chunk_daily_quota_allows(
        sent_today_user=30,
        sent_today_campaign_initial=0,
        plan_daily_limit=60,
        campaign_daily_limit=30,
        policy="g3",
    )
    assert initial_chunk_daily_quota_allows(
        sent_today_user=60,
        sent_today_campaign_initial=29,
        plan_daily_limit=60,
        campaign_daily_limit=30,
        policy="g3",
    )
    assert not initial_chunk_daily_quota_allows(
        sent_today_user=0,
        sent_today_campaign_initial=30,
        plan_daily_limit=60,
        campaign_daily_limit=30,
        policy="g3",
    )
