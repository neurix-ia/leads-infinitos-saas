"""
Regras puras do disparo via n8n (sem BD): janela, cota diária, variação de mensagem, auth.

Contrato espelha o CRM Neurix (`routers/n8n_tools.py`): o n8n pergunta "posso mandar o próximo?"
e recebe targets ou ``wait_seconds``. Toda a inteligência fica aqui, o workflow só obedece.
"""

from __future__ import annotations

import hmac
import random
from datetime import datetime, timezone
from typing import Any, Optional

import pytz

from utils.limits import CAMPAIGN_DAILY_LIMIT_MAX
from utils.next_valid_uazapi_send import (
    is_campaign_send_window,
    next_valid_send_after_current_window_utc_naive,
    next_valid_send_utc_naive,
)

BRAZIL_TZ = pytz.timezone("America/Sao_Paulo")

STEP_TO_STAGE = {1: "initial", 2: "follow1", 3: "follow2", 4: "breakup"}
STAGE_LABELS = {1: "Inicial", 2: "Follow-up 1", 3: "Follow-up 2", 4: "Break-up"}

# Mesmos defaults do CRM (segundos). ``campaigns.outbox_delay_*_seconds`` sobrescreve.
DEFAULT_MIN_DELAY = 180
DEFAULT_MAX_DELAY = 540
MAX_WAIT_SECONDS = 7 * 24 * 3600
PAUSED_RECHECK_SECONDS = 300


def api_key_matches(provided: Optional[str], expected: Optional[str]) -> bool:
    """Compara ``X-CRM-API-Key`` em tempo constante. Secret vazio nunca autentica."""
    exp = (expected or "").strip()
    if not exp:
        return False
    return hmac.compare_digest((provided or "").strip(), exp)


def campaign_daily_cap(campaign: dict) -> int:
    """Teto diário de envios da campanha (todas as etapas): min(daily_limit, 30), nunca < 1."""
    try:
        raw = int(campaign.get("daily_limit") or 0)
    except (TypeError, ValueError):
        raw = 0
    if raw <= 0:
        raw = CAMPAIGN_DAILY_LIMIT_MAX
    return max(1, min(raw, int(CAMPAIGN_DAILY_LIMIT_MAX)))


def campaign_delays_seconds(campaign: dict) -> tuple[int, int]:
    def _coerce(key: str, default: int) -> int:
        try:
            v = int(campaign.get(key) or 0)
        except (TypeError, ValueError):
            v = 0
        return v if v > 0 else default

    lo = _coerce("outbox_delay_min_seconds", DEFAULT_MIN_DELAY)
    hi = _coerce("outbox_delay_max_seconds", DEFAULT_MAX_DELAY)
    if lo > hi:
        lo, hi = hi, lo
    return lo, hi


def compute_dispatch_gate(
    campaign: dict,
    *,
    sent_today: int,
    campaign_paused: bool = False,
    now_utc: Optional[datetime] = None,
) -> dict[str, Any]:
    """
    Quanto o n8n deve esperar antes do próximo envio (0 = pode enviar agora).

    Ordem: pausa manual → janela da campanha (hora + sáb/dom, BRT) → cota diária da campanha.
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    now_br = now_utc.astimezone(BRAZIL_TZ)
    now_utc_naive = now_utc.astimezone(pytz.UTC).replace(tzinfo=None)
    cap = campaign_daily_cap(campaign)

    def _out(wait: int, reason: Optional[str], resume_utc_naive: Optional[datetime]) -> dict[str, Any]:
        resume_iso = None
        if resume_utc_naive is not None:
            resume_iso = pytz.UTC.localize(resume_utc_naive).isoformat()
        return {
            "within_window": bool(is_campaign_send_window(campaign, now_brazil=now_br)),
            "wait_seconds": int(max(0, min(wait, MAX_WAIT_SECONDS))),
            "wait_reason": reason,
            "resume_at": resume_iso,
            "daily_limit": cap,
            "daily_remaining": max(0, cap - int(sent_today)),
            "quota_exhausted": int(sent_today) >= cap,
        }

    if campaign_paused:
        return _out(PAUSED_RECHECK_SECONDS, "paused", None)

    if not is_campaign_send_window(campaign, now_brazil=now_br):
        resume = next_valid_send_utc_naive(campaign, now_utc_naive, margin_minutes=0)
        wait = int((resume - now_utc_naive).total_seconds())
        return _out(max(1, wait), "window", resume)

    if int(sent_today) >= cap:
        resume = next_valid_send_after_current_window_utc_naive(campaign, now_utc_naive)
        wait = int((resume - now_utc_naive).total_seconds())
        return _out(max(1, wait), "quota", resume)

    return _out(0, None, None)


def pick_variation(messages: list[str], rng: Optional[random.Random] = None) -> str:
    """Sorteia uma variação. ``{{nome}}`` fica no texto: o workflow n8n substitui por target."""
    msgs = [str(m).strip() for m in (messages or []) if str(m).strip()]
    if not msgs:
        return ""
    r = rng or random
    return r.choice(msgs)


def assign_round_robin(instance_ids: list[int], count: int, round_robin: bool) -> list[int]:
    """Instância por target. Sem round-robin: sempre a primeira."""
    if not instance_ids or count <= 0:
        return []
    if not round_robin:
        return [int(instance_ids[0])] * count
    n = len(instance_ids)
    return [int(instance_ids[i % n]) for i in range(count)]
