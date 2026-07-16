"""
Próximo instante válido para agendamento/materialização alinhado à janela BRT da campanha.

TD-9 / TD-11 (tech-spec recuperacao-scheduled-stale-worker-cadence-uazapi):
``next_valid_send_utc_naive`` devolve o primeiro ``datetime`` UTC **naive** (mesma convenção
do pipeline: ``datetime.utcnow()``, ``scheduled_for`` na BD) em que ``is_campaign_send_window``
é verdadeiro, opcionalmente respeitando ``margin_minutes`` aplicado em UTC antes da busca
(ex.: folga para janela de materialize / ``force_send_ids``).

``is_campaign_send_window`` foi concentrado aqui para o worker e para a Flask app importarem
o mesmo critério (antes só existia em ``worker_cadence``).
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytz

BUSINESS_HOUR_START = 8
BUSINESS_HOUR_END = 20
BRAZIL_TZ = pytz.timezone("America/Sao_Paulo")


def is_campaign_send_window(campaign: dict, now_brazil=None) -> bool:
    """
    Janela por campanha: hora do dia + opcional sábado/domingo.
    Fora da janela ou em fim de semana bloqueado: não dispara envios / process_campaign_sends.
    """
    now_brazil = now_brazil or datetime.now(BRAZIL_TZ)
    wd = now_brazil.weekday()
    if wd == 5 and not bool(campaign.get("send_saturday")):
        return False
    if wd == 6 and not bool(campaign.get("send_sunday")):
        return False
    try:
        sh = int(
            campaign.get("send_hour_start")
            if campaign.get("send_hour_start") is not None
            else BUSINESS_HOUR_START
        )
        eh = int(
            campaign.get("send_hour_end")
            if campaign.get("send_hour_end") is not None
            else BUSINESS_HOUR_END
        )
    except (TypeError, ValueError):
        sh, eh = BUSINESS_HOUR_START, BUSINESS_HOUR_END
    h = now_brazil.hour
    return sh <= h < eh


def _as_utc_naive(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(pytz.UTC).replace(tzinfo=None)


def _campaign_send_hour_bounds(campaign: dict) -> tuple[int, int]:
    try:
        sh = int(
            campaign.get("send_hour_start")
            if campaign.get("send_hour_start") is not None
            else BUSINESS_HOUR_START
        )
        eh = int(
            campaign.get("send_hour_end")
            if campaign.get("send_hour_end") is not None
            else BUSINESS_HOUR_END
        )
    except (TypeError, ValueError):
        sh, eh = BUSINESS_HOUR_START, BUSINESS_HOUR_END
    return sh, eh


def next_valid_send_utc_naive(
    campaign: dict,
    from_utc_naive: datetime,
    *,
    margin_minutes: int = 0,
    max_search_days: int = 14,
) -> datetime:
    """
    Menor instante >= ``from_utc_naive`` + ``margin_minutes`` (em UTC) tal que, em BRT,
    ``is_campaign_send_window(campaign)`` é True.

    ``from_utc_naive`` com ``tzinfo`` é normalizado para UTC antes da busca.

    Raises:
        ValueError: janela impossível (ex. ``send_hour_start`` >= ``send_hour_end``) ou
            nenhum slot em ``max_search_days``.
    """
    start_utc_naive = _as_utc_naive(from_utc_naive) + timedelta(minutes=int(margin_minutes))
    utc = pytz.UTC
    br = utc.localize(start_utc_naive).astimezone(BRAZIL_TZ)

    # Detectar janela vazia (evita loop longo inútil)
    sh, eh = _campaign_send_hour_bounds(campaign)
    if sh >= eh:
        raise ValueError(
            "campaign send window is empty or invalid (send_hour_start >= send_hour_end)"
        )

    max_minutes = max(1, int(max_search_days)) * 24 * 60
    for _ in range(max_minutes + 1):
        if is_campaign_send_window(campaign, now_brazil=br):
            return br.astimezone(utc).replace(tzinfo=None)
        br = br + timedelta(minutes=1)

    raise ValueError(
        f"no valid send window within {max_search_days} days for this campaign configuration"
    )


def next_valid_send_after_current_window_utc_naive(
    campaign: dict,
    from_utc_naive: datetime,
    *,
    max_search_days: int = 14,
) -> datetime:
    """
    Primeiro instante válido **depois** do fim da janela BRT actual (ou da corrente se
    já estamos fora). Usado quando a cota diária esgotou: evita ``next_run_at=agora``
    enquanto ainda estamos dentro de ``send_hour_start``–``send_hour_end``.
    """
    start_utc_naive = _as_utc_naive(from_utc_naive)
    utc = pytz.UTC
    br = utc.localize(start_utc_naive).astimezone(BRAZIL_TZ)
    _sh, eh = _campaign_send_hour_bounds(campaign)
    if _sh >= eh:
        raise ValueError(
            "campaign send window is empty or invalid (send_hour_start >= send_hour_end)"
        )

    if is_campaign_send_window(campaign, now_brazil=br):
        # Janela é ``sh <= h < eh`` → às ``eh:00`` já estamos fora.
        end_br = br.replace(hour=eh, minute=0, second=0, microsecond=0)
        if end_br <= br:
            end_br = br + timedelta(minutes=1)
        start_utc_naive = end_br.astimezone(utc).replace(tzinfo=None)

    return next_valid_send_utc_naive(
        campaign, start_utc_naive, margin_minutes=0, max_search_days=max_search_days
    )
