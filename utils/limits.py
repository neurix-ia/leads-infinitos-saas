"""
Módulo compartilhado para verificação de limites diários.
Usado por worker_sender e worker_cadence.
"""

import os
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor

from utils.config import SUPER_ADMIN_EMAILS
from utils.campaign_send_policy import (
    INITIAL_CHUNK_DAILY_QUOTA_POLICY,
    effective_initial_daily_caps,
    initial_chunk_daily_quota_allows,
)
# Decisão 2026-10-12: teto global de 30 disparos/dia em qualquer plano (ver docs/MAPA_SISTEMA.md).
INFINITE_DAILY_SEND_OPTIONS = (10, 20, 30)

# campaign_stage_sends (stage initial): só estes status bloqueiam novo chunk na mesma instância
# (worker_cadence.schedule_next_initial_chunk, _continue_initial_chunk_core busy check).
# failed/done não entram — após pasta órfã (sync → failed), a instância volta a poder receber chunk.
# waiting_reconnect: instância desconectada; materialização pausada até reconexão.
#
# TD-7 / T8: inclui ``queued`` para alinhar com ``app.py`` (legado ou futuro). Após ``create_advanced_campaign``
# bem-sucedido, o worker grava ``status='running'`` na BD mesmo quando a API Uazapi devolve ``status=queued``
# na pasta — ``queued`` aqui é estado de *linha* em campaign_stage_sends, não o status remoto da API.
INITIAL_CHUNK_ACTIVE_SEND_STATUSES = (
    "scheduled",
    "running",
    "partial",
    "queued",
    "waiting_reconnect",
)

# Fonte única de regras de plano.
# validity_days: dias até expiração (a partir da data de aplicação ao usuário).
# starter_trial: 7 dias; 1 instância; 100 extrações/mês; 10 disparos/dia/instância; demais planos: 365 dias.
# daily_sends_per_instance_default = teto do plano. Decisão 2026-10-12: 30/dia em todos os planos pagos
# e no máximo 2000 extrações limpas/mês; CAMPAIGN_DAILY_LIMIT_MAX (30) continua sendo o teto por campanha.
CAMPAIGN_DAILY_LIMIT_MAX = 30
MONTHLY_EXTRACTION_LIMIT_MAX = 2000

PLAN_POLICY = {
    "starter": {
        "instance_limit": 1,
        "monthly_extraction_limit": 1000,
        "daily_sends_per_instance_default": 30,
        "infinite_daily_options": (),
        "validity_days": 365,
    },
    "starter_trial": {
        "instance_limit": 1,
        "monthly_extraction_limit": 100,
        "daily_sends_per_instance_default": 10,
        "infinite_daily_options": (),
        "validity_days": 7,
    },
    "pro": {
        "instance_limit": 2,
        "monthly_extraction_limit": 2000,
        "daily_sends_per_instance_default": 30,
        "infinite_daily_options": (),
        "validity_days": 365,
    },
    "scale": {
        "instance_limit": 4,
        "monthly_extraction_limit": 2000,
        "daily_sends_per_instance_default": 30,
        "infinite_daily_options": (),
        "validity_days": 365,
    },
    "infinite": {
        "instance_limit": 20,
        "monthly_extraction_limit": 2000,
        "daily_sends_per_instance_default": 30,
        "infinite_daily_options": INFINITE_DAILY_SEND_OPTIONS,
        "validity_days": 365,
    },
}

# Compatibilidade temporária para dados legados no banco.
LEGACY_LICENSE_TYPE_FALLBACK = {
    "semestral": "pro",
    "anual": "pro",
}

PLAN_PRIORITY = {"starter": 1, "starter_trial": 1, "pro": 2, "scale": 3, "infinite": 4}

_BRT_TZ = "America/Sao_Paulo"


def brt_date_equals_today_sql(column: str) -> str:
    """SQL fragment: calendar date of ``column`` (timestamptz stored as UTC) equals today in BRT."""
    return (
        f"date({column} AT TIME ZONE 'UTC' AT TIME ZONE '{_BRT_TZ}')"
        f" = date(NOW() AT TIME ZONE '{_BRT_TZ}')"
    )


def get_db_connection():
    conn = psycopg2.connect(
        host=os.environ.get('DB_HOST', 'localhost'),
        database=os.environ.get('DB_NAME', 'leads_infinitos'),
        user=os.environ.get('DB_USER', 'postgres'),
        password=os.environ.get('DB_PASSWORD', 'devpassword'),
        port=os.environ.get('DB_PORT', '5432'),
        cursor_factory=RealDictCursor,
    )
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
    return conn


def resolve_license_type(license_type: str, allow_legacy_fallback: bool = True):
    normalized = (license_type or "").strip().lower()
    if normalized in PLAN_POLICY:
        return normalized
    if allow_legacy_fallback:
        return LEGACY_LICENSE_TYPE_FALLBACK.get(normalized)
    return None


def get_plan_policy(license_type: str, allow_legacy_fallback: bool = True):
    resolved = resolve_license_type(license_type, allow_legacy_fallback=allow_legacy_fallback)
    if not resolved:
        return PLAN_POLICY["starter"]
    return PLAN_POLICY[resolved]


def _pick_highest_plan(license_rows):
    resolved = []
    for row in license_rows or []:
        r = resolve_license_type(row.get("license_type"))
        if r:
            resolved.append(r)
    if not resolved:
        return "starter"
    return max(resolved, key=lambda plan: PLAN_PRIORITY.get(plan, 0))


def get_instance_daily_limit(user_id: int, instance_id: int = None) -> int:
    return get_user_daily_limit(user_id, instance_id=instance_id)


def get_user_daily_limit(user_id: int, instance_id: int = None) -> int:
    """
    Obtém a cota diária por instância para o usuário.
    Valor padrão definido por plano em PLAN_POLICY['daily_sends_per_instance_default'].
    Infinite pode sobrescrever por instância: 10/20/30/40/50.
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT license_type FROM licenses WHERE user_id = %s AND status = 'active' AND expires_at > NOW()",
                (user_id,),
            )
            rows = cur.fetchall()
            license_type = _pick_highest_plan(rows)
            plan = PLAN_POLICY[license_type]
            default_limit = int(plan["daily_sends_per_instance_default"])

            if license_type != "infinite":
                return default_limit

            if instance_id is not None:
                cur.execute(
                    """
                    SELECT daily_sends_per_instance
                    FROM instances
                    WHERE id = %s AND user_id = %s
                    LIMIT 1
                    """,
                    (instance_id, user_id),
                )
            else:
                cur.execute(
                    """
                    SELECT daily_sends_per_instance
                    FROM instances
                    WHERE user_id = %s
                    ORDER BY id ASC
                    LIMIT 1
                    """,
                    (user_id,),
                )
            instance_row = cur.fetchone() or {}
            configured = instance_row.get("daily_sends_per_instance")
            if configured in INFINITE_DAILY_SEND_OPTIONS:
                return int(configured)
            return default_limit
    finally:
        conn.close()


def get_sent_today_count(user_id: int) -> int:
    """
    Conta apenas disparos iniciais enviados hoje (BRT) pelo usuário.
    """
    query = """
    SELECT COUNT(cl.id) as count
    FROM campaign_leads cl
    JOIN campaigns c ON cl.campaign_id = c.id
    WHERE c.user_id = %s
      AND cl.status = 'sent'
      AND (
          COALESCE(cl.current_step, 1) = 1
          OR COALESCE(cl.last_sent_stage, '') = 'initial'
      )
      AND {brt_today}
    """.format(brt_today=brt_date_equals_today_sql("cl.sent_at"))
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (user_id,))
            row = cur.fetchone()
        return row['count'] if row else 0
    finally:
        conn.close()


def get_sent_today_count_by_instance(instance_id: int) -> int:
    """
    Conta campanhas Uazapi criadas hoje para esta instância.
    Usado para limite 1 campanha por instância por dia.
    Usa tabela uazapi_instance_sends (registrada ao criar campanha).
    """
    query = """
    SELECT COUNT(*) as count
    FROM uazapi_instance_sends
    WHERE instance_id = %s
      AND date(created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')
          = date(NOW() AT TIME ZONE 'America/Sao_Paulo')
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (instance_id,))
            row = cur.fetchone()
        return row['count'] if row else 0
    finally:
        conn.close()


# Limite de chunks por instância/dia removido para evitar travas.
# Antes: 8 chunks/dia. Agora: sem limite (bloqueio só por chunk ativo).
def can_create_campaign_today(instance_id: int) -> bool:
    """Sempre permite criar chunks (limite diário removido)."""
    return True


def get_sent_today_campaign_initial_count(campaign_id: int) -> int:
    """
    Conta disparos iniciais enviados hoje (BRT) apenas nesta campanha.
    Mesma definição de \"inicial\" que `get_sent_today_count` / `check_daily_limit`
    (current_step=1 ou last_sent_stage='initial'), com filtro `c.id = campaign_id`.

    Campanhas com fila outbox: usa o maior entre ``campaign_leads`` e linhas outbox
    ``initial`` com ``status='sent'`` hoje (evita cota errada se o lead ainda está ``pending``).
    """
    query = """
    SELECT COUNT(cl.id) as count
    FROM campaign_leads cl
    JOIN campaigns c ON cl.campaign_id = c.id
    WHERE c.id = %s
      AND cl.status = 'sent'
      AND (
          COALESCE(cl.current_step, 1) = 1
          OR COALESCE(cl.last_sent_stage, '') = 'initial'
      )
      AND {brt_today}
    """.format(brt_today=brt_date_equals_today_sql("cl.sent_at"))
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (campaign_id,))
            row = cur.fetchone()
        lead_count = int(row["count"] if row else 0)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM campaign_message_outbox WHERE campaign_id = %s LIMIT 1",
                (campaign_id,),
            )
            if not cur.fetchone():
                return lead_count
            outbox_brt_today = brt_date_equals_today_sql("updated_at")
            cur.execute(
                f"""
                SELECT COUNT(*) AS count
                FROM campaign_message_outbox
                WHERE campaign_id = %s
                  AND LOWER(TRIM(stage)) = 'initial'
                  AND status = 'sent'
                  AND {outbox_brt_today}
                """,
                (campaign_id,),
            )
            outbox_row = cur.fetchone()
        outbox_count = int(outbox_row["count"] if outbox_row else 0)
        return max(lead_count, outbox_count)
    finally:
        conn.close()


def initial_chunk_quota_snapshot(campaign_id: int) -> dict:
    """Diagnóstico: cotas, envios hoje e slots restantes para enfileirar iniciais."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT user_id, daily_limit FROM campaigns WHERE id = %s",
                (campaign_id,),
            )
            row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return {"allows_more": False, "remaining_slots": 0}

    owner_id = int(row["user_id"])
    plan_limit = int(get_user_daily_limit(owner_id))
    raw_cap = row.get("daily_limit")
    campaign_cap = int(raw_cap) if raw_cap is not None else plan_limit
    if campaign_cap <= 0:
        campaign_cap = plan_limit

    sent_user = int(get_sent_today_count(owner_id))
    sent_campaign = int(get_sent_today_campaign_initial_count(campaign_id))
    caps = effective_initial_daily_caps(plan_limit, campaign_cap, INITIAL_CHUNK_DAILY_QUOTA_POLICY)
    pol = caps["policy"]

    user_rem = max(0, int(caps["user_cap"] or 0) - sent_user) if caps["user_cap"] is not None else None
    camp_rem = max(0, int(caps["campaign_cap"] or 0) - sent_campaign) if caps["campaign_cap"] is not None else None

    if pol == "g1":
        remaining = user_rem or 0
    elif pol == "g3":
        remaining = camp_rem or 0
    else:
        remaining = min(user_rem or 0, camp_rem or 0)

    allows = initial_chunk_daily_quota_allows(
        sent_user,
        sent_campaign,
        plan_daily_limit=plan_limit,
        campaign_daily_limit=campaign_cap,
        policy=INITIAL_CHUNK_DAILY_QUOTA_POLICY,
    )
    return {
        "policy": pol,
        "plan_limit": plan_limit,
        "campaign_cap": campaign_cap,
        "sent_user_today": sent_user,
        "sent_campaign_today": sent_campaign,
        "allows_more": allows,
        "remaining_slots": remaining,
    }


def check_initial_chunk_daily_quota_for_campaign(
    campaign_id: int,
    instance_id: Optional[int] = None,
    policy: Optional[str] = None,
) -> bool:
    """
    True se a política TD-12 (default G3) permite mais envios iniciais hoje para a campanha.

    Usa sempre `campaigns.user_id` como dono da cota (AC14 / campanhas criadas por admin).
    """
    row = None
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT user_id, daily_limit FROM campaigns WHERE id = %s",
                (campaign_id,),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        return False

    owner_id = int(row["user_id"])
    plan_limit = int(get_user_daily_limit(owner_id, instance_id=instance_id))
    raw_cap = row.get("daily_limit")
    campaign_cap = int(raw_cap) if raw_cap is not None else plan_limit
    if campaign_cap <= 0:
        campaign_cap = plan_limit

    sent_user = int(get_sent_today_count(owner_id))
    sent_campaign = int(get_sent_today_campaign_initial_count(campaign_id))

    return initial_chunk_daily_quota_allows(
        sent_user,
        sent_campaign,
        plan_daily_limit=plan_limit,
        campaign_daily_limit=campaign_cap,
        policy=policy or INITIAL_CHUNK_DAILY_QUOTA_POLICY,
    )


def check_daily_limit(user_id: int, plan_limit: int) -> bool:
    """
    Verifica se o usuário já atingiu o limite diário de disparos.
    Retorna True se PODE enviar, False se atingiu o limite.
    Conta apenas disparos iniciais enviados hoje (BRT).
    """
    query = """
    SELECT COUNT(cl.id) as count
    FROM campaign_leads cl
    JOIN campaigns c ON cl.campaign_id = c.id
    WHERE c.user_id = %s
      AND cl.status = 'sent'
      AND (
          COALESCE(cl.current_step, 1) = 1
          OR COALESCE(cl.last_sent_stage, '') = 'initial'
      )
      AND date(cl.sent_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')
          = date(NOW() AT TIME ZONE 'America/Sao_Paulo')
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (user_id,))
            row = cur.fetchone()
        return row['count'] < plan_limit
    finally:
        conn.close()
