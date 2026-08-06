from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    send_file,
    flash,
    abort,
    jsonify,
    Response,
    session,
    has_request_context,
)
from flask_login import (
    LoginManager,
    UserMixin,
    login_user,
    login_required,
    logout_user,
    current_user,
)
from werkzeug.security import generate_password_hash, check_password_hash
from flask_mail import Mail, Message

import os
import random
import secrets
import string
import json
import threading
import time
from datetime import datetime, timedelta
import redis
from rq import Queue
import psycopg2
from psycopg2 import sql as psycopg2_sql
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from main import run_scraper_with_progress
import requests
from services.uazapi import UazapiService
import re
import pandas as pd
import io
import csv
import zipfile
from openai import OpenAI
from functools import wraps
from typing import Optional
import pytz
from utils.limits import (
    INITIAL_CHUNK_ACTIVE_SEND_STATUSES,
    LEGACY_LICENSE_TYPE_FALLBACK,
    PLAN_POLICY,
    INFINITE_DAILY_SEND_OPTIONS,
    get_plan_policy,
    get_sent_today_campaign_initial_count,
    get_user_daily_limit,
    resolve_license_type,
)
from utils.cadence_uazapi import iter_fu1_folder_ids, merge_fu1_folder_into_config, parse_cadence_config
from utils.lead_numeric_parse import coerce_lead_numeric_fields
from utils.campaign_dispatch_audit import append_dispatch_audit_event
from utils.uazapi_support_notify import (
    fetch_reconnect_inapp_alerts_for_user,
    get_instance_status_cached,
    is_instance_disconnected_status,
)


load_dotenv()

from utils.config import (
    EXPOSE_PROMETHEUS_METRICS,
    SUPER_ADMIN_EMAILS,
    USE_MESSAGE_OUTBOX,
)

# PROVISION_API_SECRET: token server-to-server para POST /api/provision/* (usuário e licença).
# Em produção, defina um segredo forte no ambiente antes de expor essas rotas.
# Se ausente ou string vazia, as rotas de provisionamento respondem 401 (não 503), para não
# permitir criação de usuários ou concessão de licença sem autenticação explícita por engano.
# Cliente: Authorization: Bearer <token> ou header X-Provision-Token com o mesmo valor.
PROVISION_API_SECRET = (os.environ.get("PROVISION_API_SECRET") or "").strip()

# Throttling para warning de stats Uazapi (evitar spam a cada polling do dashboard)
_stats_uazapi_warning_last = {}  # campaign_id -> timestamp


def _stats_uazapi_warning_cooldown_sec() -> int:
    raw = (os.environ.get("STATS_UAZAPI_WARNING_COOLDOWN_SEC") or "3600").strip()
    try:
        return max(120, min(int(raw), 86400))
    except ValueError:
        return 3600


STATS_UAZAPI_WARNING_COOLDOWN = _stats_uazapi_warning_cooldown_sec()
UAZAPI_SYNC_WEB_INTERVAL_MINUTES = 10

# Configuração Redis
REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')
redis_conn = redis.from_url(REDIS_URL)
q = Queue(connection=redis_conn)


def get_db_connection():
    conn = psycopg2.connect(
        host=os.environ.get('DB_HOST', 'localhost'),
        database=os.environ.get('DB_NAME', 'leads_infinitos'),
        user=os.environ.get('DB_USER', 'postgres'),
        password=os.environ.get('DB_PASSWORD', 'devpassword'),
        port=os.environ.get('DB_PORT', '5432')
    )
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
    return conn


_UI_SEND_TERMINAL_STATUSES = frozenset({"failed", "invalid"})


def sql_expr_campaign_lead_has_outbox_sent(lead_table_alias="campaign_leads"):
    """
    Expressão SQL reutilizável: há linha em campaign_message_outbox com status sent
    para o lead da linha externa. Usar na SELECT de listagens (tech-spec ui_send_status).

    lead_table_alias: identificador da tabela campaign_leads no FROM (ex.: campaign_leads ou cl).
    """
    alias = (lead_table_alias or "").strip()
    if not alias or not alias.replace("_", "").isalnum():
        raise ValueError("lead_table_alias deve ser identificador SQL seguro (alfanumérico + _)")
    return (
        f"EXISTS (SELECT 1 FROM campaign_message_outbox o "
        f"WHERE o.campaign_lead_id = {alias}.id AND o.status = 'sent')"
    )


def compute_ui_send_status(
    lead_status,
    *,
    has_outbox_sent=False,
    last_sent_stage=None,
    last_message_sent_at=None,
):
    """
    Deriva ui_send_status (pending | sent | failed | invalid) para a UI Editar Campanha.

    Regra única (BD + outbox, sem heurística HTTP no browser):
    - failed / invalid → preserva;
    - sent na coluna status → sent;
    - senão, outbox com status sent para o lead → sent;
    - senão, last_message_sent_at e last_sent_stage preenchidos (Uazapi/sync) → sent;
    - senão → pending.
    """
    s = (lead_status or "")
    if isinstance(s, str):
        s = s.strip().lower()
    elif s is not None:
        s = str(s).strip().lower()
    else:
        s = ""

    if s in _UI_SEND_TERMINAL_STATUSES:
        return s
    if s == "sent":
        return "sent"
    if has_outbox_sent:
        return "sent"

    stage_ok = last_sent_stage is not None and str(last_sent_stage).strip() != ""
    ts_ok = last_message_sent_at is not None
    if stage_ok and ts_ok:
        return "sent"
    return "pending"


def compute_ui_send_status_for_lead_row(row, *, has_outbox_sent=None):
    """Conveniência para RealDict: colunas campaign_leads + flag opcional do JOIN."""
    if has_outbox_sent is None:
        has_outbox_sent = bool(row.get("outbox_has_sent") or row.get("has_outbox_sent"))
    return compute_ui_send_status(
        row.get("status"),
        has_outbox_sent=has_outbox_sent,
        last_sent_stage=row.get("last_sent_stage"),
        last_message_sent_at=row.get("last_message_sent_at"),
    )


_KANBAN_STEP_TO_STAGE = {1: "initial", 2: "follow1", 3: "follow2", 4: "breakup"}


def kanban_column_stage_for_step(current_step):
    """Etapa de envio da coluna do Kanban (alinhar a ``getStageByStep`` em campaigns_kanban.html)."""
    try:
        n = int(current_step)
    except (TypeError, ValueError):
        return None
    return _KANBAN_STEP_TO_STAGE.get(n)


def compute_ui_sent_in_column_stage(row, *, outbox_sent_stages=None):
    """
    True se o envio da etapa da coluna em que o lead está (``current_step``) está confirmado:
    ``last_sent_stage`` coincide com a etapa da coluna e/ou existe outbox ``sent`` para esse par (lead, stage).
    ``outbox_sent_stages``: conjunto de stages em minúsculas vindos de ``campaign_message_outbox``.
    """
    column_stage = kanban_column_stage_for_step(row.get("current_step"))
    if not column_stage:
        return False
    last_sent = (row.get("last_sent_stage") or "").strip().lower()
    if last_sent == column_stage:
        return True
    if outbox_sent_stages and column_stage in outbox_sent_stages:
        return True
    return False


ACTIVE_LICENSE_TYPES = tuple(PLAN_POLICY.keys())
INSTANCE_LIMIT_REACHED_MESSAGE = "Limite de instâncias atingido. Contate o suporte para contratar instâncias adicionais"


def license_type_from_price(price_value) -> str:
    try:
        price = float(price_value or 0)
    except (TypeError, ValueError):
        price = 0.0

    if price >= 390.00:
        return "scale"
    if price >= 290.00:
        return "pro"
    return "starter"


def is_uazapi_for_all_users_enabled() -> bool:
    return str(os.environ.get("UAZAPI_FOR_ALL_USERS_ENABLED", "false")).strip().lower() in ("1", "true", "yes", "on")


def _get_user_plan_snapshot_for_limit(cur, user_id: int):
    cur.execute("SELECT id FROM users WHERE id = %s FOR UPDATE", (user_id,))
    user_row = cur.fetchone()
    if not user_row:
        return None

    cur.execute(
        """
        SELECT license_type
        FROM licenses
        WHERE user_id = %s
          AND status = 'active'
          AND expires_at > NOW()
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (user_id,),
    )
    license_row = cur.fetchone() or {}
    normalized_type = resolve_license_type(license_row.get("license_type")) or "starter"
    policy = get_plan_policy(normalized_type)

    cur.execute("SELECT COUNT(*) AS total FROM instances WHERE user_id = %s", (user_id,))
    row = cur.fetchone() or {}
    current_instances = int(row.get("total") or 0)

    return {
        "plan_type": normalized_type,
        "instance_limit": int(policy["instance_limit"]),
        "current_instances": current_instances,
    }


# Lock consultivo: serializa init_db entre réplicas web e evita deadlock DDL (AccessExclusive) vs workers (AccessShare).
INIT_DB_ADVISORY_LOCK_KEY = 873920145


def _init_db_lock_hot_tables(cur) -> None:
    """
    ACCESS EXCLUSIVE em ordem alfabética nas tabelas que workers e DDL disputam.
    Workers ficam em fila até o fim da transação de migração — evita deadlock.
    Em DB vazio (primeiro deploy), nenhuma tabela existe ainda; locks são no-op.
    """
    want = (
        "campaign_instances",
        "campaign_leads",
        "campaign_message_outbox",
        "campaign_send_attempts",
        "campaigns",
        "campaign_stage_sends",
        "instances",
        "licenses",
        "users",
    )
    cur.execute(
        """
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relkind IN ('r', 'p')
          AND c.relname = ANY(%s)
        ORDER BY 1
        """,
        (list(want),),
    )
    existing = [r[0] for r in cur.fetchall()]
    if not existing:
        return
    print(
        f"➡️ LOCK em {len(existing)} tabela(s) — workers aguardam até o fim da migração..."
    )
    for t in existing:
        cur.execute(
            psycopg2_sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE").format(
                psycopg2_sql.Identifier(t)
            )
        )


def init_db() -> None:
    """Migração idempotente com retry em deadlock / lock timeout (workers em paralelo)."""
    from psycopg2 import errors as psycopg2_errors

    last_err = None
    for attempt in range(5):
        try:
            _init_db_body()
            return
        except (
            psycopg2_errors.DeadlockDetected,
            psycopg2_errors.LockNotAvailable,
        ) as e:
            last_err = e
            print(
                f"⚠️ init_db: {type(e).__name__} (tentativa {attempt + 1}/5). "
                "Pare workers antigos ou aguarde — novo retry em ~2s..."
            )
            time.sleep(1.5 + random.uniform(0, 2.0))
    print("❌ init_db: esgotadas tentativas após deadlock/lock timeout.")
    if last_err:
        raise last_err
    raise RuntimeError("init_db failed")


def _init_db_body() -> None:
    print("🔄 Iniciando migração do banco de dados...")
    conn = get_db_connection()
    cur = conn.cursor()
    # Evita deploy pendurado para sempre se algo externo segurar lock (cancela após 2 min)
    cur.execute("SET lock_timeout = '120s'")
    cur.execute("SELECT pg_advisory_lock(%s)", (INIT_DB_ADVISORY_LOCK_KEY,))

    try:
        _init_db_lock_hot_tables(cur)
        # Tabela de usuários
        print("➡️ Verificando tabela users...")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                is_admin BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        cur.execute(
            """
            ALTER TABLE users ADD COLUMN IF NOT EXISTS display_name TEXT;
            """
        )
        cur.execute(
            """
            ALTER TABLE users ADD COLUMN IF NOT EXISTS phone_e164 TEXT;
            """
        )
    
        # Adicionar coluna is_admin se não existir (migração)
        print("➡️ Adicionando coluna is_admin se necessário...")
        cur.execute(
            """
            ALTER TABLE users ADD COLUMN IF NOT EXISTS is_admin BOOLEAN DEFAULT FALSE;
            """
        )
    
        # Tabela de licenças
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS licenses (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id),
                hotmart_purchase_id TEXT UNIQUE NOT NULL,
                hotmart_product_id TEXT NOT NULL,
                license_type TEXT NOT NULL CHECK (license_type IN ('starter', 'pro', 'scale', 'infinite')),
                status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'expired', 'cancelled')),
                purchase_date TIMESTAMP NOT NULL,
                expires_at TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
    
        # Tabela de webhooks da Hotmart
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS hotmart_webhooks (
                id SERIAL PRIMARY KEY,
                event_type TEXT NOT NULL,
                hotmart_purchase_id TEXT,
                payload TEXT NOT NULL,
                processed BOOLEAN DEFAULT FALSE,
                processed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
    
        # Tabela de configurações da Hotmart
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS hotmart_config (
                id SERIAL PRIMARY KEY,
                client_id TEXT NOT NULL,
                client_secret TEXT NOT NULL,
                webhook_secret TEXT,
                product_id TEXT NOT NULL,
                sandbox_mode BOOLEAN DEFAULT FALSE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
    
        # Tabela de webhooks da Hubla
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS hubla_webhooks (
                id SERIAL PRIMARY KEY,
                event_type TEXT NOT NULL,
                hubla_purchase_id TEXT,
                payload TEXT NOT NULL,
                processed BOOLEAN DEFAULT FALSE,
                processed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
    
        # Tabela de configurações da Hubla
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS hubla_config (
                id SERIAL PRIMARY KEY,
                webhook_token TEXT NOT NULL,
                product_id TEXT NOT NULL,
                sandbox_mode BOOLEAN DEFAULT FALSE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
    
        # Tabela de reset de senha
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS password_resets (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id),
                token TEXT UNIQUE NOT NULL,
                expires_at TIMESTAMP NOT NULL,
                used BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
    
        # Tabela de jobs de scraping
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS scraping_jobs (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id),
                keyword TEXT NOT NULL,
                locations TEXT NOT NULL,
                total_results INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
                progress INTEGER DEFAULT 0,
                current_location TEXT,
                results_path TEXT,
                error_message TEXT,
                lead_count INTEGER DEFAULT 0,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
    
        # Adicionar coluna lead_count se ainda não existir (compatibilidade com DBs existentes)
        cur.execute(
            """
            DO $$ 
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name='scraping_jobs' AND column_name='lead_count'
                ) THEN
                    ALTER TABLE scraping_jobs ADD COLUMN lead_count INTEGER DEFAULT 0;
                END IF;
            END $$;
            """
        )

        # Permitir status 'cancelled' no scraping_jobs (botão Cancelar)
        cur.execute(
            """
            ALTER TABLE scraping_jobs DROP CONSTRAINT IF EXISTS scraping_jobs_status_check;
            ALTER TABLE scraping_jobs ADD CONSTRAINT scraping_jobs_status_check
                CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled'));
            """
        )
    
        # Criar índice para queries de agregação mensal
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_scraping_jobs_user_date 
            ON scraping_jobs(user_id, created_at);
            """
        )
    
        # Tabela de histórico imutável de uso mensal (anti-bypass de limite)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS monthly_usage_history (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id),
                cycle_start DATE NOT NULL,
                cycle_end DATE NOT NULL,
                leads_extracted INTEGER NOT NULL DEFAULT 0,
                job_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
    
        # Índice para queries de limite mensal
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_monthly_usage_user_cycle 
            ON monthly_usage_history(user_id, cycle_start);
            """
        )

        # Tabela de instâncias do WhatsApp
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS instances (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id),
                name TEXT NOT NULL,
                server_url TEXT,
                apikey TEXT,
                status TEXT DEFAULT 'disconnected',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        cur.execute(
            """
            ALTER TABLE instances ADD COLUMN IF NOT EXISTS last_disconnect_notify_at TIMESTAMPTZ;
            """
        )
        # Worker: último estado de desconexão Uazapi (transição → notificação de reconexão)
        cur.execute(
            """
            ALTER TABLE instances ADD COLUMN IF NOT EXISTS worker_last_uazapi_disconnected BOOLEAN;
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS reconnect_inapp_alerts (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                instance_id INTEGER NOT NULL REFERENCES instances(id) ON DELETE CASCADE,
                instance_name TEXT,
                campaign_count INTEGER NOT NULL DEFAULT 0,
                body_text TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_reconnect_inapp_alerts_user_created
            ON reconnect_inapp_alerts(user_id, created_at DESC);
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS uazapi_reconnect_whatsapp_log (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                instance_id INTEGER NOT NULL REFERENCES instances(id) ON DELETE CASCADE,
                sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_uazapi_reconnect_wa_user_inst_sent
            ON uazapi_reconnect_whatsapp_log(user_id, instance_id, sent_at DESC);
            """
        )

        # Adicionar coluna api_provider (migração: MegaAPI vs Uazapi)
        cur.execute(
            """
            ALTER TABLE instances ADD COLUMN IF NOT EXISTS api_provider TEXT DEFAULT 'megaapi';
            """
        )

        # Configuração de limite diário por instância (somente plano Infinite)
        cur.execute(
            """
            ALTER TABLE instances ADD COLUMN IF NOT EXISTS daily_sends_per_instance INTEGER;
            """
        )
        cur.execute(
            """
            ALTER TABLE instances DROP CONSTRAINT IF EXISTS instances_daily_sends_per_instance_check;
            """
        )
        cur.execute(
            """
            ALTER TABLE instances ADD CONSTRAINT instances_daily_sends_per_instance_check
            CHECK (
                daily_sends_per_instance IS NULL
                OR daily_sends_per_instance IN (10, 20, 30, 40, 50)
            );
            """
        )

        # Migração: remover instâncias MegaAPI do superadmin (manter apenas Uazapi)
        print("➡️ Removendo instâncias MegaAPI do superadmin...")
        cur.execute(
            """
            DELETE FROM instances
            WHERE user_id IN (SELECT id FROM users WHERE email = ANY(%s))
            AND (api_provider IS NULL OR api_provider != 'uazapi');
            """,
            (list(SUPER_ADMIN_EMAILS),),
        )

        # Migração: manter apenas planos ativos no CHECK de license_type
        print("➡️ Atualizando constraint license_type para planos ativos...")
        cur.execute("""
            DO $$
            DECLARE r RECORD;
            BEGIN
                FOR r IN (
                    SELECT conname FROM pg_constraint c
                    WHERE conrelid = 'public.licenses'::regclass AND contype = 'c'
                    AND pg_get_constraintdef(c.oid) LIKE '%license_type%'
                )
                LOOP
                    EXECUTE 'ALTER TABLE licenses DROP CONSTRAINT ' || quote_ident(r.conname);
                END LOOP;
            END $$;
        """)
        cur.execute(
            "ALTER TABLE licenses DROP CONSTRAINT IF EXISTS licenses_license_type_check;"
        )
        # Garantir que nenhuma linha viole o novo CHECK (legado semestral/anual ou lixo → planos válidos).
        cur.execute(
            "UPDATE licenses SET license_type = lower(trim(license_type)) WHERE license_type IS NOT NULL;"
        )
        for legacy_key, resolved in LEGACY_LICENSE_TYPE_FALLBACK.items():
            cur.execute(
                "UPDATE licenses SET license_type = %s WHERE license_type = %s",
                (resolved, legacy_key.lower()),
            )
        allowed_license_types = tuple(PLAN_POLICY.keys())
        cur.execute(
            "UPDATE licenses SET license_type = %s WHERE license_type::text NOT IN %s",
            ("starter", allowed_license_types),
        )
        cur.execute("""
            ALTER TABLE licenses ADD CONSTRAINT licenses_license_type_check
            CHECK (license_type IN ('starter', 'starter_trial', 'pro', 'scale', 'infinite'));
        """)

        # Tabela de modelos de mensagem
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS message_templates (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id),
                name TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )

        # Tabela de campanhas
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS campaigns (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id),
                name TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                message_template TEXT,
                daily_limit INTEGER DEFAULT 0,
                closed_deals INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
    
        # Adicionar coluna closed_deals se não existir (migração)
        cur.execute(
            """
            ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS closed_deals INTEGER DEFAULT 0;
            """
        )

        # Adicionar coluna sent_today se não existir (migração)
        cur.execute(
            """
            ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS sent_today INTEGER DEFAULT 0;
            """
        )

        # Adicionar coluna scheduled_start se não existir (migração)
        cur.execute(
            """
            ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS scheduled_start TIMESTAMP;
            """
        )
    
        # Tabela de leads da campanha (Fila de Envio)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS campaign_leads (
                id SERIAL PRIMARY KEY,
                campaign_id INTEGER NOT NULL REFERENCES campaigns(id),
                phone TEXT NOT NULL,
                name TEXT,
                whatsapp_link TEXT,
                status TEXT DEFAULT 'pending',
                sent_at TIMESTAMP,
                log TEXT
            );
            """
        )
    
        # Adicionar coluna whatsapp_link se não existir (migração)
        cur.execute(
            """
            ALTER TABLE campaign_leads ADD COLUMN IF NOT EXISTS whatsapp_link TEXT;
            """
        )

        # Adicionar coluna sent_by_instance para rastrear qual instância enviou (migração)
        cur.execute(
            """
            ALTER TABLE campaign_leads ADD COLUMN IF NOT EXISTS sent_by_instance VARCHAR(255);
            """
        )

        # Tabela de junção: instâncias vinculadas a campanhas (multi-instance)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS campaign_instances (
                id SERIAL PRIMARY KEY,
                campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
                instance_id INTEGER NOT NULL REFERENCES instances(id) ON DELETE CASCADE,
                UNIQUE(campaign_id, instance_id)
            );
            """
        )

        # Adicionar coluna rotation_mode em campaigns (migração)
        cur.execute(
            """
            ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS rotation_mode TEXT DEFAULT 'single';
            """
        )

        # ============================================================
        # CADENCE FEATURE MIGRATIONS
        # ============================================================

        # Cadence toggle + config on campaigns
        cur.execute(
            """
            ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS enable_cadence BOOLEAN DEFAULT FALSE;
            ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS cadence_config JSONB DEFAULT '{}';
            ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS terms_accepted BOOLEAN DEFAULT FALSE;
            """
        )

        # Campaign Steps table — stores message content per cadence step
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS campaign_steps (
                id SERIAL PRIMARY KEY,
                campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
                step_number INTEGER NOT NULL,
                step_label TEXT NOT NULL DEFAULT '',
                message_template TEXT NOT NULL DEFAULT '[]',
                media_path TEXT,
                media_type TEXT,
                delay_days INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(campaign_id, step_number)
            );
            """
        )

        # Cadence tracking columns on campaign_leads
        cur.execute(
            """
            ALTER TABLE campaign_leads ADD COLUMN IF NOT EXISTS current_step INTEGER DEFAULT 1;
            ALTER TABLE campaign_leads ADD COLUMN IF NOT EXISTS cadence_status TEXT DEFAULT 'pending';
            ALTER TABLE campaign_leads ADD COLUMN IF NOT EXISTS snooze_until TIMESTAMP;
            ALTER TABLE campaign_leads ADD COLUMN IF NOT EXISTS last_message_sent_at TIMESTAMP;
            ALTER TABLE campaign_leads ADD COLUMN IF NOT EXISTS chatwoot_conversation_id INTEGER;
            ALTER TABLE campaign_leads ADD COLUMN IF NOT EXISTS campaign_tags TEXT[] DEFAULT '{}';
            ALTER TABLE campaign_leads ADD COLUMN IF NOT EXISTS notes TEXT;
            ALTER TABLE campaign_leads ADD COLUMN IF NOT EXISTS send_batch INTEGER DEFAULT NULL;
            ALTER TABLE campaign_leads ADD COLUMN IF NOT EXISTS last_sent_stage TEXT;
            ALTER TABLE campaign_leads ADD COLUMN IF NOT EXISTS last_sent_instance_id INTEGER;
            ALTER TABLE campaign_leads ADD COLUMN IF NOT EXISTS last_sent_instance_remote_jid TEXT;
            ALTER TABLE campaign_leads ADD COLUMN IF NOT EXISTS last_sent_folder_id TEXT;
            ALTER TABLE campaign_leads ADD COLUMN IF NOT EXISTS removed_from_funnel BOOLEAN DEFAULT FALSE;
            """
        )

        cur.execute(
            """
            ALTER TABLE campaign_leads ADD COLUMN IF NOT EXISTS csv_row_order INTEGER;
            """
        )
        cur.execute(
            """
            UPDATE campaign_leads cl
            SET csv_row_order = s.rn
            FROM (
                SELECT id, ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY id) AS rn
                FROM campaign_leads
            ) s
            WHERE cl.id = s.id AND cl.csv_row_order IS NULL;
            """
        )

        # uazapi_last_send_lead_ids para sync via listfolders (F9)
        cur.execute(
            """
            ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS uazapi_last_send_lead_ids JSONB;
            """
        )

        # Tabela para rastrear 1 campanha por instância por dia (F2)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS uazapi_instance_sends (
                id SERIAL PRIMARY KEY,
                instance_id INTEGER NOT NULL REFERENCES instances(id) ON DELETE CASCADE,
                campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
                created_at TIMESTAMP DEFAULT NOW()
            );
            """
        )

        # Tracking de envios por etapa/instância (redesenho funil completo Uazapi)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS campaign_stage_sends (
                id SERIAL PRIMARY KEY,
                campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
                stage TEXT NOT NULL,
                instance_id INTEGER NOT NULL REFERENCES instances(id) ON DELETE CASCADE,
                instance_remote_jid TEXT,
                uazapi_folder_id TEXT,
                scheduled_for TIMESTAMP,
                status TEXT DEFAULT 'scheduled',
                planned_count INTEGER DEFAULT 0,
                success_count INTEGER DEFAULT 0,
                failed_count INTEGER DEFAULT 0,
                lead_ids JSONB,
                delay_min_minutes INTEGER,
                delay_max_minutes INTEGER,
                message_variations JSONB,
                last_sync_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            );
            ALTER TABLE campaign_stage_sends ADD COLUMN IF NOT EXISTS delay_min_minutes INTEGER;
            ALTER TABLE campaign_stage_sends ADD COLUMN IF NOT EXISTS delay_max_minutes INTEGER;
            ALTER TABLE campaign_stage_sends ADD COLUMN IF NOT EXISTS message_variations JSONB;
            ALTER TABLE campaign_stage_sends ADD COLUMN IF NOT EXISTS fu_rollover_done BOOLEAN DEFAULT FALSE;
            ALTER TABLE campaign_stage_sends ADD COLUMN IF NOT EXISTS last_materialize_error TEXT;
            ALTER TABLE campaign_stage_sends ADD COLUMN IF NOT EXISTS materialize_attempt_count INTEGER DEFAULT 0;
            CREATE INDEX IF NOT EXISTS idx_campaign_stage_sends_campaign_stage
                ON campaign_stage_sends(campaign_id, stage);
            CREATE INDEX IF NOT EXISTS idx_campaign_stage_sends_folder_id
                ON campaign_stage_sends(uazapi_folder_id);
            CREATE INDEX IF NOT EXISTS idx_campaign_stage_sends_status
                ON campaign_stage_sends(status);
            CREATE INDEX IF NOT EXISTS idx_campaign_stage_sends_schedule
                ON campaign_stage_sends(scheduled_for);
            CREATE UNIQUE INDEX IF NOT EXISTS uq_campaign_stage_sends_window
                ON campaign_stage_sends(campaign_id, stage, instance_id, scheduled_for)
                WHERE scheduled_for IS NOT NULL AND status = 'scheduled';
            """
        )

        # T10: auditoria de flush admin de sends stale (Uazapi initial)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS admin_uazapi_stale_flush_audit (
                id SERIAL PRIMARY KEY,
                admin_user_id INTEGER NOT NULL REFERENCES users(id),
                campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
                dry_run BOOLEAN NOT NULL DEFAULT FALSE,
                recovery_mode TEXT NOT NULL DEFAULT 'recovery',
                force_any_campaign_status BOOLEAN NOT NULL DEFAULT FALSE,
                bumped_send_ids INTEGER[] NOT NULL DEFAULT '{}',
                failed_send_ids INTEGER[] NOT NULL DEFAULT '{}',
                dry_run_stale_send_ids INTEGER[] NOT NULL DEFAULT '{}',
                extra JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )

        # Fila outbox: envio unitário Uazapi (Postgres v1; tech-spec envio-individual-fila-intercalada)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS campaign_message_outbox (
                id SERIAL PRIMARY KEY,
                campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
                campaign_lead_id INTEGER NOT NULL REFERENCES campaign_leads(id) ON DELETE CASCADE,
                instance_id INTEGER NOT NULL REFERENCES instances(id) ON DELETE CASCADE,
                stage TEXT NOT NULL,
                step_priority INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending',
                queued_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                next_run_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                idempotency_key TEXT NOT NULL,
                uazapi_track_id TEXT,
                payload_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS campaign_send_attempts (
                id SERIAL PRIMARY KEY,
                outbox_id INTEGER NOT NULL REFERENCES campaign_message_outbox(id) ON DELETE CASCADE,
                attempt_no INTEGER NOT NULL,
                http_status INTEGER,
                uazapi_response TEXT,
                outcome TEXT NOT NULL,
                latency_ms INTEGER,
                started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                finished_at TIMESTAMP
            );
            COMMENT ON TABLE campaign_message_outbox IS
                'Fila de envio 1 mensagem/lead Uazapi. Cooldown entre envios: colunas ADR-2 em campaigns (ex.: outbox_delay_* em segundos); não usar delay_min_minutes/delay_max_minutes do legado como segundos outbox.';
            COMMENT ON COLUMN campaign_message_outbox.next_run_at IS
                'Momento mínimo para o worker fazer claim; unidade: mesmo relógio que o restante do app (timestamps sem tz).';
            COMMENT ON COLUMN campaign_message_outbox.payload_summary IS
                'JSONB sem PII (ex.: tipo de mídia, flags); não telefone nem corpo da mensagem.';
            COMMENT ON COLUMN campaign_send_attempts.uazapi_response IS
                'Evidência HTTP truncada no worker; evitar PII em volume.';
            CREATE UNIQUE INDEX IF NOT EXISTS uq_campaign_message_outbox_idempotency_key
                ON campaign_message_outbox (idempotency_key);
            CREATE UNIQUE INDEX IF NOT EXISTS uq_campaign_message_outbox_lead_stage
                ON campaign_message_outbox (campaign_lead_id, stage);
            CREATE INDEX IF NOT EXISTS idx_campaign_message_outbox_status_next_run
                ON campaign_message_outbox (status, next_run_at);
            CREATE INDEX IF NOT EXISTS idx_campaign_message_outbox_instance_next_run
                ON campaign_message_outbox (instance_id, next_run_at);
            CREATE INDEX IF NOT EXISTS idx_campaign_message_outbox_campaign_updated
                ON campaign_message_outbox (campaign_id, updated_at);
            CREATE UNIQUE INDEX IF NOT EXISTS uq_campaign_send_attempts_outbox_attempt
                ON campaign_send_attempts (outbox_id, attempt_no);
            CREATE INDEX IF NOT EXISTS idx_campaign_send_attempts_outbox
                ON campaign_send_attempts (outbox_id);
            """
        )

        # Pausa sistema vs utilizador (desconexão Uazapi — tech-spec desconexao-whatsapp)
        cur.execute(
            """
            ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS pause_origin TEXT;
            ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS pause_reason_code TEXT;
            ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS system_paused_at TIMESTAMP;
            COMMENT ON COLUMN campaigns.pause_origin IS
                'Origem da pausa: user, system, ou NULL (legado / não aplicável).';
            COMMENT ON COLUMN campaigns.pause_reason_code IS
                'Código quando pause_origin=system, ex.: instance_disconnected.';
            COMMENT ON COLUMN campaigns.system_paused_at IS
                'Registo de quando o sistema pausou a campanha (ex.: instância offline).';
            COMMENT ON COLUMN campaign_message_outbox.status IS
                'pending | sending | sent | failed; waiting_instance reservado para hold transitório.';
            """
        )

        # ============================================================
        # END CADENCE FEATURE MIGRATIONS
        # ============================================================

        # ============================================================
        # UAZAPI CAMPAIGN API MIGRATIONS
        # ============================================================

        # Colunas para campanhas via API Uazapi (envio em massa avançado)
        cur.execute(
            """
            ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS uazapi_folder_id TEXT;
            ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS use_uazapi_sender BOOLEAN DEFAULT false;
            ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS delay_min_minutes INTEGER;
            ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS delay_max_minutes INTEGER;
            """
        )

        # Horário comercial configurável por campanha (faixa de horários + sábado/domingo)
        cur.execute(
            """
            ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS send_hour_start INTEGER DEFAULT 8;
            ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS send_hour_end INTEGER DEFAULT 20;
            ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS send_saturday BOOLEAN DEFAULT false;
            ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS send_sunday BOOLEAN DEFAULT false;
            """
        )

        # Auditoria: quem criou a campanha pelo superadmin
        cur.execute(
            """
            ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS created_by_admin_id INTEGER REFERENCES users(id) ON DELETE SET NULL DEFAULT NULL;
            """
        )

        # ADR-2: cooldown outbox em segundos (não reutilizar delay_*_minutes do legado como segundos)
        cur.execute(
            """
            ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS outbox_delay_min_seconds INTEGER;
            ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS outbox_delay_max_seconds INTEGER;
            COMMENT ON COLUMN campaigns.outbox_delay_min_seconds IS
                'Limite inferior (s) do intervalo aleatório entre envios Uazapi outbox; definido na criação da campanha.';
            COMMENT ON COLUMN campaigns.outbox_delay_max_seconds IS
                'Limite superior (s) do intervalo aleatório entre envios Uazapi outbox; definido na criação da campanha.';
            """
        )

        # ============================================================
        # END UAZAPI CAMPAIGN API MIGRATIONS
        # ============================================================

        # Inserir configuração inicial da Hotmart se não existir
        cur.execute(
            """
            INSERT INTO hotmart_config 
            (client_id, client_secret, product_id, sandbox_mode) 
            VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING
            """,
            ('cb6bcde6-24cd-464f-80f3-e4efce3f048c', '7ee4a93d-1aec-473b-a8e6-1d0a813382e2', '5974664', True)
        )
    
        # Inserir configuração inicial da Hubla se não existir
        cur.execute(
            """
            INSERT INTO hubla_config 
            (webhook_token, product_id, sandbox_mode) 
            VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
            """,
            ('your-hubla-webhook-token', 'your-hubla-product-id', True)
        )
    
        conn.commit()
    except BaseException:
        try:
            conn.rollback()
        except BaseException:
            pass
        raise
    finally:
        try:
            conn.close()
        except BaseException:
            pass


class User(UserMixin):
    def __init__(self, id: int, email: str, password_hash: str, is_admin: bool = False):
        self.id = id
        self.email = email
        self.password_hash = password_hash
        self.is_admin = is_admin

    @staticmethod
    def get_by_id(user_id: int) -> "User | None":
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT id, email, password_hash, is_admin FROM users WHERE id = %s", (user_id,))
            row = cur.fetchone()
        conn.close()
        if row:
            return User(row[0], row[1], row[2], row[3])
        return None

    @staticmethod
    def get_by_email(email: str) -> "User | None":
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT id, email, password_hash, is_admin FROM users WHERE email = %s", (email,))
            row = cur.fetchone()
        conn.close()
        if row:
            return User(row[0], row[1], row[2], row[3])
        return None

    @staticmethod
    def create(email: str, password: str) -> "User":
        password_hash = generate_password_hash(password)
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (email, password_hash) VALUES (%s, %s) RETURNING id",
                (email, password_hash),
            )
            new_id = cur.fetchone()[0]
        conn.commit()
        conn.close()
        return User(new_id, email, password_hash, False)

    def has_active_license(self) -> bool:
        """Verifica se o usuário tem uma licença ativa"""
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT COUNT(*) as count FROM licenses 
                WHERE user_id = %s AND status = 'active' AND expires_at > NOW()
                """,
                (self.id,)
            )
            row = cur.fetchone()
        conn.close()
        return row['count'] > 0


class License:
    def __init__(self, id: int, user_id: int, hotmart_purchase_id: str, hotmart_product_id: str, 
                 license_type: str, status: str, purchase_date: str, expires_at: str):
        self.id = id
        self.user_id = user_id
        self.hotmart_purchase_id = hotmart_purchase_id
        self.hotmart_product_id = hotmart_product_id
        self.license_type = license_type
        self.status = status
        self.purchase_date = purchase_date
        self.expires_at = expires_at

    @property
    def daily_limit(self) -> int:
        return get_user_daily_limit(self.user_id)

    @property
    def monthly_extraction_limit(self) -> int:
        """Limite mensal de extração de leads (scraping)."""
        policy = get_plan_policy(self.license_type)
        return int(policy["monthly_extraction_limit"])

    @staticmethod
    def create(user_id: int, hotmart_purchase_id: str, hotmart_product_id: str, 
               license_type: str, purchase_date: str) -> "License":
        # Calcular data de expiração baseada no tipo de licença
        from datetime import datetime, timedelta
        
        # Garantir formato correto e impedir planos legados no fluxo ativo.
        license_type = (license_type or "").strip().lower()
        if license_type not in ACTIVE_LICENSE_TYPES:
            raise ValueError("license_type inválido. Use starter, pro, scale ou infinite.")
        
        purchase_dt = datetime.fromisoformat(purchase_date.replace('Z', '+00:00'))
        
        # Validade por plano: starter_trial = 7 dias; demais = 365 dias.
        policy = get_plan_policy(license_type)
        validity_days = int(policy.get("validity_days", 365))
        expires_at = purchase_dt + timedelta(days=validity_days)
        
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO licenses 
                (user_id, hotmart_purchase_id, hotmart_product_id, license_type, purchase_date, expires_at)
                VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
                """,
                (user_id, hotmart_purchase_id, hotmart_product_id, license_type, 
                 purchase_date, expires_at.isoformat())
            )
            new_id = cur.fetchone()[0]
        conn.commit()
        conn.close()
        
        return License(new_id, user_id, hotmart_purchase_id, hotmart_product_id, 
                      license_type, 'active', purchase_date, expires_at.isoformat())

    @staticmethod
    def get_by_user_id(user_id: int) -> list["License"]:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM licenses WHERE user_id = %s ORDER BY created_at DESC",
                (user_id,)
            )
            rows = cur.fetchall()
        conn.close()
        
        return [License(row['id'], row['user_id'], row['hotmart_purchase_id'], 
                       row['hotmart_product_id'], row['license_type'], row['status'],
                       row['purchase_date'], row['expires_at']) for row in rows]


# Decorator para rotas de admin
def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin:
            flash("Acesso não autorizado.", "error")
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function


def _verify_json_csrf():
    """T10: exige token igual ao da sessão (header ``X-CSRF-Token`` ou JSON ``csrf_token``)."""
    expected = session.get("csrf_token")
    token = request.headers.get("X-CSRF-Token") or request.headers.get("X-Csrf-Token")
    if not token and request.is_json:
        token = (request.get_json(silent=True) or {}).get("csrf_token")
    if not expected or not token:
        return (
            jsonify(
                {
                    "error": "csrf_invalid",
                    "message": "Token CSRF ausente ou inválido.",
                }
            ),
            403,
        )
    if not secrets.compare_digest(str(expected), str(token)):
        return (
            jsonify(
                {
                    "error": "csrf_invalid",
                    "message": "Token CSRF ausente ou inválido.",
                }
            ),
            403,
        )
    return None


def _require_provision_secret():
    """Retorna None se autorizado; caso contrário tupla (jsonify(...), 401)."""
    if not PROVISION_API_SECRET:
        return (
            jsonify({"error": "provision_unauthorized", "message": "Provisionamento não configurado."}),
            401,
        )

    token = None
    auth = (request.headers.get("Authorization") or "").strip()
    if auth:
        parts = auth.split(None, 1)
        if len(parts) == 2 and parts[0].lower() == "bearer":
            token = parts[1].strip()
    if token is None:
        token = (request.headers.get("X-Provision-Token") or "").strip()

    if not token or not secrets.compare_digest(str(token), str(PROVISION_API_SECRET)):
        return (
            jsonify({"error": "provision_unauthorized", "message": "Token de provisionamento inválido ou ausente."}),
            401,
        )
    return None


def _admin_stale_flush_rate_allow(user_id: int) -> bool:
    """T10: até 15 POST / minuto por admin (Redis)."""
    try:
        key = f"admin:uazapi:stale_flush:{int(user_id)}"
        n = redis_conn.incr(key)
        if n == 1:
            redis_conn.expire(key, 60)
        return n <= 15
    except Exception as exc:
        print(f"[admin_flush_stale] rate limit redis error: {exc}")
        return True


def _admin_outbox_poll_rate_allow(user_id: int) -> bool:
    """Task 7 / AC6: polling GET outbox-state — até 120 req/min por admin (Redis)."""
    try:
        key = f"admin:outbox:poll:{int(user_id)}"
        n = redis_conn.incr(key)
        if n == 1:
            redis_conn.expire(key, 60)
        return n <= 120
    except Exception as exc:
        print(f"[admin_outbox_poll] rate limit redis error: {exc}")
        return True


def _admin_outbox_mutate_rate_allow(user_id: int) -> bool:
    """Task 7: POST pause/resume outbox — até 30 req/min por admin (Redis)."""
    try:
        key = f"admin:outbox:mutate:{int(user_id)}"
        n = redis_conn.incr(key)
        if n == 1:
            redis_conn.expire(key, 60)
        return n <= 30
    except Exception as exc:
        print(f"[admin_outbox_mutate] rate limit redis error: {exc}")
        return True


def _campaign_sent_today_for_display(campaign_id: int) -> int:
    """Envios iniciais hoje (BRT) — mesma fonte que cotas diárias, não a coluna acumulada."""
    return int(get_sent_today_campaign_initial_count(campaign_id))


def _phase1_outbox_operator_is_superadmin(admin_id):
    """
    Fase 1 (ADR-4): criar/processar fila outbox exige operador em SUPER_ADMIN_EMAILS e flag no env.
    ``created_by_admin_id`` na campanha é só auditoria — não entra neste gate.

    Em pedido HTTP, exige ``current_user.id == admin_id`` e ``is_super_admin()`` (anti-confusão de sessão).
    Sem contexto de pedido (p.ex. scripts), infere pelo email do utilizador ``admin_id`` na BD.
    """
    if admin_id is None:
        return False
    if has_request_context() and getattr(current_user, "is_authenticated", False):
        if current_user.id != admin_id:
            return False
        return is_super_admin()
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT email FROM users WHERE id = %s", (admin_id,))
            row = cur.fetchone()
        return bool(row and row[0] and row[0] in SUPER_ADMIN_EMAILS)
    finally:
        conn.close()


def _require_message_outbox_phase1_api():
    """
    Fase 1 fila outbox: superadmin por email + USE_MESSAGE_OUTBOX (tech-spec ADR-4 / F17).
    Retorna None se autorizado; caso contrário (response, status_code).
    """
    if not current_user.is_authenticated:
        return jsonify({"error": "unauthorized", "message": "Autenticação necessária."}), 401
    if not is_super_admin():
        return (
            jsonify(
                {
                    "error": "forbidden",
                    "message": "Fila outbox (fase 1): apenas superadmin.",
                }
            ),
            403,
        )
    if not USE_MESSAGE_OUTBOX:
        return (
            jsonify(
                {
                    "error": "forbidden",
                    "message": "USE_MESSAGE_OUTBOX não está ativo no ambiente.",
                }
            ),
            403,
        )
    return None


def _isoformat_dt(dt):
    if dt is None:
        return None
    if isinstance(dt, datetime):
        return dt.isoformat()
    return str(dt)


def is_super_admin(user=None):
    """Verifica se o usuário é super admin (multi-instance feature)"""
    u = user or current_user
    return u.is_authenticated and u.email in SUPER_ADMIN_EMAILS

class Campaign:
    def __init__(self, id, user_id, name, status, message_template, daily_limit, created_at, closed_deals=0, scheduled_start=None, sent_today=0, rotation_mode='single', enable_cadence=False, terms_accepted=False, cadence_config=None, use_uazapi_sender=False, uazapi_folder_id=None, **kwargs):
        self.id = id
        self.user_id = user_id
        self.name = name
        self.status = status
        self.message_template = message_template
        self.daily_limit = daily_limit
        self.created_at = created_at
        self.closed_deals = closed_deals
        self.scheduled_start = scheduled_start
        self.sent_today = sent_today
        self.rotation_mode = rotation_mode
        self.enable_cadence = enable_cadence or False
        self.terms_accepted = terms_accepted or False
        self.cadence_config = cadence_config or {}
        self.use_uazapi_sender = bool(use_uazapi_sender)
        self.uazapi_folder_id = uazapi_folder_id

    @staticmethod
    def create(user_id: int, name: str, message_template: str, daily_limit: int) -> "Campaign":
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO campaigns (user_id, name, message_template, daily_limit)
                VALUES (%s, %s, %s, %s) RETURNING id, created_at
                """,
                (user_id, name, message_template, daily_limit)
            )
            row = cur.fetchone()
            new_id = row[0]
            created_at = row[1]
        conn.commit()
        conn.close()
        return Campaign(new_id, user_id, name, 'pending', message_template, daily_limit, created_at)

    @staticmethod
    def get_by_user(user_id: int):
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM campaigns WHERE user_id = %s ORDER BY created_at DESC", (user_id,))
            rows = cur.fetchall()
        conn.close()
        return [Campaign(**row) for row in rows]

    @staticmethod
    def get_by_id(campaign_id: int, user_id: int):
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM campaigns WHERE id = %s AND user_id = %s", (campaign_id, user_id))
            row = cur.fetchone()
        conn.close()
        if row:
            return Campaign(**row)
        return None

    def delete(self):
        """Exclui a campanha e seus leads associados"""
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # 1. Excluir leads da campanha
                cur.execute("DELETE FROM campaign_leads WHERE campaign_id = %s", (self.id,))
                
                # 2. Excluir a campanha
                cur.execute("DELETE FROM campaigns WHERE id = %s", (self.id,))
            conn.commit()
            return True
        except Exception as e:
            print(f"Erro ao excluir campanha {self.id}: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()


class CampaignLead:
    @staticmethod
    def add_leads(campaign_id: int, leads: list[dict]):
        """
        Adiciona leads à campanha em lote.
        leads = [{'phone': '...', 'name': '...', 'whatsapp_link': '...', 'address': '...', ...}]
        """
        if not leads:
            return
            
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # Incluir status='pending' explicitamente para garantir processamento pelo worker
                # Novas colunas de enriquecimento; csv_row_order = ordem da planilha (1..n)
                for l in leads:
                    coerce_lead_numeric_fields(l)
                args_str = ','.join(
                    cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", 
                               (campaign_id, l.get('phone'), l.get('name'), l.get('whatsapp_link'), 'pending',
                                l.get('address'), l.get('website'), l.get('category'), l.get('location'),
                                l.get('reviews_count'), l.get('reviews_rating'), l.get('latitude'), l.get('longitude'),
                                idx + 1,
                               )).decode('utf-8') 
                    for idx, l in enumerate(leads)
                )
                cur.execute("""
                    INSERT INTO campaign_leads 
                    (campaign_id, phone, name, whatsapp_link, status, address, website, category, location, reviews_count, reviews_rating, latitude, longitude, csv_row_order) 
                    VALUES 
                """ + args_str)
            conn.commit()
        except Exception as e:
            print(f"Erro ao adicionar leads: {e}")
            conn.rollback()
        finally:
            conn.close()


class MessageTemplate:
    def __init__(self, id, user_id, name, content, created_at):
        self.id = id
        self.user_id = user_id
        self.name = name
        self.content = content
        self.created_at = created_at

    @staticmethod
    def create(user_id: int, name: str, content: str) -> "MessageTemplate":
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO message_templates (user_id, name, content)
                VALUES (%s, %s, %s) RETURNING id, created_at
                """,
                (user_id, name, content)
            )
            row = cur.fetchone()
            new_id = row[0]
            created_at = row[1]
        conn.commit()
        conn.close()
        return MessageTemplate(new_id, user_id, name, content, created_at)

    @staticmethod
    def get_by_user(user_id: int):
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM message_templates WHERE user_id = %s ORDER BY created_at DESC", (user_id,))
            rows = cur.fetchall()
        conn.close()
        return [MessageTemplate(**row) for row in rows]


class HotmartService:
    def __init__(self):
        self.base_url = "https://developers.hotmart.com/payments/api/v1"
        self.config = self._get_config()
    
    def _get_config(self) -> dict:
        """Obtém configuração da Hotmart do banco"""
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM hotmart_config LIMIT 1")
            row = cur.fetchone()
        conn.close()
        
        if not row:
            raise Exception("Configuração da Hotmart não encontrada")
        
        return dict(row)
    
    def _get_auth_header(self) -> str:
        """Gera header de autenticação Basic"""
        import base64
        credentials = f"{self.config['client_id']}:{self.config['client_secret']}"
        encoded = base64.b64encode(credentials.encode()).decode()
        return f"Basic {encoded}"
    
    def verify_purchase(self, email: str) -> dict | None:
        """
        Verifica se o email tem uma compra válida do produto
        Retorna dados da compra ou None se não encontrada
        """
        import requests
        from datetime import datetime
        
        headers = {
            'Authorization': self._get_auth_header(),
            'Content-Type': 'application/json'
        }
        
        # Parâmetros para buscar vendas
        params = {
            'buyer_email': email,
            'product_id': self.config['product_id'],
            'status': 'approved'  # Apenas vendas aprovadas
        }
        
        try:
            response = requests.get(
                f"{self.base_url}/sales/history",
                headers=headers,
                params=params,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Verificar se há vendas aprovadas
                if data.get('items') and len(data['items']) > 0:
                    sale = data['items'][0]  # Pegar a venda mais recente
                    
                    return {
                        'purchase_id': sale.get('purchase_id'),
                        'product_id': sale.get('product_id'),
                        'buyer_email': sale.get('buyer_email'),
                        'purchase_date': sale.get('purchase_date'),
                        'status': sale.get('status'),
                        'price': sale.get('price'),
                        'currency': sale.get('currency')
                    }
            
            return None
            
        except Exception as e:
            print(f"Erro ao verificar compra na Hotmart: {e}")
            return None
    
    
    def process_webhook(self, payload: dict, signature: str) -> bool:
        """
        Processa webhook da Hotmart v2.0.0
        Retorna True se processado com sucesso
        """
        # Validação de Segurança (Hottok)
        expected_hottok = os.environ.get('HOTMART_HOTTOK')
        if not expected_hottok:
            print("⚠️ HOTMART_HOTTOK não configurado no .env")
            # Em dev/test, talvez permitir passar? Não, segurança primeiro.
            # Mas se não estiver configurado, não temos como validar.
            pass 
        elif signature != expected_hottok:
            print(f"❌ Assinatura inválida: Recebido={signature}, Esperado={expected_hottok}")
            return False

        # Extrair dados do Payload v2.0.0
        event = payload.get('event')
        data = payload.get('data', {})
        
        # Identificadores para Log
        purchase_id = data.get('purchase', {}).get('transaction')
        product_id = str(data.get('product', {}).get('id', ''))
        
        # Salvar webhook (Audit Log)
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO hotmart_webhooks (event_type, hotmart_purchase_id, payload)
                VALUES (%s, %s, %s)
                """,
                (event, purchase_id, json.dumps(payload))
            )
        conn.commit()
        conn.close()
        
        # Eventos de Interesse: PURCHASE_APPROVED ou PURCHASE_COMPLETE
        if event in ['PURCHASE_APPROVED', 'PURCHASE_COMPLETE']:
            return self._process_sale_approved(data)
        
        print(f"⚠️ Evento Hotmart ignorado: {event}")
        return True
    
    def _process_sale_approved(self, data: dict) -> bool:
        """
        Processa venda aprovada: 
        1. Cria usuário se não existir
        2. Cria/Atualiza licença
        """
        try:
            buyer = data.get('buyer', {})
            email = buyer.get('email')
            purchase = data.get('purchase', {})
            purchase_id = purchase.get('transaction')
            product = data.get('product', {})
            product_id = str(product.get('id', ''))
            
            # Data de Aprovação
            approved_date_ms = purchase.get('approved_date')
            if approved_date_ms:
                purchase_date = datetime.fromtimestamp(approved_date_ms / 1000).isoformat()
            else:
                purchase_date = datetime.utcnow().isoformat()

            if not email or not purchase_id:
                print("❌ Dados insuficientes no payload da Hotmart")
                return False
                
            email = email.lower().strip()
            
            # 1. Verificar/Criar Usuário
            user = User.get_by_email(email)
            if not user:
                # Gerar senha temporária segura
                temp_password = secrets.token_urlsafe(8)
                print(f"🆕 Criando usuário para {email} (Senha: {temp_password})")
                
                user = User.create(email, temp_password)
                
                # Enviar email com a senha
                print(f"📧 Enviando email de boas-vindas para {email}...")
                send_welcome_email(email, temp_password)

            
            # 2. Determinar tipo de licença pelos planos ativos.
            price_value = purchase.get('price', {}).get('value', 0)
            license_type = license_type_from_price(price_value)
                
            # 3. Verificar se licença já existe (Idempotência)
            existing_licenses = License.get_by_user_id(user.id)
            for lic in existing_licenses:
                if lic.hotmart_purchase_id == purchase_id:
                    print(f"ℹ️ Licença já existe para {email} (Purchase: {purchase_id})")
                    return True
            
            # 4. Criar Licença
            License.create(user.id, purchase_id, product_id, license_type, purchase_date)
            print(f"✅ Licença {license_type} criada com sucesso para {email}!")
            
            return True

        except Exception as e:
            print(f"❌ Erro ao processar venda aprovada: {e}")
            return False




class HublaService:
    def __init__(self):
        self.config = self._get_config()
    
    def _get_config(self) -> dict:
        """Obtém configuração da Hubla do banco"""
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM hubla_config LIMIT 1")
            row = cur.fetchone()
        conn.close()
        
        if not row:
            raise Exception("Configuração da Hubla não encontrada")
        
        return dict(row)
    
    def verify_webhook_signature(self, payload: str, signature: str) -> bool:
        """
        Verifica a assinatura do webhook da Hubla
        Baseado na documentação da Hubla, eles usam um token de autenticação
        """
        # TODO: Implementar validação de assinatura específica da Hubla
        # Por enquanto, apenas verifica se o token está presente
        expected_token = self.config.get('webhook_token')
        if not expected_token or expected_token == 'your-hubla-webhook-token':
            print("⚠️ Token da Hubla não configurado, aceitando webhook sem validação")
            return True
        
        # Aqui você pode implementar a validação específica da Hubla
        # Por exemplo, verificar se o signature contém o token
        return signature == expected_token or expected_token in signature
    
    def process_webhook(self, payload: dict, signature: str) -> bool:
        """
        Processa webhook da Hubla
        Retorna True se processado com sucesso
        """
        # Verificar assinatura
        if not self.verify_webhook_signature(json.dumps(payload), signature):
            print("❌ Assinatura do webhook da Hubla inválida")
            return False
        
        # Normalizar tipo de evento: v2 usa 'type' (string); v1 pode usar 'event' (string)
        raw_event_type = payload.get('type') or payload.get('event') or ''
        event_type = raw_event_type.lower() if isinstance(raw_event_type, str) else ''

        # Extrair identificador (purchase.id em v1; subscription.id em v2)
        purchase_id = None
        if payload.get('data', {}).get('purchase', {}).get('id'):
            purchase_id = payload.get('data', {}).get('purchase', {}).get('id')
        elif payload.get('purchase', {}).get('id'):
            purchase_id = payload.get('purchase', {}).get('id')
        else:
            evt_obj = payload.get('event') if isinstance(payload.get('event'), dict) else {}
            if evt_obj.get('subscription', {}) and evt_obj.get('subscription', {}).get('id'):
                purchase_id = evt_obj.get('subscription', {}).get('id')
        
        # Salvar webhook no banco
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO hubla_webhooks (event_type, hubla_purchase_id, payload)
                VALUES (%s, %s, %s)
                """,
                (event_type, purchase_id, json.dumps(payload))
            )
        conn.commit()
        conn.close()
        
        # v2: Membro > Acesso concedido → criar usuário automaticamente
        if event_type == 'customer.member_added':
            evt_obj = payload.get('event') if isinstance(payload.get('event'), dict) else {}
            return self._process_member_added_v2(evt_obj)

        # v2: confirmação financeira → criar licença
        if event_type in (
            'subscription.activated',
            'invoice.paid',
            'invoice.payment_succeeded',  # variação v2 observada na UI
            'payment_succeeded',          # fallback defensivo
        ):
            evt_obj = payload.get('event') if isinstance(payload.get('event'), dict) else {}
            return self._create_license_from_v2(evt_obj)

        # v1/várias integrações: eventos de compra com completed/approved
        if 'purchase' in event_type and ('completed' in event_type or 'approved' in event_type):
            return self._process_sale_completed(payload.get('data', {}))
        
        return True
    
    def _process_sale_completed(self, sale_data: dict) -> bool:
        """Processa evento de venda completada da Hubla"""
        try:
            # Extrair dados do formato da Hubla
            # A estrutura pode variar, então vamos ser flexíveis
            buyer_email = None
            purchase_id = None
            product_id = None
            purchase_date = None
            
            # Tentar diferentes estruturas possíveis
            if sale_data.get('buyer', {}).get('email'):
                buyer_email = sale_data.get('buyer', {}).get('email')
            elif sale_data.get('customer', {}).get('email'):
                buyer_email = sale_data.get('customer', {}).get('email')
            elif sale_data.get('user', {}).get('email'):
                buyer_email = sale_data.get('user', {}).get('email')
            
            if sale_data.get('purchase', {}).get('id'):
                purchase_id = sale_data.get('purchase', {}).get('id')
            
            if sale_data.get('product', {}).get('id'):
                product_id = str(sale_data.get('product', {}).get('id', ''))
            
            if sale_data.get('purchase', {}).get('created_at'):
                purchase_date = sale_data.get('purchase', {}).get('created_at')
            elif sale_data.get('purchase', {}).get('approved_at'):
                purchase_date = sale_data.get('purchase', {}).get('approved_at')
            
            if not all([buyer_email, purchase_id, product_id, purchase_date]):
                print(f"Dados insuficientes da Hubla: email={buyer_email}, purchase_id={purchase_id}, product_id={product_id}, date={purchase_date}")
                return False
            
            # Verificar se já existe licença para esta compra
            conn = get_db_connection()
            existing = conn.execute(
                "SELECT id FROM licenses WHERE hotmart_purchase_id = ?",
                (purchase_id,)
            ).fetchone()
            conn.close()
            
            if existing:
                return True  # Licença já existe
            
            # Determinar tipo de licença baseado no preço (sem planos legados).
            price = float(sale_data.get('purchase', {}).get('price', {}).get('value', 0))
            license_type = license_type_from_price(price)
            
            # Buscar usuário pelo email
            user = User.get_by_email(buyer_email)
            if user:
                # Criar licença para usuário existente
                License.create(user.id, purchase_id, product_id, license_type, purchase_date)
                print(f"Licença Hubla criada para {buyer_email}: {license_type} - {purchase_id}")
            else:
                # Usuário ainda não se registrou, a licença será criada quando ele se registrar
                print(f"Usuário Hubla {buyer_email} não encontrado. Licença será criada no registro.")
                pass
            
            return True
            
        except Exception as e:
            print(f"Erro ao processar venda completada da Hubla: {e}")
            return False

    def _process_member_added_v2(self, event_data: dict) -> bool:
        """Cria o usuário (se não existir) ao receber Hubla v2 customer.member_added.
        Espera payload no formato: { "type": "customer.member_added", "event": { "user": {"email": ...}, ... } }
        """
        try:
            # Extrair email do usuário. Preferir subscription.payer.email
            # pois o teste da Hubla muitas vezes popula apenas esse campo.
            user_email = None
            if isinstance(event_data, dict):
                if event_data.get('subscription', {}) and event_data.get('subscription', {}).get('payer', {}) and event_data.get('subscription', {}).get('payer', {}).get('email'):
                    user_email = event_data.get('subscription', {}).get('payer', {}).get('email')
                elif event_data.get('user', {}) and event_data.get('user', {}).get('email'):
                    user_email = event_data.get('user', {}).get('email')
                elif event_data.get('customer', {}) and event_data.get('customer', {}).get('email'):
                    user_email = event_data.get('customer', {}).get('email')

            if not user_email:
                print('Evento customer.member_added sem email do usuário')
                return False

            user_email = user_email.strip().lower()
            existing = User.get_by_email(user_email)
            if existing:
                return True

            # Criar usuário com senha temporária aleatória
            temp_password = generate_temp_password()
            User.create(user_email, temp_password)
            print(f"Usuário criado via Hubla member_added: {user_email}")
            return True
        except Exception as e:
            print(f"Erro ao processar member_added v2: {e}")
            return False

    def _create_license_from_v2(self, event_data: dict) -> bool:
        """Cria licença a partir de eventos v2 (subscription.activated, invoice.paid)."""
        try:
            # Extrair email (ordem de prioridade v2): invoice.payer.email > subscription.payer.email > event.payer.email > user.email
            buyer_email = None
            if isinstance(event_data, dict):
                invoice = event_data.get('invoice', {}) or {}
                inv_payer = invoice.get('payer', {}) if isinstance(invoice, dict) else {}
                buyer_email = (inv_payer or {}).get('email') or None

                if not buyer_email:
                    sub = event_data.get('subscription', {}) or {}
                    payer = sub.get('payer', {}) or {}
                    buyer_email = payer.get('email') or None

                if not buyer_email:
                    # Fallback: payer no nível do evento
                    evt_payer = event_data.get('payer', {}) or {}
                    buyer_email = evt_payer.get('email') or None

                if not buyer_email:
                    user = event_data.get('user', {}) or {}
                    buyer_email = user.get('email') or None

            if not buyer_email:
                print('Evento v2 sem email do comprador')
                return False

            buyer_email = buyer_email.strip().lower()

            # Extrair identificadores
            product_id = str((event_data.get('product', {}) or {}).get('id', '') or '')
            invoice = event_data.get('invoice', {}) or {}
            subscription = event_data.get('subscription', {}) or {}
            purchase_id = invoice.get('id') or subscription.get('id') or None

            # Datas
            purchase_date = (
                subscription.get('activatedAt')
                or invoice.get('paidAt')
                or invoice.get('createdAt')
                or invoice.get('updatedAt')
                or invoice.get('completedAt')
                or subscription.get('modifiedAt')
            )

            # Preço (para determinar tipo de licença)
            price = None
            amount = (invoice.get('amount') or {}) if isinstance(invoice, dict) else {}
            total_cents = (
                amount.get('totalCents')
                or amount.get('totalcents')
                or amount.get('valueCents')
            )
            if total_cents is not None:
                try:
                    price = float(total_cents) / 100.0
                except Exception:
                    price = None
            # Outros formatos possíveis
            if price is None:
                try:
                    price = float((invoice.get('price') or {}).get('value'))
                except Exception:
                    price = None

            if price is None:
                price = 297.00  # fallback seguro para plano Pro

            if not all([buyer_email, purchase_id, product_id]):
                print(f"Dados insuficientes v2: email={buyer_email}, purchase_id={purchase_id}, product_id={product_id}, date={purchase_date}")
                return False

            # Definir tipo de licença apenas para planos ativos.
            license_type = license_type_from_price(price)

            # Verificar se já existe licença para esta compra
            conn = get_db_connection()
            existing = conn.execute(
                "SELECT id FROM licenses WHERE hotmart_purchase_id = ?",
                (purchase_id,)
            ).fetchone()
            conn.close()
            if existing:
                return True

            # Garantir usuário
            user = User.get_by_email(buyer_email)
            if not user:
                temp_password = generate_temp_password()
                user = User.create(buyer_email, temp_password)

            # Normalizar purchase_date
            from datetime import datetime
            if not purchase_date:
                purchase_date = datetime.utcnow().isoformat() + 'Z'

            License.create(user.id, purchase_id, product_id, license_type, purchase_date)
            print(f"Licença v2 criada para {buyer_email}: {license_type} - {purchase_id}")
            return True
        except Exception as e:
            print(f"Erro ao criar licença v2: {e}")
            return False


def generate_temp_password(length=12):
    """Gera uma senha temporária aleatória"""
    characters = string.ascii_letters + string.digits + "!@#$%^&*"
    return ''.join(secrets.choice(characters) for _ in range(length))


class ScrapingJob:
    """Manages scraping jobs in the database"""
    
    @staticmethod
    def create(user_id: int, keyword: str, locations: list, total_results: int) -> int:
        """Create a new scraping job"""
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO scraping_jobs (user_id, keyword, locations, total_results)
                VALUES (%s, %s, %s, %s) RETURNING id
                """,
                (user_id, keyword, json.dumps(locations), total_results)
            )
            job_id = cur.fetchone()[0]
        conn.commit()
        conn.close()
        return job_id
    
    @staticmethod
    def update_status(job_id: int, status: str, progress: int = None, 
                     current_location: str = None, error_message: str = None):
        """Update job status and progress"""
        conn = get_db_connection()
        
        update_fields = ["status = %s"]
        params = [status]
        
        if progress is not None:
            update_fields.append("progress = %s")
            params.append(progress)
        
        if current_location is not None:
            update_fields.append("current_location = %s")
            params.append(current_location)
            params.append(datetime.now().isoformat())
        
        params.append(job_id)
        
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE scraping_jobs SET {', '.join(update_fields)} WHERE id = %s",
                tuple(params)
            )
        conn.commit()
        conn.close()
    
    @staticmethod
    def set_results(job_id: int, results_path: str):
        """Set the results file path for a completed job"""
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE scraping_jobs SET results_path = %s WHERE id = %s",
                (results_path, job_id)
            )
        conn.commit()
        conn.close()
    
    def get_by_id(job_id: int) -> dict:
        """Get job by ID"""
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM scraping_jobs WHERE id = %s", (job_id,))
            row = cur.fetchone()
        conn.close()
        return dict(row) if row else None
    
    def get_by_user_id(user_id: int, limit: int = 10) -> list:
        """Get recent jobs for a user"""
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT * FROM scraping_jobs 
                WHERE user_id = %s 
                ORDER BY created_at DESC 
                LIMIT %s
                """,
                (user_id, limit)
            )
            rows = cur.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    @staticmethod
    def get_monthly_lead_count(user_id: int, subscription_date: datetime) -> dict:
        """
        Retorna leads usados no ciclo mensal atual baseado em purchase_date
        AGORA CONSULTA O HISTÓRICO IMUTÁVEL (anti-bypass por deleção de jobs)
        
        Args:
            user_id: ID do usuário
            subscription_date: Data de compra da licença (purchase_date)
        
        Returns:
            {
                'used': int,           # Leads usados no ciclo atual  
                'cycle_start': datetime,
                'cycle_end': datetime
            }
        """
        from datetime import datetime, timedelta
        
        # Calcular quantos meses (ciclos de 30 dias) passaram desde a assinatura
        today = datetime.now()
        days_since_purchase = (today - subscription_date).days
        months_elapsed = days_since_purchase // 30
        
        # Ciclo atual: purchase_date + (30 * months_elapsed) dias
        cycle_start = subscription_date + timedelta(days=30 * months_elapsed)
        cycle_end = cycle_start + timedelta(days=30)
        
        # Se hoje < cycle_start (edge case), estamos no ciclo anterior
        if today < cycle_start:
            months_elapsed -= 1
            cycle_start = subscription_date + timedelta(days=30 * months_elapsed)
            cycle_end = cycle_start + timedelta(days=30)
        
        conn = get_db_connection()
        with conn.cursor() as cur:
            # QUERY IMUTÁVEL: Consulta histórico ao invés de scraping_jobs
            # Isso impede bypass por deleção de jobs
            cur.execute("""
                SELECT COALESCE(SUM(leads_extracted), 0) as total
                FROM monthly_usage_history
                WHERE user_id = %s
                  AND cycle_start = %s
            """, (user_id, cycle_start.date()))
            used = cur.fetchone()[0]
        conn.close()
        
        return {
            'used': used,
            'cycle_start': cycle_start,
            'cycle_end': cycle_end
        }


def run_scraping_job(job_id: int):
    """Run scraping job in background thread"""
    try:
        job = ScrapingJob.get_by_id(job_id)
        if not job:
            return
        
        ScrapingJob.update_status(job_id, 'running', 0)
        
        # Parse job data
        locations = json.loads(job['locations'])
        keyword = job['keyword']
        total_results = job['total_results']
        user_id = job['user_id']
        
        # Create queries for each location
        queries = [f"{keyword} in {loc}" for loc in locations]
        
        # Set up user directory
        user_base_dir = os.path.join(STORAGE_ROOT, str(user_id), "GMaps Data")
        
        # Run scraper with progress tracking
        results = run_scraper_with_progress(
            queries, 
            total=total_results, 
            headless=True, 
            save_base_dir=user_base_dir, 
            concatenate_results=True,
            progress_callback=lambda progress, current_loc: ScrapingJob.update_status(
                job_id, 'running', progress, current_loc
            )
        )
        
        if results and len(results) > 0:
            # Set results path
            results_path = results[0].get('csv_path', '')
            ScrapingJob.set_results(job_id, results_path)
            ScrapingJob.update_status(job_id, 'completed', 100)
        else:
            ScrapingJob.update_status(job_id, 'failed', error_message="No results generated")
            
    except Exception as e:
        ScrapingJob.update_status(job_id, 'failed', error_message=str(e))
        print(f"Scraping job {job_id} failed: {e}")


def run_scraping_job_async(job_id: int):
    """Start scraping job in background thread"""
    thread = threading.Thread(target=run_scraping_job, args=(job_id,))
    thread.daemon = True
    thread.start()


def send_reset_email(email, token):
    """Enfileira o envio de email com link de redefinição de senha"""
    try:
        reset_url = url_for('reset_password', token=token, _external=True)
        
        # HTML Body
        html_body = f"""
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <h2 style="color: #4f7cff;">Redefinição de Senha</h2>
            <p>Olá!</p>
            <p>Você solicitou a redefinição de sua senha no Leads Infinitos.</p>
            <p>Clique no botão abaixo para criar uma nova senha:</p>
            <div style="text-align: center; margin: 30px 0;">
                <a href="{reset_url}" style="background-color: #4f7cff; color: white; padding: 12px 24px; text-decoration: none; border-radius: 4px; font-weight: bold;">Redefinir Minha Senha</a>
            </div>
            <p>Ou copie e cole o link abaixo no seu navegador:</p>
            <p style="background-color: #f5f5f5; padding: 10px; font-family: monospace; word-break: break-all;">{reset_url}</p>
            <p><strong>Importante:</strong> Este link expira em 1 hora.</p>
            <p>Se você não solicitou esta redefinição, ignore este email.</p>
            <p>Atenciosamente,<br>Equipe Leads Infinitos</p>
        </div>
        """
        
        # Enfileirar tarefa no RQ
        from worker_email import send_email_task
        q.enqueue(send_email_task, email, 'Redefinição de Senha - Leads Infinitos', html_body)
        
        return True
    except Exception as e:
        print(f"Erro ao enfileirar email: {e}")
        return False


app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret")

# Timezone BRT para exibição de datas (Postgres armazena UTC)
BRAZIL_TZ = pytz.timezone('America/Sao_Paulo')

@app.template_filter('to_brt')
def to_brt(dt):
    """Converte datetime para BRT. Assume UTC se naive."""
    if dt is None:
        return None
    if hasattr(dt, 'tzinfo') and dt.tzinfo:
        return dt.astimezone(BRAZIL_TZ)
    # Naive = assume UTC (Postgres sem timezone)
    return pytz.UTC.localize(dt).astimezone(BRAZIL_TZ)
STORAGE_ROOT = os.environ.get("STORAGE_DIR", "storage")
_storage_abs = os.path.abspath(STORAGE_ROOT)
os.makedirs(_storage_abs, exist_ok=True)

# Configuração do Flask-Mail
app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 587
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USERNAME'] = os.environ.get('MAIL_USERNAME')
app.config['MAIL_PASSWORD'] = os.environ.get('MAIL_PASSWORD')
app.config['MAIL_DEFAULT_SENDER'] = os.environ.get('MAIL_USERNAME')

mail = Mail(app)

login_manager = LoginManager()
login_manager.login_view = "login"
login_manager.init_app(app)


@app.before_request
def _ensure_csrf_token_for_session():
    """T10: token CSRF para rotas admin JSON (header X-CSRF-Token ou body csrf_token)."""
    if current_user.is_authenticated and "csrf_token" not in session:
        session["csrf_token"] = secrets.token_urlsafe(32)
        session.modified = True


@login_manager.unauthorized_handler
def unauthorized():
    """Return JSON 401 for API/XHR requests instead of redirecting to HTML login."""
    if request.path.startswith('/api/') or request.is_json or request.accept_mimetypes.best == 'application/json':
        return jsonify({"error": "unauthorized", "message": "Sessão expirada. Faça login novamente."}), 401
    return redirect(url_for(login_manager.login_view, next=request.url))


@login_manager.user_loader
def load_user(user_id: str):
    try:
        return User.get_by_id(int(user_id))
    except Exception:
        return None


@app.route("/api/provision/user", methods=["POST"])
def api_provision_user():
    """
    Cria usuário por e-mail (server-to-server). Autenticação: ver PROVISION_API_SECRET.
    Body JSON: email (obrigatório), password (opcional; se omitido ou vazio, gera e devolve uma vez).
    """
    auth_err = _require_provision_secret()
    if auth_err is not None:
        return auth_err

    if not request.is_json:
        return jsonify({"error": "invalid_request", "message": "Content-Type deve ser application/json."}), 400

    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    if not email:
        return jsonify({"error": "invalid_request", "message": "Campo email é obrigatório."}), 400

    if User.get_by_email(email):
        return jsonify({"error": "email_already_registered"}), 409

    raw_password = data.get("password")
    if raw_password is None or (isinstance(raw_password, str) and not raw_password.strip()):
        password_plain = secrets.token_urlsafe(12)
        password_generated = True
    else:
        password_plain = raw_password if isinstance(raw_password, str) else str(raw_password)
        password_generated = False

    user = User.create(email, password_plain)

    body = {"user_id": user.id, "email": user.email}
    if password_generated:
        body["password"] = password_plain
    else:
        body["password_set"] = True
    return jsonify(body), 201


@app.route("/api/provision/license", methods=["POST"])
def api_provision_license():
    """
    Aplica licença a usuário existente por e-mail (server-to-server). Autenticação: ver PROVISION_API_SECRET.
    Body JSON: email (obrigatório), license_type (opcional; default starter_trial).
    Revoga licenças do usuário e insere a nova na mesma transação (um único get_db_connection).
    """
    auth_err = _require_provision_secret()
    if auth_err is not None:
        return auth_err

    if not request.is_json:
        return jsonify({"error": "invalid_request", "message": "Content-Type deve ser application/json."}), 400

    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    if not email:
        return jsonify({"error": "invalid_request", "message": "Campo email é obrigatório."}), 400

    raw_lt = data.get("license_type")
    if raw_lt is None:
        license_type_input = "starter_trial"
    else:
        if not isinstance(raw_lt, str):
            return jsonify({"error": "invalid_request", "message": "Campo license_type deve ser string."}), 400
        license_type_input = raw_lt.strip().lower()
        if not license_type_input:
            license_type_input = "starter_trial"

    license_type = resolve_license_type(license_type_input, allow_legacy_fallback=False)
    if not license_type or license_type not in ACTIVE_LICENSE_TYPES:
        allowed_plans = ", ".join(ACTIVE_LICENSE_TYPES)
        return (
            jsonify(
                {
                    "error": "invalid_license_type",
                    "message": f"Plano inválido: '{license_type_input}'. Use apenas: {allowed_plans}.",
                    "allowed_license_types": list(ACTIVE_LICENSE_TYPES),
                }
            ),
            400,
        )

    user = User.get_by_email(email)
    if not user:
        return jsonify({"error": "user_not_found", "message": "Usuário não encontrado para este email."}), 404

    purchase_id = f"MANUAL-{secrets.token_hex(8)}"
    product_id = "MANUAL-GRANT"
    purchase_date = datetime.utcnow().isoformat()
    purchase_dt = datetime.fromisoformat(purchase_date.replace("Z", "+00:00"))
    policy = get_plan_policy(license_type)
    validity_days = int(policy.get("validity_days", 365))
    expires_at = purchase_dt + timedelta(days=validity_days)

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE licenses SET status = 'cancelled' WHERE user_id = %s",
                (user.id,),
            )
            cur.execute(
                """
                INSERT INTO licenses
                (user_id, hotmart_purchase_id, hotmart_product_id, license_type, purchase_date, expires_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    user.id,
                    purchase_id,
                    product_id,
                    license_type,
                    purchase_date,
                    expires_at.isoformat(),
                ),
            )
            row = cur.fetchone()
            new_id = row[0]
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Erro ao provisionar licença: {e}")
        return jsonify({"error": "server_error", "message": "Erro ao criar licença."}), 500
    finally:
        conn.close()

    return (
        jsonify(
            {
                "license_id": new_id,
                "user_id": user.id,
                "email": user.email,
                "license_type": license_type,
                "status": "active",
            }
        ),
        201,
    )


# Inicializa o banco na carga da aplicação (Flask 3 removeu before_first_request)
# init_db()


@app.route("/", methods=["GET"]) 
@login_required
def index():
    return render_template("index.html")


@app.route("/register", methods=["GET", "POST"]) 
def register():
    if current_user.is_authenticated:
        return redirect(url_for("index"))
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        if not email or not password:
            flash("Preencha email e senha.")
            return redirect(url_for("register"))
        if User.get_by_email(email):
            flash("Email já registrado.")
            return redirect(url_for("register"))
        
        # Verificar se o email tem uma compra válida na Hotmart
        try:
            hotmart_service = HotmartService()
            purchase_data = hotmart_service.verify_purchase(email)
            
            if not purchase_data:
                flash("Email não encontrado em nossas vendas. Verifique se você comprou o produto Leads Infinitos na Hotmart.")
                return redirect(url_for("register"))
            
            # Criar usuário
            user = User.create(email, password)
            
            # Criar licença baseada na compra (somente planos ativos).
            price = float(purchase_data.get('price', 0))
            license_type = license_type_from_price(price)
            
            License.create(
                user.id, 
                purchase_data['purchase_id'], 
                purchase_data['product_id'], 
                license_type, 
                purchase_data['purchase_date']
            )
            
            login_user(user)
            session["csrf_token"] = secrets.token_urlsafe(32)
            session.modified = True
            flash(f"Conta criada com sucesso! Sua licença {license_type} está ativa.")
            return redirect(url_for("index"))
            
        except Exception as e:
            flash(f"Erro ao verificar compra: {str(e)}. Entre em contato com o suporte.")
            return redirect(url_for("register"))
    
    return render_template("register.html")


@app.route("/api/login", methods=["POST"])
def api_login():
    """
    Login via JSON para automação (n8n, curl, etc).
    Body: {"email": "...", "password": "..."}
    Retorna 200 + Set-Cookie (session) em sucesso.
    """
    if request.is_json:
        data = request.get_json() or {}
        email = (data.get("email") or "").strip().lower()
        password = data.get("password") or ""
        user = User.get_by_email(email)
        if not user or not check_password_hash(user.password_hash, password):
            return json.dumps({"error": "Credenciais inválidas"}), 401
        login_user(user)
        session["csrf_token"] = secrets.token_urlsafe(32)
        session.modified = True
        return json.dumps({"ok": True, "user_id": user.id, "csrf_token": session["csrf_token"]}), 200
    return json.dumps({"error": "Content-Type deve ser application/json"}), 400


@app.route("/login", methods=["GET", "POST"]) 
def login():
    if current_user.is_authenticated:
        return redirect(url_for("index"))
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        user = User.get_by_email(email)
        if not user or not check_password_hash(user.password_hash, password):
            flash("Credenciais inválidas.")
            return redirect(url_for("login"))
        login_user(user)
        session["csrf_token"] = secrets.token_urlsafe(32)
        session.modified = True
        return redirect(url_for("index"))
    return render_template("login.html")


@app.route("/logout") 
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))


@app.route("/healthz", methods=["GET"]) 
def healthz():
    return "ok", 200


@app.route("/metrics", methods=["GET"])
def prometheus_metrics():
    """Prometheus (Task 14); desligado por omissão — ver ``EXPOSE_PROMETHEUS_METRICS``."""
    if not EXPOSE_PROMETHEUS_METRICS:
        abort(404)
    from utils.outbox_prometheus import flask_metrics_response

    return flask_metrics_response()


@app.route("/scrape", methods=["POST"]) 
@login_required
def scrape():
    # Verificar se usuário tem licença ativa
    if not current_user.has_active_license():
        flash("Sua licença expirou ou não está ativa. Entre em contato com o suporte para renovar.")
        return redirect(url_for("index"))
    
    palavra_chave = request.form.get("palavra_chave", "").strip()
    localizacoes = request.form.getlist("localizacoes[]")  # Lista de localizações
    total_raw = request.form.get("total", "").strip() or "100"
    
    try:
        total = int(total_raw)
    except Exception:
        total = 100
    
    # Guardrails: clamp total and inputs
    total = max(1, min(total, 500))
    if len(palavra_chave) > 100:
        palavra_chave = palavra_chave[:100]
    
    # Validar entrada
    if not palavra_chave:
        flash("Por favor, preencha 'Palavra-chave'.")
        return redirect(url_for("index"))
    
    if not localizacoes or not any(loc.strip() for loc in localizacoes):
        flash("Por favor, adicione pelo menos uma localização.")
        return redirect(url_for("index"))
    
    # Limitar a 15 localizações
    # APPEND ", Brasil" to force country context (User Request)
    cleaned_locs = []
    for loc in localizacoes:
        l = loc.strip()
        if not l: continue
        # Avoid double suffix if user already typed it
        if not l.lower().endswith('brasil') and not l.lower().endswith('brazil'):
             l = f"{l}, Brasil"
        cleaned_locs.append(l)
    
    localizacoes = cleaned_locs[:15]
    
    # VALIDAÇÃO: Limite mensal por plano (Starter/Pro/Scale/Infinite)
    # Buscar licença ativa do usuário
    licenses = License.get_by_user_id(current_user.id)
    active_license = next((l for l in licenses if l.status == 'active'), None)
    
    if not active_license:
        flash("Você não possui uma licença ativa. Por favor, adquira uma licença para continuar.", "error")
        return redirect(url_for("index"))
    
    # Converter purchase_date para datetime (pode já ser datetime do banco)
    from datetime import datetime
    subscription_date = active_license.purchase_date
    
    # Se for string, converter
    if isinstance(subscription_date, str):
        subscription_date = datetime.fromisoformat(subscription_date.replace('Z', '+00:00'))
    
    # Remover timezone info se presente
    if hasattr(subscription_date, 'tzinfo') and subscription_date.tzinfo is not None:
        subscription_date = subscription_date.replace(tzinfo=None)
    
    # Calcular uso mensal
    cycle_info = ScrapingJob.get_monthly_lead_count(current_user.id, subscription_date)
    
    active_plan_type = resolve_license_type(active_license.license_type, allow_legacy_fallback=False) or "starter"
    plan_policy = get_plan_policy(active_plan_type, allow_legacy_fallback=False)
    monthly_limit = int(plan_policy["monthly_extraction_limit"])
    requested_leads = total
    
    if cycle_info['used'] + requested_leads > monthly_limit:
        available = max(monthly_limit - cycle_info['used'], 0)
        renewal_date = cycle_info['cycle_end'].date().isoformat()
        plan_label = active_plan_type.upper()
        
        flash(
            f"Limite mensal de extrações do plano {plan_label} atingido. "
            f"Você já usou {cycle_info['used']} de {monthly_limit} leads neste ciclo. "
            f"Disponível: {available} leads. Renovação em {renewal_date}.",
            "error"
        )
        return redirect(url_for("index"))
    
    # Create background job
    job_id = ScrapingJob.create(
        user_id=current_user.id,
        keyword=palavra_chave,
        locations=localizacoes,
        total_results=total
    )
    
    # Validar API Token antes de enfileirar
    if not os.environ.get('APIFY_TOKEN'):
        flash("Erro de Configuração: APIFY_TOKEN não encontrado. Contate o suporte ou configure no arquivo .env/Dokploy.", "error")
        # Marcar job como falho imediatamente para não ficar 'pending' para sempre
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("UPDATE scraping_jobs SET status = 'failed', error_message = %s WHERE id = %s", 
                        ('APIFY_TOKEN não configurado', job_id))
        conn.commit()
        conn.close()
        return redirect(url_for("jobs"))

    # Start job in background (Queue)
    try:
        from worker_scraper import run_scraper_task
        q.enqueue(run_scraper_task, job_id, job_timeout=3600)
        flash(f"Scraping enfileirado! Job ID: {job_id}. Você pode acompanhar o progresso na página de jobs.")
    except Exception as e:
        print(f"❌ Erro ao enfileirar job no Redis: {e}")
        flash(f"Erro ao iniciar o job: {str(e)}", "error")
        # Tentar marcar como falho no banco
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute("UPDATE scraping_jobs SET status = 'failed', error_message = %s WHERE id = %s", 
                            (f'Erro de Fila: {str(e)}', job_id))
            conn.commit()
            conn.close()
        except:
            pass

    return redirect(url_for("jobs"))


def _is_path_owned_by_current_user(path: str) -> bool:
    if not current_user.is_authenticated:
        return False
    user_root = os.path.abspath(os.path.join(STORAGE_ROOT, str(current_user.id)))
    abs_path = os.path.abspath(path)
    try:
        return os.path.commonpath([abs_path, user_root]) == user_root
    except Exception:
        return False


@app.route("/download") 
@login_required
def download():
    path = request.args.get("path")
    if not path or not os.path.exists(path):
        flash("Arquivo não encontrado para download.")
        return redirect(url_for("index"))
    if not _is_path_owned_by_current_user(path):
        flash("Acesso negado ao arquivo solicitado.")
        return redirect(url_for("index"))
    filename = os.path.basename(path)
    return send_file(path, as_attachment=True, download_name=filename)




@app.route("/webhook/hubla", methods=["POST"])
def hubla_webhook():
    """Endpoint para receber webhooks da Hubla"""
    try:
        payload = request.get_json()
        # Hubla envia o token no header 'x-hubla-token' na UI. Mantemos compatibilidade com 'Authorization'.
        signature = (
            request.headers.get('Authorization')
            or request.headers.get('x-hubla-token')
            or request.headers.get('X-Hubla-Token')
            or ''
        )
        
        hubla_service = HublaService()
        success = hubla_service.process_webhook(payload, signature)
        
        if success:
            return {"status": "success"}, 200
        else:
            return {"status": "error", "message": "Failed to process webhook"}, 400
            
    except Exception as e:
        print(f"Erro no webhook da Hubla: {e}")
        return {"status": "error", "message": str(e)}, 500


@app.route("/cron/expire-starter-trial", methods=["GET", "POST"])
def cron_expire_starter_trial():
    """
    Expira licenças starter_trial vencidas e deleta instâncias Uazapi.
    Protegido por token: ?token=<CRON_SECRET>
    Uso: cron diário (ex: 0 2 * * * curl -s "https://app.../cron/expire-starter-trial?token=...")
    """
    token = request.args.get("token") or (request.get_json(silent=True) or {}).get("token")
    expected = os.environ.get("CRON_SECRET", "")
    if not expected or token != expected:
        return jsonify({"error": "unauthorized"}), 401
    try:
        from utils.expire_starter_trial import expire_starter_trial_licenses
        count = expire_starter_trial_licenses()
        return jsonify({"ok": True, "processed": count})
    except Exception as e:
        print(f"❌ cron_expire_starter_trial: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/licenses")
@login_required
def licenses():
    """Página para visualizar licenças do usuário"""
    user_licenses = License.get_by_user_id(current_user.id)
    return render_template("licenses.html", licenses=user_licenses)


@app.route("/api/verify-license")
@login_required
def verify_license():
    """API para verificar status da licença (usado por JavaScript)"""
    has_license = current_user.has_active_license()
    return {"has_active_license": has_license}


def send_async_email(app_instance, msg):
    with app_instance.app_context():
        try:
            mail.send(msg)
            print(f"📧 Email enviado para {msg.recipients} via SMTP")
        except Exception as e:
            print(f"❌ Erro ao enviar email: {e}")

def send_reset_email(to_email, token):
    """Envia email de redefinição de senha"""
    reset_url = url_for('reset_password', token=token, _external=True)
    msg = Message("Redefinição de Senha - Leads Infinitos", recipients=[to_email])
    msg.body = f"""Olá!

Recebemos uma solicitação para redefinir sua senha.
Clique no link abaixo para criar uma nova senha:

{reset_url}

Se você não solicitou isso, ignore este email. O link expira em 1 hora.

Atenciosamente,
Equipe Leads Infinitos"""
    
    # Enviar em thread separada para não bloquear
    threading.Thread(target=send_async_email, args=(app, msg)).start()

def send_welcome_email(to_email, password):
    """Envia email de boas-vindas com credenciais"""
    login_url = url_for('login', _external=True)
    msg = Message("Bem-vindo ao Leads Infinitos!", recipients=[to_email])
    msg.body = f"""Olá!

Sua conta foi criada com sucesso após a confirmação do pagamento.
Aqui estão suas credenciais de acesso:

Email: {to_email}
Senha: {password}

Acesse em: {login_url}

Recomendamos que altere sua senha após o primeiro login.

Atenciosamente,
Equipe Leads Infinitos"""

    # Enviar em thread separada
    threading.Thread(target=send_async_email, args=(app, msg)).start()



@app.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    """Página para solicitar reset de senha"""
    if current_user.is_authenticated:
        return redirect(url_for("index"))
    
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        
        if not email:
            flash("Por favor, informe seu email.")
            return redirect(url_for("forgot_password"))
        
        # Verificar se o usuário existe
        user = User.get_by_email(email)
        
        if user:
            # Gerar token único
            token = secrets.token_urlsafe(32)
            expires_at = datetime.now() + timedelta(hours=1)
            
            # Salvar token no banco
            conn = get_db_connection()
            with conn.cursor() as cur:
                # Invalidar tokens anteriores
                cur.execute("UPDATE password_resets SET used = TRUE WHERE user_id = %s", (user.id,))
                # Criar novo token
                cur.execute(
                    """
                    INSERT INTO password_resets (user_id, token, expires_at)
                    VALUES (%s, %s, %s)
                    """,
                    (user.id, token, expires_at)
                )
            conn.commit()
            conn.close()
            
            # Enviar email (agora assíncrono)
            send_reset_email(email, token)
            
        # SEMPRE retornar mensagem de sucesso para evitar enumeração de usuários
        flash("Se o email informado estiver cadastrado, você receberá um link para redefinir sua senha em instantes.")
        return redirect(url_for("login"))
    
    return render_template("forgot_password.html")


@app.route("/reset-password/<token>", methods=["GET", "POST"])
def reset_password(token):
    """Página para definir nova senha usando token"""
    if current_user.is_authenticated:
        return redirect(url_for("index"))
    
    # Verificar token
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT user_id, expires_at, used 
            FROM password_resets 
            WHERE token = %s
            """, 
            (token,)
        )
        reset_data = cur.fetchone()
    conn.close()
    
    if not reset_data:
        flash("Link inválido ou inexistente.")
        return redirect(url_for("login"))
    
    user_id, expires_at, used = reset_data
    
    if used:
        flash("Este link já foi utilizado.")
        return redirect(url_for("login"))
        
    if datetime.now() > expires_at:
        flash("Este link expirou. Solicite um novo.")
        return redirect(url_for("forgot_password"))
    
    if request.method == "POST":
        password = request.form.get("password", "")
        confirm_password = request.form.get("confirm_password", "")
        
        if not password or not confirm_password:
            flash("Preencha todos os campos.")
            return render_template("reset_password.html", token=token)
            
        if password != confirm_password:
            flash("As senhas não coincidem.")
            return render_template("reset_password.html", token=token)
            
        if len(password) < 6:
            flash("A senha deve ter pelo menos 6 caracteres.")
            return render_template("reset_password.html", token=token)
        
        # Atualizar senha e invalidar token
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET password_hash = %s WHERE id = %s",
                (generate_password_hash(password), user_id)
            )
            cur.execute(
                "UPDATE password_resets SET used = TRUE WHERE token = %s",
                (token,)
            )
        conn.commit()
        conn.close()
        
        flash("Senha alterada com sucesso! Faça login com sua nova senha.")
        return redirect(url_for("login"))
        
    return render_template("reset_password.html", token=token)


@app.route("/change-password", methods=["GET", "POST"])
@login_required
def change_password():
    """Página para alterar senha"""
    if request.method == "POST":
        current_password = request.form.get("current_password", "")
        new_password = request.form.get("new_password", "")
        confirm_password = request.form.get("confirm_password", "")
        
        if not all([current_password, new_password, confirm_password]):
            flash("Por favor, preencha todos os campos.")
            return redirect(url_for("change_password"))
        
        if new_password != confirm_password:
            flash("As senhas não coincidem.")
            return redirect(url_for("change_password"))
        
        if len(new_password) < 6:
            flash("A nova senha deve ter pelo menos 6 caracteres.")
            return redirect(url_for("change_password"))
        
        # Verificar senha atual
        if not check_password_hash(current_user.password_hash, current_password):
            flash("Senha atual incorreta.")
            return redirect(url_for("change_password"))
        
        # Atualizar senha
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET password_hash = %s WHERE id = %s",
                (generate_password_hash(new_password), current_user.id)
            )
        conn.commit()
        conn.close()
        
        flash("Senha alterada com sucesso!")
        return redirect(url_for("index"))
    
    return render_template("change_password.html")


@app.route('/account')
@login_required
def account():
    # 1. Obter Licenças
    licenses = License.get_by_user_id(current_user.id)
    active_license = None
    for lic in licenses:
        if lic.status == 'active':
            active_license = lic
            break

    # 2. Obter Instância(s) WhatsApp
    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if is_super_admin():
            # Superadmin: todas as instâncias Uazapi com status real
            cur.execute(
                "SELECT * FROM instances WHERE user_id = %s AND api_provider = 'uazapi'",
                (current_user.id,),
            )
            instances = cur.fetchall()
        else:
            cur.execute("SELECT * FROM instances WHERE user_id = %s", (current_user.id,))
            instances = cur.fetchall()
    conn.close()

    instance = None if is_super_admin() else (instances[0] if instances else None)
    instances_with_status = None

    if is_super_admin() and instances:
        uazapi = UazapiService()
        instances_with_status = []
        for inst in instances:
            apikey = inst.get("apikey") or ""
            if not apikey:
                instances_with_status.append(
                    {"id": inst["id"], "name": inst.get("name", "?"), "status": "Desconectado"}
                )
                continue
            result = uazapi.get_status(apikey)
            raw_status = "disconnected"
            if result:
                raw_status = (
                    result.get("instance", {}).get("status")
                    or result.get("status")
                    or "disconnected"
                )
            status_label = {
                "connected": "Conectado",
                "connecting": "Conectando",
                "disconnected": "Desconectado",
            }.get(raw_status, "Desconectado")
            instances_with_status.append(
                {"id": inst["id"], "name": inst.get("name", "?"), "status": status_label}
            )

    return render_template(
        "account.html",
        user=current_user,
        license=active_license,
        instance=instance,
        instances=instances,
        instances_with_status=instances_with_status,
        is_super_admin=is_super_admin(),
    )


@app.route('/api/account/instances/<int:instance_id>/daily-limit', methods=['POST'])
@login_required
def update_instance_daily_limit(instance_id):
    """Atualiza limite diário por instância para usuários com plano Infinite."""
    payload = request.get_json(silent=True) or {}
    if not payload:
        payload = request.form

    try:
        daily_value = int(payload.get('daily_sends_per_instance'))
    except (TypeError, ValueError):
        return json.dumps({"error": "Valor inválido para daily_sends_per_instance."}), 400

    if daily_value not in INFINITE_DAILY_SEND_OPTIONS:
        return json.dumps({"error": "Valor permitido: 10, 20, 30, 40 ou 50."}), 400

    licenses = License.get_by_user_id(current_user.id)
    active_license = next((lic for lic in licenses if lic.status == 'active'), None)
    if not active_license or active_license.license_type != 'infinite':
        return json.dumps({"error": "Configuração disponível apenas para plano Infinite."}), 403

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                UPDATE instances
                SET daily_sends_per_instance = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s AND user_id = %s
                RETURNING id
                """,
                (daily_value, instance_id, current_user.id),
            )
            updated = cur.fetchone()

        if not updated:
            conn.rollback()
            return json.dumps({"error": "Instância não encontrada."}), 404

        conn.commit()
        return json.dumps(
            {
                "success": True,
                "instance_id": instance_id,
                "daily_sends_per_instance": daily_value,
            }
        )
    except Exception as e:
        conn.rollback()
        print(f"Erro ao atualizar daily_sends_per_instance: {e}")
        return json.dumps({"error": "Falha ao salvar configuração da instância."}), 500
    finally:
        conn.close()


@app.route("/api/account/instances", methods=["GET"])
@login_required
def api_account_instances():
    """Lista instâncias do usuário (automação / scripts). Sem apikey."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, name, status, COALESCE(api_provider, 'megaapi') AS api_provider
                FROM instances
                WHERE user_id = %s
                ORDER BY id ASC
                """,
                (current_user.id,),
            )
            rows = cur.fetchall() or []
    finally:
        conn.close()
    return json.dumps([dict(r) for r in rows])


@app.route('/campaigns')
@login_required
def campaigns_list():
    user_campaigns = Campaign.get_by_user(current_user.id)
    conn = get_db_connection()
    try:
        reconnect_alerts = fetch_reconnect_inapp_alerts_for_user(conn, current_user.id)
    finally:
        conn.close()
    return render_template(
        'campaigns_list.html',
        campaigns=user_campaigns,
        reconnect_alerts=reconnect_alerts,
    )


@app.route("/api/reconnect-alerts/<int:alert_id>/dismiss", methods=["POST"])
@login_required
def dismiss_reconnect_alert(alert_id):
    """Remove um banner de reconexão in-app (utilizador só pode dispensar os seus)."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM reconnect_inapp_alerts WHERE id = %s AND user_id = %s",
                (alert_id, current_user.id),
            )
            deleted = cur.rowcount or 0
        if deleted == 0:
            conn.rollback()
            return json.dumps({"success": False, "error": "not_found"}), 404
        conn.commit()
        return json.dumps({"success": True})
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return json.dumps({"success": False, "error": str(e)}), 500
    finally:
        conn.close()


@app.route('/campaigns/delete/<int:campaign_id>', methods=['POST'])
@login_required
def delete_campaign(campaign_id):
    campaign = Campaign.get_by_id(campaign_id, current_user.id)
    if not campaign:
        flash("Campanha não encontrada.", "error")
        return redirect(url_for('campaigns_list'))

    if campaign.use_uazapi_sender and campaign.uazapi_folder_id:
        success, err = _uazapi_control_campaign(campaign_id, current_user.id, 'delete')
        if success:
            flash("Campanha excluída com sucesso!", "success")
        else:
            flash(f"Erro ao excluir campanha: {err}", "error")
    elif campaign.delete():
        flash("Campanha excluída com sucesso!", "success")
    else:
        flash("Erro ao excluir campanha.", "error")

    return redirect(url_for('campaigns_list'))

# --- Kanban Board Routes ---

@app.route('/campaigns/<int:campaign_id>/kanban')
@login_required
def campaign_kanban(campaign_id):
    """Render the Kanban board for a campaign"""
    campaign = Campaign.get_by_id(campaign_id, current_user.id)
    if not campaign:
        flash("Campanha não encontrada.", "error")
        return redirect(url_for('campaigns_list'))
    
    # Get total lead count
    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT COUNT(*) AS total FROM campaign_leads WHERE campaign_id = %s", (campaign_id,))
        total_leads = int((cur.fetchone() or {}).get("total") or 0)
        cur.execute(
            """
            SELECT i.id, i.name, i.status, COALESCE(i.api_provider, 'megaapi') AS api_provider
            FROM campaign_instances ci
            JOIN instances i ON i.id = ci.instance_id
            WHERE ci.campaign_id = %s
            ORDER BY i.id ASC
            """,
            (campaign_id,),
        )
        campaign_instances = cur.fetchall() or []
        step_generate_templates = {2: [], 3: [], 4: []}
        cur.execute(
            """
            SELECT step_number, message_template FROM campaign_steps
            WHERE campaign_id = %s AND step_number IN (2, 3, 4)
            """,
            (campaign_id,),
        )
        for r in cur.fetchall() or []:
            sn = int(r.get("step_number") or 0)
            if sn not in step_generate_templates:
                continue
            try:
                p = json.loads(r.get("message_template") or "[]")
                if isinstance(p, list):
                    step_generate_templates[sn] = [str(x).strip() for x in p if str(x).strip()]
                elif isinstance(p, str) and p.strip():
                    step_generate_templates[sn] = [p.strip()]
            except Exception:
                pass
    conn.close()

    try:
        base_var = json.loads(getattr(campaign, "message_template", None) or "[]")
        if isinstance(base_var, str):
            base_list = [base_var.strip()] if base_var.strip() else []
        elif isinstance(base_var, list):
            base_list = [str(x).strip() for x in base_var if str(x).strip()]
        else:
            base_list = []
    except Exception:
        base_list = []
    for sn in (2, 3, 4):
        if not step_generate_templates[sn] and base_list:
            step_generate_templates[sn] = list(base_list)

    return render_template(
        'campaigns_kanban.html',
        campaign=campaign,
        total_leads=total_leads,
        campaign_instances=campaign_instances,
        step_generate_templates=step_generate_templates,
    )


@app.route('/api/campaigns/<int:campaign_id>/kanban-data')
@login_required
def campaign_kanban_data(campaign_id):
    """API: leads do Kanban a partir de ``campaign_leads`` (SSOT por lead).

    Para UAZAPI pode correr ``sync_campaign_leads_from_uazapi`` antes da leitura;
    o JSON ``leads[]`` reflete colunas da BD (ex.: ``status``, ``current_step``,
    ``last_sent_stage``) e ``ui_sent_in_column_stage`` (envio confirmado para a etapa
    da coluna atual, via ``last_sent_stage`` e/ou ``campaign_message_outbox`` com
    ``status='sent'`` por stage). Não há recálculo por ``listfolders`` neste handler.

    ``uazapi_stats`` e ``stage_progress`` são agregados de ``campaign_stage_sends``
    (contagens / estado do envio) para resumo operacional; não substituem o estado
    por destinatário em ``leads[]`` (tech-spec Task 9 / F12).
    """
    campaign = Campaign.get_by_id(campaign_id, current_user.id)
    if not campaign:
        return json.dumps({'error': 'Campanha não encontrada'}), 404

    stats = None
    if getattr(campaign, 'use_uazapi_sender', False):
        conn_sync = get_db_connection()
        try:
            with conn_sync.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT i.apikey FROM campaign_instances ci
                    JOIN instances i ON i.id = ci.instance_id
                    WHERE ci.campaign_id = %s AND COALESCE(i.api_provider, 'megaapi') = 'uazapi'
                    LIMIT 1
                """, (campaign_id,))
                inst = cur.fetchone()
            if inst and inst.get('apikey'):
                from utils.sync_uazapi import sync_campaign_leads_from_uazapi
                uazapi = UazapiService()
                with conn_sync.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        """SELECT 1 FROM campaign_stage_sends
                           WHERE campaign_id = %s AND status IN ('scheduled', 'waiting_reconnect', 'running', 'partial')
                           LIMIT 1""",
                        (campaign_id,),
                    )
                    has_active_chunks = cur.fetchone() is not None
                if has_active_chunks:
                    should_sync = True
                    with conn_sync.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute(
                            """SELECT MAX(last_sync_at) AS last_sync_at
                               FROM campaign_stage_sends WHERE campaign_id = %s""",
                            (campaign_id,),
                        )
                        sync_row = cur.fetchone() or {}
                    last_sync_at = sync_row.get("last_sync_at")
                    if last_sync_at:
                        now_utc = datetime.utcnow()
                        should_sync = (now_utc - last_sync_at).total_seconds() >= (UAZAPI_SYNC_WEB_INTERVAL_MINUTES * 60)
                    if should_sync:
                        sync_campaign_leads_from_uazapi(conn_sync, campaign_id, inst['apikey'], campaign.uazapi_folder_id, uazapi)
                    # Agregados da etapa inicial (não aplicados sobre ``leads[]``; só ``uazapi_stats``).
                    # Usar campaign_stage_sends (initial) para stats — cobre multi-instância e chunks fragmentados.
                    with conn_sync.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute(
                            """SELECT COALESCE(SUM(success_count), 0)::int AS sent,
                                      COALESCE(SUM(failed_count), 0)::int AS failed,
                                      COALESCE(SUM(GREATEST(0, planned_count - success_count - failed_count)), 0)::int AS scheduled
                               FROM campaign_stage_sends WHERE campaign_id = %s AND stage = 'initial'""",
                            (campaign_id,),
                        )
                        row = cur.fetchone() or {}
                    stats = {
                        "sent": int(row.get("sent", 0)),
                        "failed": int(row.get("failed", 0)),
                        "scheduled": int(row.get("scheduled", 0)),
                        "initial_campaign_finished": int(row.get("scheduled", 0)) == 0,
                    }
                elif campaign.uazapi_folder_id:
                    with conn_sync.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute(
                            """SELECT COALESCE(SUM(success_count), 0) AS sent, COALESCE(SUM(failed_count), 0) AS failed
                               FROM campaign_stage_sends WHERE campaign_id = %s AND stage = 'initial'""",
                            (campaign_id,),
                        )
                        row = cur.fetchone() or {}
                    stats = {
                        "sent": int(row.get("sent", 0)),
                        "failed": int(row.get("failed", 0)),
                        "scheduled": 0,
                        "initial_campaign_finished": True,
                    }
        finally:
            conn_sync.close()

    conn = get_db_connection()
    outbox_stages_by_lead = {}
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT id, phone, name, status, current_step, cadence_status,
                   snooze_until, last_message_sent_at, chatwoot_conversation_id,
                   sent_at, last_sent_stage, whatsapp_link, notes, log,
                   address, website, category, location, reviews_count, reviews_rating, latitude, longitude,
                   CASE 
                       WHEN cadence_status IN ('snoozed', 'active') THEN 1
                       WHEN status IN ('sent', 'pending') THEN 2
                       ELSE 3
                       END as status_priority
            FROM campaign_leads 
            WHERE campaign_id = %s 
            ORDER BY current_step ASC, status_priority ASC, COALESCE(csv_row_order, id) ASC, id ASC
        """, (campaign_id,))
        leads = cur.fetchall()
        cur.execute(
            """
            SELECT campaign_lead_id, lower(trim(stage)) AS stage
            FROM campaign_message_outbox
            WHERE campaign_id = %s AND status = 'sent'
            """,
            (campaign_id,),
        )
        for ob in cur.fetchall() or []:
            lid = ob.get("campaign_lead_id")
            st = ob.get("stage")
            if lid is None or not st:
                continue
            outbox_stages_by_lead.setdefault(lid, set()).add(st)
    conn.close()
    
    # Serialize datetime objects
    serialized = []
    for lead in leads:
        row = dict(lead)
        for key in ['snooze_until', 'last_message_sent_at', 'sent_at']:
            if row.get(key):
                row[key] = row[key].isoformat()
        lid = row.get("id")
        stages = outbox_stages_by_lead.get(lid, set())
        row["ui_sent_in_column_stage"] = compute_ui_sent_in_column_stage(row, outbox_sent_stages=stages)
        serialized.append(row)

    out = {'leads': serialized, 'campaign_id': campaign_id}
    if stats:
        out['uazapi_stats'] = stats
    conn_stage = get_db_connection()
    try:
        out["stage_progress"] = _get_campaign_stage_progress(conn_stage, campaign_id)
    finally:
        conn_stage.close()
    out["stage_unlocks"] = {
        "2": _is_previous_stage_fully_done(campaign_id, 2),
        "3": _is_previous_stage_fully_done(campaign_id, 3),
        "4": _is_previous_stage_fully_done(campaign_id, 4),
    }
    return json.dumps(out)


def _fetch_remanent_lead_rows(campaign_id: int, scope: str) -> list:
    """
    Leads exportáveis por escopo:
    - funnel: funil ativo (exceto convertido/perdido/respondeu)
    - breakup: current_step=4
    - pending_initial: ainda sem 1º envio (status pending, passo 1)
    """
    scope = (scope or "funnel").strip().lower()
    if scope not in ("funnel", "breakup", "pending_initial"):
        scope = "funnel"

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            base = """
                SELECT id, phone, name, status, current_step, cadence_status,
                       last_sent_stage, last_message_sent_at, whatsapp_link, notes
                FROM campaign_leads
                WHERE campaign_id = %s
                  AND COALESCE(removed_from_funnel, FALSE) = FALSE
                  AND COALESCE(cadence_status, 'active') NOT IN ('converted', 'lost', 'replied')
            """
            if scope == "breakup":
                cur.execute(
                    base + " AND current_step = 4 ORDER BY COALESCE(csv_row_order, id), id",
                    (campaign_id,),
                )
            elif scope == "pending_initial":
                cur.execute(
                    base
                    + " AND status = 'pending' AND current_step = 1 ORDER BY COALESCE(csv_row_order, id), id",
                    (campaign_id,),
                )
            else:
                cur.execute(
                    base + " ORDER BY current_step ASC, COALESCE(csv_row_order, id), id",
                    (campaign_id,),
                )
            return cur.fetchall() or []
    finally:
        conn.close()


def _remanent_csv_response(rows: list, campaign_name: str, scope: str) -> Response:
    def _cell(v):
        if v is None:
            return ""
        if hasattr(v, "isoformat"):
            return v.isoformat()
        return str(v)

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(
        [
            "id",
            "phone",
            "name",
            "status",
            "current_step",
            "cadence_status",
            "last_sent_stage",
            "last_message_sent_at",
            "whatsapp_link",
            "notes",
        ]
    )
    for r in rows:
        w.writerow(
            [
                r.get("id"),
                _cell(r.get("phone")),
                _cell(r.get("name")),
                _cell(r.get("status")),
                r.get("current_step"),
                _cell(r.get("cadence_status")),
                _cell(r.get("last_sent_stage")),
                _cell(r.get("last_message_sent_at")),
                _cell(r.get("whatsapp_link")),
                _cell(r.get("notes")),
            ]
        )

    safe_name = re.sub(r"[^\w\-]+", "_", (campaign_name or "campanha"))[:80]
    fname = f"{safe_name}_remanescentes_{scope}.csv"
    return Response(
        "\ufeff" + buf.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@app.route("/api/campaigns/<int:campaign_id>/export-remanent-csv")
@login_required
def export_remanent_csv(campaign_id):
    """
    CSV de leads remanescentes no funil (exclui convertido/perdido/respondeu).
    scope=funnel (default): todos os passos ativos.
    scope=breakup: apenas current_step=4 (coluna Break-up).
    scope=pending_initial: status pending e passo 1 (ainda sem 1º disparo).
    """
    scope = (request.args.get("scope") or "funnel").strip().lower()
    campaign = Campaign.get_by_id(campaign_id, current_user.id)
    if not campaign:
        abort(404)

    rows = _fetch_remanent_lead_rows(campaign_id, scope)
    return _remanent_csv_response(rows, campaign.name, scope)


@app.route("/api/admin/campaigns/<int:campaign_id>/export-remanent-csv")
@login_required
@admin_required
def admin_export_remanent_csv(campaign_id):
    """Superadmin: mesmo CSV de remanescentes, sem exigir ser dono da campanha."""
    scope = (request.args.get("scope") or "funnel").strip().lower()
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT name FROM campaigns WHERE id = %s", (campaign_id,))
            row = cur.fetchone()
        if not row:
            abort(404)
        name = row.get("name") or "campanha"
    finally:
        conn.close()
    rows = _fetch_remanent_lead_rows(campaign_id, scope)
    return _remanent_csv_response(rows, name, scope)


@app.route("/api/admin/campaigns/<int:campaign_id>/export-restore-snapshot", methods=["GET"])
@login_required
@admin_required
def admin_export_restore_snapshot(campaign_id):
    """
    JSON para recriar campanha depois: mensagens iniciais, steps de cadência,
    instâncias, horários, delays e create_campaign_payload (falta só job_id do CSV).
    """
    from utils.campaign_restore_snapshot import build_campaign_restore_snapshot, slug_for_filename

    conn = get_db_connection()
    try:
        snapshot = build_campaign_restore_snapshot(conn, campaign_id)
    except ValueError as e:
        return jsonify({"error": "not_found", "message": str(e)}), 404
    finally:
        conn.close()

    cname = (snapshot.get("campaign") or {}).get("name") or "campanha"
    slug = slug_for_filename(cname)
    fname = f"campaign_{campaign_id}_{slug}_restore_snapshot.json"
    body = json.dumps(snapshot, ensure_ascii=False, indent=2)
    return Response(
        body,
        mimetype="application/json; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


_DEFAULT_BACKUP_CAMPAIGN_STATUSES = ("running", "pending", "paused")


def _parse_backup_campaign_statuses(raw) -> tuple:
    """Lista de status de campanha para backup/purge (query ``statuses``, vírgula)."""
    if raw is None or str(raw).strip() == "":
        return _DEFAULT_BACKUP_CAMPAIGN_STATUSES
    parts = [p.strip().lower() for p in str(raw).split(",") if p.strip()]
    return tuple(parts) if parts else _DEFAULT_BACKUP_CAMPAIGN_STATUSES


def _slug_for_export_filename(name: str, max_len: int = 60) -> str:
    s = re.sub(r"[^\w\-]+", "_", (name or "campanha").strip())
    s = re.sub(r"_+", "_", s).strip("_")
    return (s or "campanha")[:max_len]


def _count_pending_initial_leads(conn, campaign_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::int AS n
            FROM campaign_leads
            WHERE campaign_id = %s
              AND COALESCE(removed_from_funnel, FALSE) = FALSE
              AND COALESCE(cadence_status, 'active') NOT IN ('converted', 'lost', 'replied')
              AND status = 'pending'
              AND current_step = 1
            """,
            (campaign_id,),
        )
        row = cur.fetchone()
        return int((row[0] if row else 0) or 0)


def _remanent_rows_to_csv_text(rows: list) -> str:
    """Mesmas colunas do export unitário (_remanent_csv_response), sem Response HTTP."""

    def _cell(v):
        if v is None:
            return ""
        if hasattr(v, "isoformat"):
            return v.isoformat()
        return str(v)

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(
        [
            "id",
            "phone",
            "name",
            "status",
            "current_step",
            "cadence_status",
            "last_sent_stage",
            "last_message_sent_at",
            "whatsapp_link",
            "notes",
        ]
    )
    for r in rows:
        w.writerow(
            [
                r.get("id"),
                _cell(r.get("phone")),
                _cell(r.get("name")),
                _cell(r.get("status")),
                r.get("current_step"),
                _cell(r.get("cadence_status")),
                _cell(r.get("last_sent_stage")),
                _cell(r.get("last_message_sent_at")),
                _cell(r.get("whatsapp_link")),
                _cell(r.get("notes")),
            ]
        )
    return buf.getvalue()


def _fetch_campaigns_for_ops_backup(conn, *, user_id=None, statuses=None):
    """Campanhas elegíveis ao backup em lote (por status operacional)."""
    statuses = statuses or _DEFAULT_BACKUP_CAMPAIGN_STATUSES
    params = [list(statuses)]
    user_clause = ""
    if user_id is not None:
        user_clause = " AND c.user_id = %s"
        params.append(int(user_id))
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT c.id, c.name, c.status, c.user_id, c.enable_cadence, c.use_uazapi_sender,
                   u.email AS user_email
            FROM campaigns c
            JOIN users u ON u.id = c.user_id
            WHERE c.status = ANY(%s)
            {user_clause}
            ORDER BY c.user_id ASC, c.id ASC
            """,
            tuple(params),
        )
        return [dict(r) for r in (cur.fetchall() or [])]


def _build_pending_initial_backup_plan(conn, *, user_id=None, statuses=None) -> dict:
    campaigns = _fetch_campaigns_for_ops_backup(conn, user_id=user_id, statuses=statuses)
    by_user: dict[int, dict] = {}
    total_leads = 0
    campaigns_in_zip = 0

    for camp in campaigns:
        cid = int(camp["id"])
        uid = int(camp["user_id"])
        pending_n = _count_pending_initial_leads(conn, cid)
        block = by_user.setdefault(
            uid,
            {
                "user_id": uid,
                "email": camp.get("user_email") or "",
                "campaigns": [],
            },
        )
        block["campaigns"].append(
            {
                "id": cid,
                "name": camp.get("name") or "",
                "status": camp.get("status") or "",
                "pending_initial_count": pending_n,
                "enable_cadence": bool(camp.get("enable_cadence")),
                "use_uazapi_sender": bool(camp.get("use_uazapi_sender")),
            }
        )
        if pending_n > 0:
            total_leads += pending_n
            campaigns_in_zip += 1

    users_list = [by_user[k] for k in sorted(by_user.keys())]
    return {
        "dry_run": True,
        "criteria": {
            "statuses": list(statuses or _DEFAULT_BACKUP_CAMPAIGN_STATUSES),
            "scoped_user_id": user_id,
            "omit_empty_campaigns_in_zip": True,
        },
        "summary": {
            "users": len(users_list),
            "campaigns": len(campaigns),
            "campaigns_with_pending_initial": campaigns_in_zip,
            "pending_initial_leads": total_leads,
        },
        "users": users_list,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }


def _build_pending_initial_backup_zip(conn, *, user_id=None, statuses=None) -> io.BytesIO:
    """
    ZIP em memória: um CSV por campanha com pendentes iniciais.
    Campanhas com 0 pendentes são omitidas (sem arquivo vazio).
    """
    campaigns = _fetch_campaigns_for_ops_backup(conn, user_id=user_id, statuses=statuses)
    zbuf = io.BytesIO()
    with zipfile.ZipFile(zbuf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for camp in campaigns:
            cid = int(camp["id"])
            uid = int(camp["user_id"])
            rows = _fetch_remanent_lead_rows(cid, "pending_initial")
            if not rows:
                continue
            slug = _slug_for_export_filename(camp.get("name") or "campanha")
            arcname = f"{uid}_{cid}_{slug}_pending_initial.csv"
            csv_text = "\ufeff" + _remanent_rows_to_csv_text(rows)
            zf.writestr(arcname, csv_text.encode("utf-8"))
    zbuf.seek(0)
    return zbuf


def _build_purge_active_dry_run(conn, *, user_id=None, statuses=None) -> dict:
    """O que seria afetado se o operador excluir manualmente campanhas ativas (sem delete)."""
    campaigns = _fetch_campaigns_for_ops_backup(conn, user_id=user_id, statuses=statuses)
    by_user: dict[int, dict] = {}
    total_leads = 0
    total_pending_initial = 0

    for camp in campaigns:
        cid = int(camp["id"])
        uid = int(camp["user_id"])
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*)::int FROM campaign_leads WHERE campaign_id = %s",
                (cid,),
            )
            lead_total = int((cur.fetchone() or [0])[0] or 0)
        pending_initial = _count_pending_initial_leads(conn, cid)
        total_leads += lead_total
        total_pending_initial += pending_initial
        block = by_user.setdefault(
            uid,
            {
                "user_id": uid,
                "email": camp.get("user_email") or "",
                "campaigns": [],
            },
        )
        block["campaigns"].append(
            {
                "id": cid,
                "name": camp.get("name") or "",
                "status": camp.get("status") or "",
                "total_leads": lead_total,
                "pending_initial_count": pending_initial,
                "enable_cadence": bool(camp.get("enable_cadence")),
                "use_uazapi_sender": bool(camp.get("use_uazapi_sender")),
            }
        )

    return {
        "dry_run": True,
        "action": "purge_manual_only",
        "message": "Nenhuma campanha foi excluída. Use o admin para excluir após conferir o backup.",
        "criteria": {
            "statuses": list(statuses or _DEFAULT_BACKUP_CAMPAIGN_STATUSES),
            "scoped_user_id": user_id,
        },
        "summary": {
            "users": len(by_user),
            "campaigns": len(campaigns),
            "total_leads": total_leads,
            "pending_initial_leads": total_pending_initial,
        },
        "users": [by_user[k] for k in sorted(by_user.keys())],
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }


@app.route("/api/admin/campaigns/export-pending-initial-backup", methods=["GET"])
@login_required
@admin_required
def admin_export_pending_initial_backup():
    """
    Backup ZIP: CSV ``pending_initial`` por campanha (running/pending/paused por padrão).
    Query: ``user_id``, ``statuses`` (vírgula), ``dry_run=1`` (JSON plano, sem ZIP).
    """
    scoped_user_id = None
    uid_raw = request.args.get("user_id")
    if uid_raw is not None and str(uid_raw).strip() != "":
        try:
            scoped_user_id = int(uid_raw)
        except (TypeError, ValueError):
            return (
                jsonify(
                    {
                        "error": "invalid_user_id",
                        "message": "user_id deve ser inteiro.",
                    }
                ),
                400,
            )
    statuses = _parse_backup_campaign_statuses(request.args.get("statuses"))
    dry_run = _truthy_json_flag(request.args.get("dry_run"), default=False)

    conn = get_db_connection()
    try:
        if dry_run:
            return jsonify(
                _build_pending_initial_backup_plan(
                    conn, user_id=scoped_user_id, statuses=statuses
                )
            )
        zbuf = _build_pending_initial_backup_zip(
            conn, user_id=scoped_user_id, statuses=statuses
        )
    finally:
        conn.close()

    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    fname = f"pending_initial_backup_{stamp}Z.zip"
    return send_file(
        zbuf,
        mimetype="application/zip",
        as_attachment=True,
        download_name=fname,
    )


@app.route("/api/admin/campaigns/purge-active/dry-run", methods=["GET"])
@login_required
@admin_required
def admin_purge_active_dry_run():
    """
    Dry-run: campanhas que entrariam num purge manual (mesmos filtros do backup em lote).
    Query: ``user_id``, ``statuses``.
    """
    scoped_user_id = None
    uid_raw = request.args.get("user_id")
    if uid_raw is not None and str(uid_raw).strip() != "":
        try:
            scoped_user_id = int(uid_raw)
        except (TypeError, ValueError):
            return (
                jsonify(
                    {
                        "error": "invalid_user_id",
                        "message": "user_id deve ser inteiro.",
                    }
                ),
                400,
            )
    statuses = _parse_backup_campaign_statuses(request.args.get("statuses"))
    conn = get_db_connection()
    try:
        payload = _build_purge_active_dry_run(
            conn, user_id=scoped_user_id, statuses=statuses
        )
        return jsonify(payload)
    finally:
        conn.close()


@app.route('/api/campaigns/<int:campaign_id>/leads/<int:lead_id>/move', methods=['POST'])
@login_required
def move_campaign_lead(campaign_id, lead_id):
    """API: Move a lead to a different step or status on the kanban board"""
    campaign = Campaign.get_by_id(campaign_id, current_user.id)
    if not campaign:
        return json.dumps({'error': 'Campanha não encontrada'}), 404
    
    data = request.json
    target_step = data.get('target_step', 1)
    target_status = data.get('target_status', 'active')
    
    # Validate target_status
    valid_statuses = ['pending', 'active', 'snoozed', 'converted', 'lost', 'stopped', 'replied']
    if target_status not in valid_statuses:
        return json.dumps({'error': f'Status inválido: {target_status}'}), 400
    
    conn = get_db_connection()
    with conn.cursor() as cur:
        # Verify lead belongs to this campaign
        cur.execute("SELECT id FROM campaign_leads WHERE id = %s AND campaign_id = %s", (lead_id, campaign_id))
        if not cur.fetchone():
            conn.close()
            return json.dumps({'error': 'Lead não encontrado'}), 404
        
        remove_from_funnel = target_status in ('converted', 'lost')
        cur.execute("""
            UPDATE campaign_leads 
            SET current_step = %s,
                cadence_status = %s,
                removed_from_funnel = %s
            WHERE id = %s AND campaign_id = %s
        """, (target_step, target_status, remove_from_funnel, lead_id, campaign_id))
    conn.commit()
    conn.close()
    
    return json.dumps({'success': True, 'lead_id': lead_id, 'new_step': target_step, 'new_status': target_status})


@app.route('/api/leads/<int:lead_id>/note', methods=['POST'])
@login_required
def update_lead_note(lead_id):
    """API: Update note for a specific lead"""
    data = request.json
    note = data.get('note', '')
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Security check: ensure lead belongs to a campaign owned by user
            cur.execute("""
                SELECT cl.id FROM campaign_leads cl
                JOIN campaigns c ON cl.campaign_id = c.id
                WHERE cl.id = %s AND c.user_id = %s
            """, (lead_id, current_user.id))
            
            if not cur.fetchone():
                return json.dumps({'error': 'Lead não encontrado ou acesso negado'}), 404
            
            cur.execute("UPDATE campaign_leads SET notes = %s WHERE id = %s", (note, lead_id))
        conn.commit()
        return json.dumps({'success': True})
    except Exception as e:
        conn.rollback()
        return json.dumps({'error': str(e)}), 500
    finally:
        conn.close()


# --- Rotas de Admin ---

@app.route('/admin')
@login_required
@admin_required
def admin_dashboard():
    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Estatísticas Gerais
        cur.execute("SELECT COUNT(*) as count FROM users")
        total_users = cur.fetchone()['count']
        
        cur.execute("SELECT COUNT(*) as count FROM licenses WHERE status = 'active'")
        active_licenses = cur.fetchone()['count']
        
        cur.execute("SELECT COUNT(*) as count FROM campaigns")
        total_campaigns = cur.fetchone()['count']

        cur.execute("SELECT COUNT(*) as count FROM campaign_leads WHERE status = 'sent'")
        total_sent = cur.fetchone()['count']
        
    conn.close()
    
    return render_template('admin/dashboard.html', 
                         total_users=total_users, 
                         active_licenses=active_licenses,
                         total_campaigns=total_campaigns,
                         total_sent=total_sent)

@app.route('/admin/campaigns')
@login_required
@admin_required
def admin_campaigns():
    status_filter = request.args.get('status')
    
    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Counts for filters
        cur.execute("SELECT COUNT(*) as count FROM campaigns")
        count_all = cur.fetchone()['count']
        
        cur.execute("SELECT COUNT(*) as count FROM campaigns WHERE status = 'running'")
        count_running = cur.fetchone()['count']
        
        cur.execute("SELECT COUNT(*) as count FROM campaigns WHERE status = 'pending'")
        count_pending = cur.fetchone()['count']
        
        cur.execute("SELECT COUNT(*) as count FROM campaigns WHERE status = 'paused'")
        count_paused = cur.fetchone()['count']
        
        cur.execute("SELECT COUNT(*) as count FROM campaigns WHERE status = 'completed'")
        count_completed = cur.fetchone()['count']
        
        # Build query based on filter
        if status_filter:
            cur.execute("""
                SELECT c.*, u.email as user_email,
                       (SELECT COUNT(*) FROM campaign_leads WHERE campaign_id = c.id) as total_leads,
                       (SELECT COUNT(*) FROM campaign_leads WHERE campaign_id = c.id AND status = 'sent') as sent_count,
                       (SELECT COUNT(*) FROM campaign_leads WHERE campaign_id = c.id AND status = 'pending') as pending_count
                FROM campaigns c
                JOIN users u ON c.user_id = u.id
                WHERE c.status = %s
                ORDER BY c.created_at DESC
            """, (status_filter,))
        else:
            cur.execute("""
                SELECT c.*, u.email as user_email,
                       (SELECT COUNT(*) FROM campaign_leads WHERE campaign_id = c.id) as total_leads,
                       (SELECT COUNT(*) FROM campaign_leads WHERE campaign_id = c.id AND status = 'sent') as sent_count,
                       (SELECT COUNT(*) FROM campaign_leads WHERE campaign_id = c.id AND status = 'pending') as pending_count
                FROM campaigns c
                JOIN users u ON c.user_id = u.id
                ORDER BY c.created_at DESC
            """)
        
        campaigns = cur.fetchall()

    try:
        campaigns_out = []
        for c in campaigns:
            c = dict(c)
            rec = None
            if c.get("enable_cadence") and c.get("use_uazapi_sender"):
                rec = _reconciled_uazapi_cadence_counts_via_stage_progress(
                    conn, c["id"], c, int(c.get("total_leads") or 0)
                )
            elif c.get("use_uazapi_sender") and c.get("uazapi_folder_id") and not c.get("enable_cadence"):
                rec = _reconciled_uazapi_single_folder_list_folders(
                    c["id"], c, int(c.get("total_leads") or 0)
                )
            if rec:
                c["sent_count"] = rec["sent"]
                c["pending_count"] = rec["pending"]
            campaigns_out.append(c)
        campaigns = campaigns_out
    finally:
        conn.close()

    counts = {
        'all': count_all,
        'running': count_running,
        'pending': count_pending,
        'paused': count_paused,
        'completed': count_completed
    }

    return render_template('admin/campaigns.html',
                         campaigns=campaigns,
                         status_filter=status_filter,
                         counts=counts,
                         csrf_token=session.get('csrf_token', ''))


@app.route('/api/admin/campaigns/<int:campaign_id>/detail', methods=['GET'])
@login_required
@admin_required
def admin_campaign_detail_api(campaign_id):
    """Superadmin: agregação de campanha + fila + instâncias + chunks Uazapi (equivale às queries manuais)."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT c.*, u.email AS user_email
                FROM campaigns c
                JOIN users u ON u.id = c.user_id
                WHERE c.id = %s
                """,
                (campaign_id,),
            )
            campaign = cur.fetchone()
            if not campaign:
                return jsonify({"error": "Campanha não encontrada"}), 404

            cur.execute(
                """
                SELECT status, COUNT(*)::int AS n
                FROM campaign_leads
                WHERE campaign_id = %s
                GROUP BY status
                ORDER BY status
                """,
                (campaign_id,),
            )
            lead_status_counts = cur.fetchall()

            cur.execute(
                """
                SELECT i.id, i.name, i.status,
                       COALESCE(i.api_provider, '') AS api_provider,
                       (i.apikey IS NOT NULL AND TRIM(i.apikey) <> '') AS has_apikey
                FROM campaign_instances ci
                JOIN instances i ON i.id = ci.instance_id
                WHERE ci.campaign_id = %s
                ORDER BY i.id
                """,
                (campaign_id,),
            )
            instances = cur.fetchall()

            cur.execute(
                """
                SELECT i.id, i.name, i.status,
                       COALESCE(i.api_provider, 'megaapi') AS api_provider,
                       (i.apikey IS NOT NULL AND TRIM(i.apikey) <> '') AS has_apikey
                FROM instances i
                WHERE i.user_id = %s
                  AND COALESCE(i.api_provider, 'megaapi') = 'uazapi'
                ORDER BY i.id
                """,
                (campaign["user_id"],),
            )
            available_instances = cur.fetchall()

            cur.execute(
                """
                SELECT id, stage, instance_id, scheduled_for, status, planned_count,
                       success_count, failed_count, uazapi_folder_id,
                       created_at, updated_at
                FROM campaign_stage_sends
                WHERE campaign_id = %s
                ORDER BY scheduled_for NULLS LAST, id
                LIMIT 200
                """,
                (campaign_id,),
            )
            stage_sends = cur.fetchall()

            cur.execute(
                """
                SELECT step_number, step_label, delay_days, created_at
                FROM campaign_steps
                WHERE campaign_id = %s
                ORDER BY step_number
                """,
                (campaign_id,),
            )
            steps = cur.fetchall()

            cur.execute(
                """
                SELECT COUNT(*)::int AS total_leads,
                       COUNT(*) FILTER (WHERE status = 'sent')::int AS sent_count,
                       COUNT(*) FILTER (WHERE status = 'pending')::int AS pending_count
                FROM campaign_leads
                WHERE campaign_id = %s
                """,
                (campaign_id,),
            )
            counts = cur.fetchone()

            # Mesmo SSOT que Minhas Campanhas (cadência + stage_progress OU pasta única + list_folders).
            rec = None
            if campaign.get("enable_cadence") and campaign.get("use_uazapi_sender"):
                rec = _reconciled_uazapi_cadence_counts_via_stage_progress(
                    conn, campaign_id, dict(campaign), int(counts.get("total_leads") or 0)
                )
            elif (
                campaign.get("use_uazapi_sender")
                and campaign.get("uazapi_folder_id")
                and not campaign.get("enable_cadence")
            ):
                rec = _reconciled_uazapi_single_folder_list_folders(
                    campaign_id, dict(campaign), int(counts.get("total_leads") or 0)
                )
            if rec:
                counts = dict(counts)
                counts["sent_count"] = rec["sent"]
                counts["pending_count"] = rec["pending"]
    finally:
        conn.close()

    def fmt_dt(dt):
        if dt is None:
            return None
        brt = to_brt(dt)
        return brt.strftime('%d/%m/%Y %H:%M') + ' BRT' if brt else None

    def serialize_row(r):
        out = dict(r)
        for k, v in list(out.items()):
            if isinstance(v, datetime):
                out[k] = fmt_dt(v)
        return out

    campaign = dict(campaign)
    campaign["sent_today"] = _campaign_sent_today_for_display(campaign_id)
    c = serialize_row(campaign)
    mt = c.get('message_template')
    if isinstance(mt, str) and len(mt) > 1200:
        c['message_template'] = mt[:1200] + '… (truncado no painel; ver DB para o texto completo)'

    return jsonify(
        {
            "campaign": c,
            "lead_status_counts": [dict(r) for r in lead_status_counts],
            "counts": dict(counts) if counts else {},
            "instances": [_admin_serialize_campaign_instance_row(r) for r in instances],
            "available_instances": [
                _admin_serialize_campaign_instance_row(r) for r in available_instances
            ],
            "stage_sends": [serialize_row(dict(r)) for r in stage_sends],
            "steps": [serialize_row(dict(r)) for r in steps],
        }
    )


def _admin_serialize_campaign_instance_row(row) -> dict:
    d = dict(row)
    d["has_apikey"] = bool(d.get("has_apikey"))
    return d


@app.route("/api/admin/campaigns/<int:campaign_id>/instances", methods=["POST"])
@login_required
@admin_required
def admin_link_campaign_instance(campaign_id):
    """
    Vincula uma instância Uazapi do dono da campanha em ``campaign_instances``.
    Body: ``{ "instance_id": int, "replace": bool? }`` — se ``replace``, remove vínculos atuais antes.
    """
    csrf_err = _verify_json_csrf()
    if csrf_err:
        return csrf_err

    data = request.get_json(silent=True) or {}
    try:
        instance_id = int(data.get("instance_id"))
    except (TypeError, ValueError):
        return jsonify({"error": "Informe instance_id válido."}), 400
    replace = bool(data.get("replace"))

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, user_id, name
                FROM campaigns
                WHERE id = %s
                FOR UPDATE
                """,
                (campaign_id,),
            )
            campaign = cur.fetchone()
            if not campaign:
                conn.rollback()
                return jsonify({"error": "Campanha não encontrada."}), 404

            cur.execute(
                """
                SELECT id, name, status, user_id,
                       COALESCE(api_provider, 'megaapi') AS api_provider,
                       (apikey IS NOT NULL AND TRIM(apikey) <> '') AS has_apikey
                FROM instances
                WHERE id = %s
                FOR UPDATE
                """,
                (instance_id,),
            )
            inst = cur.fetchone()
            if not inst:
                conn.rollback()
                return jsonify({"error": "Instância não encontrada."}), 404
            if int(inst["user_id"]) != int(campaign["user_id"]):
                conn.rollback()
                return jsonify(
                    {"error": "A instância não pertence ao dono desta campanha."}
                ), 400
            if (inst.get("api_provider") or "").lower() != "uazapi":
                conn.rollback()
                return jsonify(
                    {"error": "Só é possível vincular instâncias com api_provider=uazapi."}
                ), 400

            if replace:
                cur.execute(
                    "DELETE FROM campaign_instances WHERE campaign_id = %s",
                    (campaign_id,),
                )

            cur.execute(
                """
                INSERT INTO campaign_instances (campaign_id, instance_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                """,
                (campaign_id, instance_id),
            )

            cur.execute(
                """
                SELECT i.id, i.name, i.status,
                       COALESCE(i.api_provider, '') AS api_provider,
                       (i.apikey IS NOT NULL AND TRIM(i.apikey) <> '') AS has_apikey
                FROM campaign_instances ci
                JOIN instances i ON i.id = ci.instance_id
                WHERE ci.campaign_id = %s
                ORDER BY i.id
                """,
                (campaign_id,),
            )
            linked = [_admin_serialize_campaign_instance_row(r) for r in cur.fetchall()]
        conn.commit()
        warning = None
        if not inst.get("has_apikey"):
            warning = (
                "Instância vinculada, mas sem token Uazapi. "
                "Cole o token em Detalhes do Usuário antes do force chunk."
            )
        return jsonify(
            {
                "success": True,
                "campaign_id": campaign_id,
                "instance_id": instance_id,
                "replaced": replace,
                "instances": linked,
                "warning": warning,
            }
        )
    except Exception as e:
        conn.rollback()
        print(f"[admin_link_campaign_instance] campaign={campaign_id}: {e}")
        return jsonify({"error": "Erro ao vincular instância."}), 500
    finally:
        conn.close()


@app.route(
    "/api/admin/campaigns/<int:campaign_id>/instances/<int:instance_id>",
    methods=["DELETE"],
)
@login_required
@admin_required
def admin_unlink_campaign_instance(campaign_id, instance_id):
    """Remove o vínculo campanha↔instância em ``campaign_instances``."""
    csrf_err = _verify_json_csrf()
    if csrf_err:
        return csrf_err

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id FROM campaigns WHERE id = %s FOR UPDATE",
                (campaign_id,),
            )
            if not cur.fetchone():
                conn.rollback()
                return jsonify({"error": "Campanha não encontrada."}), 404

            cur.execute(
                """
                DELETE FROM campaign_instances
                WHERE campaign_id = %s AND instance_id = %s
                RETURNING id
                """,
                (campaign_id, instance_id),
            )
            deleted = cur.fetchone()
            if not deleted:
                conn.rollback()
                return jsonify({"error": "Vínculo não encontrado."}), 404

            cur.execute(
                """
                SELECT i.id, i.name, i.status,
                       COALESCE(i.api_provider, '') AS api_provider,
                       (i.apikey IS NOT NULL AND TRIM(i.apikey) <> '') AS has_apikey
                FROM campaign_instances ci
                JOIN instances i ON i.id = ci.instance_id
                WHERE ci.campaign_id = %s
                ORDER BY i.id
                """,
                (campaign_id,),
            )
            linked = [_admin_serialize_campaign_instance_row(r) for r in cur.fetchall()]
        conn.commit()
        return jsonify(
            {
                "success": True,
                "campaign_id": campaign_id,
                "instance_id": instance_id,
                "instances": linked,
            }
        )
    except Exception as e:
        conn.rollback()
        print(f"[admin_unlink_campaign_instance] campaign={campaign_id}: {e}")
        return jsonify({"error": "Erro ao desvincular instância."}), 500
    finally:
        conn.close()


@app.route("/api/admin/campaigns/<int:campaign_id>/outbox-state", methods=["GET"])
@login_required
def admin_campaign_outbox_state(campaign_id):
    """
    Polling da fila Postgres ``campaign_message_outbox`` + tentativas (tech-spec §7, AC6).
    Query: ``since_id`` (cursor de id outbox), ``since_attempt_id``, ``updated_after`` (ISO-8601).
    """
    gate = _require_message_outbox_phase1_api()
    if gate:
        return gate
    if not _admin_outbox_poll_rate_allow(current_user.id):
        return (
            jsonify(
                {
                    "error": "rate_limited",
                    "message": "Muitas requisições de polling. Aguarde ou aumente o intervalo.",
                }
            ),
            429,
        )

    since_id = request.args.get("since_id", default=0)
    since_attempt_id = request.args.get("since_attempt_id", default=0)
    try:
        since_id = max(0, int(since_id))
        since_attempt_id = max(0, int(since_attempt_id))
    except (TypeError, ValueError):
        return jsonify({"error": "invalid_cursor", "message": "since_id e since_attempt_id devem ser inteiros."}), 400

    updated_after_raw = (request.args.get("updated_after") or "").strip()
    updated_after_ts = None
    if updated_after_raw:
        try:
            s = updated_after_raw
            if s.endswith("Z"):
                s = s[:-1]
            updated_after_ts = datetime.fromisoformat(s)
        except ValueError:
            return jsonify({"error": "invalid_updated_after", "message": "Use ISO-8601 para updated_after."}), 400

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, user_id, name, status, sent_today, use_uazapi_sender, uazapi_folder_id,
                       scheduled_start, created_at
                FROM campaigns
                WHERE id = %s
                """,
                (campaign_id,),
            )
            camp = cur.fetchone()
            if not camp:
                return jsonify({"error": "not_found", "message": "Campanha não encontrada."}), 404

            cur.execute(
                """
                SELECT status, COUNT(*)::int AS n
                FROM campaign_message_outbox
                WHERE campaign_id = %s
                GROUP BY status
                """,
                (campaign_id,),
            )
            outbox_status_counts = {r["status"]: r["n"] for r in (cur.fetchall() or [])}

            cur.execute(
                """
                SELECT id, campaign_id, campaign_lead_id, instance_id, stage, step_priority, status,
                       queued_at, next_run_at, idempotency_key, uazapi_track_id, payload_summary,
                       created_at, updated_at
                FROM campaign_message_outbox
                WHERE campaign_id = %s
                  AND (
                    id > %s
                    OR (%s IS NOT NULL AND updated_at > %s)
                  )
                ORDER BY id ASC
                LIMIT 400
                """,
                (campaign_id, since_id, updated_after_ts, updated_after_ts),
            )
            outbox_rows = cur.fetchall() or []

            cur.execute(
                """
                SELECT a.id, a.outbox_id, a.attempt_no, a.http_status, a.outcome, a.latency_ms,
                       a.started_at, a.finished_at
                FROM campaign_send_attempts a
                INNER JOIN campaign_message_outbox o ON o.id = a.outbox_id
                WHERE o.campaign_id = %s AND a.id > %s
                ORDER BY a.id ASC
                LIMIT 400
                """,
                (campaign_id, since_attempt_id),
            )
            attempt_rows = cur.fetchall() or []

        def ser_outbox(r):
            d = dict(r)
            for k in ("queued_at", "next_run_at", "created_at", "updated_at"):
                if k in d:
                    d[k] = _isoformat_dt(d[k])
            if d.get("payload_summary") is not None and hasattr(d["payload_summary"], "__iter__"):
                if not isinstance(d["payload_summary"], (dict, list)):
                    d["payload_summary"] = str(d["payload_summary"])
            return d

        def ser_attempt(r):
            d = dict(r)
            for k in ("started_at", "finished_at"):
                if k in d:
                    d[k] = _isoformat_dt(d[k])
            return d

        c = dict(camp)
        c["sent_today"] = _campaign_sent_today_for_display(campaign_id)
        for k in ("scheduled_start", "created_at"):
            if k in c:
                c[k] = _isoformat_dt(c[k])

        max_oid = max((r["id"] for r in outbox_rows), default=since_id)
        max_aid = max((r["id"] for r in attempt_rows), default=since_attempt_id)

        return jsonify(
            {
                "campaign": c,
                "outbox_counts_by_status": outbox_status_counts,
                "outbox": [ser_outbox(r) for r in outbox_rows],
                "attempts": [ser_attempt(r) for r in attempt_rows],
                "cursor": {
                    "since_id": max_oid,
                    "since_attempt_id": max_aid,
                    "server_time": datetime.utcnow().isoformat() + "Z",
                },
            }
        )
    finally:
        conn.close()


@app.route("/api/admin/campaigns/<int:campaign_id>/dispatch-audit", methods=["GET"])
@login_required
def admin_campaign_dispatch_audit(campaign_id):
    """
    Auditoria append-only dos disparos outbox (JSONL em disco).
    Query: ``tail`` (max linhas a partir do fim, default 800, máx 5000), ``format=json|ndjson``.
    """
    gate = _require_message_outbox_phase1_api()
    if gate:
        return gate

    fmt = (request.args.get("format") or "json").strip().lower()
    try:
        tail_n = min(5000, max(1, int(request.args.get("tail", "800"))))
    except (TypeError, ValueError):
        tail_n = 800

    from utils.campaign_dispatch_audit import dispatch_audit_jsonl_path

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT user_id FROM campaigns WHERE id = %s", (campaign_id,))
            row = cur.fetchone()
        if not row:
            return jsonify({"error": "not_found", "message": "Campanha não encontrada."}), 404
        uid = int(row["user_id"])
    finally:
        conn.close()

    path = dispatch_audit_jsonl_path(uid, campaign_id, ensure_parent=False)
    if not os.path.isfile(path):
        if fmt == "ndjson":
            return Response("", mimetype="application/x-ndjson")
        return jsonify({"campaign_id": campaign_id, "path": path, "events": [], "truncated": False})

    with open(path, "r", encoding="utf-8") as f:
        all_lines = f.readlines()
    chunk = all_lines[-tail_n:]
    truncated = len(all_lines) > tail_n

    if fmt == "ndjson":
        return Response("".join(chunk), mimetype="application/x-ndjson")

    events = []
    for ln in chunk:
        ln = ln.strip()
        if not ln:
            continue
        try:
            events.append(json.loads(ln))
        except json.JSONDecodeError:
            events.append({"raw": ln, "parse_error": True})

    return jsonify(
        {
            "campaign_id": campaign_id,
            "path": path,
            "events": events,
            "truncated": truncated,
            "returned_lines": len(chunk),
        }
    )


_ACTIVE_CAMPAIGN_STATUSES_FOR_PAUSE_PLAN = ("running", "pending")


def _campaign_row_for_pause_plan(row: dict) -> dict:
    """Serialização mínima de campanha para dry-run pause-except-latest."""
    return {
        "id": int(row["id"]),
        "name": row.get("name") or "",
        "status": (row.get("status") or "").strip(),
        "created_at": _isoformat_dt(row.get("created_at")),
        "use_uazapi_sender": bool(row.get("use_uazapi_sender")),
        "enable_cadence": bool(row.get("enable_cadence")),
        "uazapi_folder_id": row.get("uazapi_folder_id"),
    }


def _build_pause_except_latest_dry_run(
    conn,
    *,
    user_id: Optional[int] = None,
    only_conflicts: bool = False,
) -> dict:
    """
    Por usuário: entre campanhas ``running``/``pending``, mantém a mais recente
    (``created_at`` DESC, ``id`` DESC) e lista as demais que seriam pausadas.
    """
    params: list = []
    user_clause = ""
    if user_id is not None:
        user_clause = " AND c.user_id = %s"
        params.append(int(user_id))

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT c.id, c.name, c.status, c.created_at, c.user_id,
                   c.use_uazapi_sender, c.enable_cadence, c.uazapi_folder_id,
                   u.email AS user_email
            FROM campaigns c
            JOIN users u ON u.id = c.user_id
            WHERE c.status IN ('running', 'pending')
            {user_clause}
            ORDER BY c.user_id ASC, c.created_at DESC NULLS LAST, c.id DESC
            """,
            tuple(params),
        )
        rows = [dict(r) for r in (cur.fetchall() or [])]

    by_user: dict[int, list[dict]] = {}
    for row in rows:
        uid = int(row["user_id"])
        by_user.setdefault(uid, []).append(row)

    users_out = []
    total_would_pause = 0
    users_multiple = 0
    users_single = 0

    for uid in sorted(by_user.keys()):
        camps = by_user[uid]
        keep_row = camps[0]
        pause_rows = camps[1:]
        if only_conflicts and len(camps) < 2:
            continue
        if len(camps) > 1:
            users_multiple += 1
        else:
            users_single += 1
        total_would_pause += len(pause_rows)
        users_out.append(
            {
                "user_id": uid,
                "user_email": keep_row.get("user_email") or "",
                "active_count": len(camps),
                "keep": _campaign_row_for_pause_plan(keep_row),
                "would_pause": [_campaign_row_for_pause_plan(r) for r in pause_rows],
            }
        )

    return {
        "dry_run": True,  # sobrescrito pelo caller em execução real
        "criteria": {
            "active_statuses": list(_ACTIVE_CAMPAIGN_STATUSES_FOR_PAUSE_PLAN),
            "keep_rule": "most_recent_by_created_at_then_id",
            "scoped_user_id": user_id,
            "only_conflicts": only_conflicts,
        },
        "summary": {
            "users_in_report": len(users_out),
            "users_with_multiple_active": users_multiple,
            "users_with_single_active": users_single,
            "campaigns_would_stay_active": len(users_out),
            "campaigns_would_pause": total_would_pause,
        },
        "users": users_out,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }


def _parse_pause_except_latest_scope(*, user_id_raw, only_conflicts_raw):
    """Resolve filtros comuns (query ou JSON body)."""
    scoped_user_id = None
    if user_id_raw is not None and str(user_id_raw).strip() != "":
        try:
            scoped_user_id = int(user_id_raw)
        except (TypeError, ValueError):
            return None, None, (
                jsonify({"error": "invalid_user_id", "message": "user_id deve ser inteiro."}),
                400,
            )
    only_conflicts = str(only_conflicts_raw or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    return scoped_user_id, only_conflicts, None


def _truthy_json_flag(value, *, default=False) -> bool:
    if value is None:
        return default
    if value is True:
        return True
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    return bool(value)


def _admin_pause_campaign_active(campaign_id: int) -> dict:
    """Pausa campanha running/pending (admin; mesma regra do pause unitário, sem gate outbox)."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, status, user_id, use_uazapi_sender, uazapi_folder_id
                FROM campaigns WHERE id = %s
                """,
                (campaign_id,),
            )
            row = cur.fetchone()
        if not row:
            return {"campaign_id": campaign_id, "ok": False, "error": "not_found"}
        st = (row.get("status") or "").strip()
        if st not in _ACTIVE_CAMPAIGN_STATUSES_FOR_PAUSE_PLAN:
            return {
                "campaign_id": campaign_id,
                "ok": False,
                "error": "skip_status",
                "status": st,
            }
        if row.get("use_uazapi_sender") and row.get("uazapi_folder_id"):
            success, err = _uazapi_control_campaign(
                campaign_id, int(row["user_id"]), "stop", admin_mode=True
            )
            return {
                "campaign_id": campaign_id,
                "ok": bool(success),
                "error": err,
                "via": "uazapi_folder",
            }
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE campaigns SET status = 'paused'
                WHERE id = %s AND status IN ('running', 'pending')
                """,
                (campaign_id,),
            )
            ok = cur.rowcount > 0
        if ok:
            conn.commit()
            return {"campaign_id": campaign_id, "ok": True, "via": "db"}
        conn.rollback()
        return {"campaign_id": campaign_id, "ok": False, "error": "conflict"}
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"campaign_id": campaign_id, "ok": False, "error": str(e)}
    finally:
        conn.close()


@app.route("/api/admin/campaigns/pause-except-latest/dry-run", methods=["GET"])
@login_required
@admin_required
def admin_campaigns_pause_except_latest_dry_run():
    """
    Dry-run (GET legado): por usuário, qual campanha running/pending permanece ativa (mais recente)
    e quais seriam pausadas. Não altera o banco nem a Uazapi.

    Query: ``user_id`` (opcional), ``only_conflicts`` (1/true = só usuários com 2+ ativas).
    Preferir POST ``/api/admin/campaigns/pause-except-latest`` com ``dry_run: true``.
    """
    scoped_user_id, only_conflicts, err = _parse_pause_except_latest_scope(
        user_id_raw=request.args.get("user_id"),
        only_conflicts_raw=request.args.get("only_conflicts"),
    )
    if err:
        return err

    conn = get_db_connection()
    try:
        payload = _build_pause_except_latest_dry_run(
            conn,
            user_id=scoped_user_id,
            only_conflicts=only_conflicts,
        )
        return jsonify(payload)
    finally:
        conn.close()


@app.route("/api/admin/campaigns/pause-except-latest", methods=["POST"])
@login_required
@admin_required
def admin_campaigns_pause_except_latest():
    """
    Por usuário: mantém a campanha running/pending mais recente; pausa as demais se ``dry_run`` é false.

    JSON: ``dry_run`` (default true), ``user_id``, ``only_conflicts``, ``confirm`` (obrigatório se dry_run false).
    """
    body = request.get_json(silent=True) or {}
    dry_run = _truthy_json_flag(body.get("dry_run"), default=True)

    scoped_user_id, only_conflicts, err = _parse_pause_except_latest_scope(
        user_id_raw=body.get("user_id"),
        only_conflicts_raw=body.get("only_conflicts"),
    )
    if err:
        return err

    if not dry_run:
        csrf_err = _verify_json_csrf()
        if csrf_err:
            return csrf_err
        if not _admin_outbox_mutate_rate_allow(current_user.id):
            return (
                jsonify(
                    {
                        "error": "rate_limited",
                        "message": "Muitas requisições. Tente novamente em um minuto.",
                    }
                ),
                429,
            )
        if not _truthy_json_flag(body.get("confirm")):
            return (
                jsonify(
                    {
                        "error": "confirm_required",
                        "message": "Envie confirm: true para executar pausas reais.",
                    }
                ),
                400,
            )

    conn = get_db_connection()
    try:
        payload = _build_pause_except_latest_dry_run(
            conn,
            user_id=scoped_user_id,
            only_conflicts=only_conflicts,
        )
        if dry_run:
            return jsonify(payload)

        payload["dry_run"] = False
        paused = []
        errors = []
        for user_block in payload.get("users") or []:
            for camp in user_block.get("would_pause") or []:
                cid = int(camp["id"])
                result = _admin_pause_campaign_active(cid)
                entry = {
                    "campaign_id": cid,
                    "name": camp.get("name"),
                    "user_id": user_block.get("user_id"),
                    **result,
                }
                if result.get("ok"):
                    paused.append(entry)
                else:
                    errors.append(entry)

        summary = dict(payload.get("summary") or {})
        summary["campaigns_paused"] = len(paused)
        summary["campaigns_pause_failed"] = len(errors)
        payload["summary"] = summary
        payload["paused"] = paused
        payload["errors"] = errors
        return jsonify(payload)
    finally:
        conn.close()


@app.route("/api/admin/users/<int:user_id>/campaigns-active")
@login_required
def admin_user_active_campaigns_api(user_id):
    """
    Superadmin + USE_MESSAGE_OUTBOX: campanhas ``running`` / ``pending`` / ``paused`` do usuário
    (dropdown auditoria — inclui pausadas para leitura do JSONL histórico).
    """
    gate = _require_message_outbox_phase1_api()
    if gate:
        return gate
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, name, status, use_uazapi_sender, enable_cadence
                FROM campaigns
                WHERE user_id = %s AND status IN ('running', 'pending', 'paused')
                ORDER BY
                    CASE status WHEN 'running' THEN 0 WHEN 'pending' THEN 1 WHEN 'paused' THEN 2 ELSE 3 END,
                    name ASC
                """,
                (user_id,),
            )
            rows = cur.fetchall() or []
        return jsonify({"campaigns": [dict(r) for r in rows]})
    finally:
        conn.close()


@app.route("/admin/dispatch-audit")
@login_required
@admin_required
def admin_dispatch_audit_page():
    """UI: seletor usuário + campanha ativa + leitura do JSONL de disparos."""
    if not is_super_admin():
        flash("Apenas superadmin pode acessar a auditoria de disparos.", "error")
        return redirect(url_for("admin_dashboard"))
    if not USE_MESSAGE_OUTBOX:
        flash("USE_MESSAGE_OUTBOX não está ativo no ambiente.", "error")
        return redirect(url_for("admin_dashboard"))
    return render_template("admin/dispatch_audit.html")


@app.route("/api/admin/campaigns/<int:campaign_id>/outbox/pause", methods=["POST"])
@login_required
def admin_campaign_outbox_pause(campaign_id):
    """Pausa campanha (worker outbox não envia enquanto ``status`` ≠ running/pending). CSRF obrigatório."""
    gate = _require_message_outbox_phase1_api()
    if gate:
        return gate
    csrf_err = _verify_json_csrf()
    if csrf_err:
        return csrf_err
    if not _admin_outbox_mutate_rate_allow(current_user.id):
        return (
            jsonify(
                {
                    "error": "rate_limited",
                    "message": "Muitas requisições. Tente novamente em um minuto.",
                }
            ),
            429,
        )

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, status, user_id, use_uazapi_sender, uazapi_folder_id
                FROM campaigns WHERE id = %s
                """,
                (campaign_id,),
            )
            row = cur.fetchone()
        if not row:
            return jsonify({"error": "not_found", "message": "Campanha não encontrada."}), 404

        st = (row.get("status") or "").strip()
        if st not in ("running", "pending"):
            return (
                jsonify(
                    {
                        "error": "conflict",
                        "message": "Só é possível pausar campanha em running ou pending.",
                        "status": st,
                    }
                ),
                409,
            )

        use_uaz = bool(row.get("use_uazapi_sender"))
        folder_id = row.get("uazapi_folder_id")
        needs_uazapi_stop = (
            use_uaz
            and folder_id
            and not _campaign_has_message_outbox_rows(campaign_id)
            and not _campaign_has_chunk_stage_sends(campaign_id)
        )
        if needs_uazapi_stop:
            success, err = _uazapi_control_campaign(
                campaign_id, int(row["user_id"]), "stop", admin_mode=True
            )
            if not success:
                return jsonify({"error": "uazapi_control_failed", "message": err or "Falha na API Uazapi."}), 500
        elif use_uaz:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE campaigns SET status = 'paused' WHERE id = %s AND status IN ('running', 'pending')",
                    (campaign_id,),
                )
                if cur.rowcount == 0:
                    conn.rollback()
                    return (
                        jsonify({"error": "conflict", "message": "Estado da campanha mudou; recarregue."}),
                        409,
                    )
            conn.commit()
        else:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE campaigns SET status = 'paused' WHERE id = %s AND status IN ('running', 'pending')",
                    (campaign_id,),
                )
                if cur.rowcount == 0:
                    conn.rollback()
                    return (
                        jsonify({"error": "conflict", "message": "Estado da campanha mudou; recarregue."}),
                        409,
                    )
            conn.commit()

        return jsonify({"success": True, "status": "paused"})
    except Exception as e:
        conn.rollback()
        print(f"[admin_outbox_pause] {e}")
        return jsonify({"error": "server_error", "message": str(e)}), 500
    finally:
        conn.close()


@app.route("/api/admin/campaigns/<int:campaign_id>/outbox/resume", methods=["POST"])
@login_required
def admin_campaign_outbox_resume(campaign_id):
    """Retoma campanha pausada (status ``running``). CSRF obrigatório."""
    gate = _require_message_outbox_phase1_api()
    if gate:
        return gate
    csrf_err = _verify_json_csrf()
    if csrf_err:
        return csrf_err
    if not _admin_outbox_mutate_rate_allow(current_user.id):
        return (
            jsonify(
                {
                    "error": "rate_limited",
                    "message": "Muitas requisições. Tente novamente em um minuto.",
                }
            ),
            429,
        )

    body = request.get_json(silent=True) or {}
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, status, user_id, use_uazapi_sender, uazapi_folder_id
                FROM campaigns WHERE id = %s
                """,
                (campaign_id,),
            )
            row = cur.fetchone()
        if not row:
            return jsonify({"error": "not_found", "message": "Campanha não encontrada."}), 404

        st = (row.get("status") or "").strip()
        if st != "paused":
            return (
                jsonify(
                    {
                        "error": "conflict",
                        "message": "Só é possível retomar campanha pausada.",
                        "status": st,
                    }
                ),
                409,
            )

        guard = _guard_resume_after_whatsapp_disconnect(conn, campaign_id, body)
        if guard:
            return guard

        conn.close()
        conn = None
        success, err, extra = _resume_campaign_after_pause(
            campaign_id,
            int(row["user_id"]),
            admin_mode=True,
            trigger_initial_chunk=True,
        )
        if not success:
            return jsonify(
                {"error": "uazapi_control_failed", "message": err or "Falha ao retomar campanha."}
            ), 500

        payload = {"success": True, "status": "running"}
        if extra.get("initial_chunk"):
            payload["initial_chunk"] = extra["initial_chunk"]
        return jsonify(payload)
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        print(f"[admin_outbox_resume] {e}")
        return jsonify({"error": "server_error", "message": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route('/api/admin/campaigns/<int:campaign_id>', methods=['DELETE'])
@login_required
@admin_required
def admin_delete_campaign(campaign_id):
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT use_uazapi_sender, uazapi_folder_id FROM campaigns WHERE id = %s", (campaign_id,))
            row = cur.fetchone()
        conn.close()

        if row and row.get('use_uazapi_sender') and row.get('uazapi_folder_id'):
            success, err = _uazapi_control_campaign(campaign_id, current_user.id, 'delete', admin_mode=True)
            if success:
                return {"success": True}
            # MegaAPI, sem instância Uazapi ou API falhou: remover do DB mesmo assim
            print(f"[Admin] Uazapi delete falhou ({err}), removendo campanha do DB.")

        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM campaign_leads WHERE campaign_id = %s", (campaign_id,))
            cur.execute("DELETE FROM campaign_instances WHERE campaign_id = %s", (campaign_id,))
            cur.execute("DELETE FROM campaigns WHERE id = %s", (campaign_id,))
        conn.commit()
        conn.close()
        return {"success": True}
    except Exception as e:
        print(f"Erro ao excluir campanha: {e}")
        return {"error": str(e)}, 500


@app.route('/api/admin/campaigns', methods=['POST'])
@login_required
@admin_required
def admin_create_campaign():
    data = request.get_json(silent=True)
    if not data:
        return json.dumps({'error': 'JSON body é obrigatório'}), 400
    target_user_id = data.get('user_id')
    if not target_user_id:
        return json.dumps({'error': 'user_id é obrigatório'}), 400
    try:
        target_user_id = int(target_user_id)
    except (ValueError, TypeError):
        return json.dumps({'error': 'user_id inválido'}), 400
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM users WHERE id = %s", (target_user_id,))
        if not cur.fetchone():
            conn.close()
            return json.dumps({'error': 'Usuário não encontrado'}), 404
    conn.close()
    return _create_campaign_core(target_user_id, data, admin_id=current_user.id)


@app.route('/api/admin/campaigns/validate-csv', methods=['POST'])
@login_required
@admin_required
def admin_validate_csv():
    """Upload CSV com validação opcional de WhatsApp para criação de campanha admin."""
    from utils.validate_job_csv import (
        _check_phone_with_retry,
        _get_connected_uazapi_token_for_user,
        _normalize_phone_for_api,
    )

    if 'file' not in request.files:
        return json.dumps({'error': 'Nenhum arquivo enviado'}), 400
    file = request.files['file']
    if not file.filename or not file.filename.endswith('.csv'):
        return json.dumps({'error': 'Apenas arquivos .csv são permitidos'}), 400

    target_user_id = request.form.get('user_id')
    if not target_user_id:
        return json.dumps({'error': 'user_id é obrigatório'}), 400
    try:
        target_user_id = int(target_user_id)
    except (ValueError, TypeError):
        return json.dumps({'error': 'user_id inválido'}), 400

    validate_whatsapp = request.form.get('validate_whatsapp', 'false').lower() == 'true'

    try:
        try:
            df = pd.read_csv(file, dtype=str, encoding='utf-8', encoding_errors='replace')
        except TypeError:
            file.seek(0)
            df = pd.read_csv(file, dtype=str, encoding='utf-8')

        cols = [c.lower() for c in df.columns]
        df.columns = cols

        phone_col = next((c for c in cols if 'phone' in c or 'tel' in c or 'cel' in c), None)
        whatsapp_link_col = next((c for c in cols if c == 'whatsapp_link'), None)
        name_col = next((c for c in cols if 'name' in c or 'nome' in c), None)
        status_col = next((c for c in cols if c == 'status'), None)

        if not phone_col and not whatsapp_link_col:
            return json.dumps({'error': 'Nenhuma coluna de telefone encontrada no CSV'}), 400

        if status_col:
            df_filtered = df[df[status_col].astype(str).str.strip() == '1']
        else:
            df_filtered = df

        rows = []
        for df_idx, row in df_filtered.iterrows():
            raw_phone = None
            if whatsapp_link_col and pd.notna(row.get(whatsapp_link_col)):
                from utils.validate_job_csv import _extract_phone_from_link
                raw_phone = _extract_phone_from_link(row[whatsapp_link_col])
            if not raw_phone and phone_col and pd.notna(row.get(phone_col)):
                digits = re.sub(r'\D', '', str(row[phone_col]))
                if len(digits) >= 10:
                    raw_phone = digits
            phone = _normalize_phone_for_api(raw_phone) if raw_phone else None
            if phone:
                rows.append((df_idx, phone))

        seen = set()
        unique_rows = []
        for df_idx, phone in rows:
            if phone not in seen:
                seen.add(phone)
                unique_rows.append((df_idx, phone))
        rows = unique_rows

        if not rows:
            return json.dumps({'error': 'Nenhum número válido encontrado no CSV'}), 400

        indices_drop = set()
        batches_skipped = 0

        if validate_whatsapp:
            conn = get_db_connection()
            token = _get_connected_uazapi_token_for_user(conn, target_user_id)
            conn.close()
            if not token:
                return json.dumps({'error': 'Nenhuma instância Uazapi conectada para validar'}), 400

            uazapi = UazapiService()
            BATCH_SIZE = 5
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i:i + BATCH_SIZE]
                numbers = [phone for _, phone in batch]
                result, err = _check_phone_with_retry(uazapi, token, numbers, timeout=30)
                if result is None:
                    batches_skipped += 1
                    print(f"[admin_validate_csv] batch {i//BATCH_SIZE+1} FALHOU ({err})")
                else:
                    for j, item in enumerate(result):
                        if j < len(batch) and not item.get('isInWhatsapp', True):
                            indices_drop.add(batch[j][0])
                if i + BATCH_SIZE < len(rows):
                    time.sleep(2)

        valid_indices = set(idx for idx, _ in rows) - indices_drop
        df_valid = df_filtered[df_filtered.index.isin(valid_indices)]

        if 'status' not in cols:
            df_valid = df_valid.copy()
            df_valid['status'] = 1

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        user_dir = os.path.join(os.environ.get("STORAGE_DIR", "storage"), str(target_user_id), "uploads")
        os.makedirs(user_dir, exist_ok=True)
        save_path = os.path.join(user_dir, f"admin_upload_{timestamp}.csv")
        df_valid.to_csv(save_path, index=False, encoding='utf-8')

        valid_count = len(df_valid)
        invalid_count = len(indices_drop)

        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO scraping_jobs (user_id, keyword, locations, total_results, lead_count, status, results_path, progress, completed_at)
                VALUES (%s, 'Upload Admin', 'Upload', %s, %s, 'completed', %s, 100, NOW())
                RETURNING id
                """,
                (target_user_id, valid_count, valid_count, save_path)
            )
            job_id = cur.fetchone()[0]
        conn.commit()
        conn.close()

        return json.dumps({
            'success': True,
            'valid': valid_count,
            'invalid': invalid_count,
            'total': len(rows),
            'job_id': job_id,
            'batches_skipped': batches_skipped,
            'partial': batches_skipped > 0,
        })

    except Exception as e:
        print(f"[admin_validate_csv] Erro: {e}")
        return json.dumps({'error': str(e)}), 500


@app.route('/admin/campaigns/new')
@login_required
@admin_required
def admin_new_campaign():
    return render_template(
        'admin/campaigns_new.html',
        plan_daily_limit=5,
        use_message_outbox=USE_MESSAGE_OUTBOX,
        is_super_admin=is_super_admin(),
    )


@app.route('/admin/campaigns/<int:campaign_id>/edit')
@login_required
@admin_required
def admin_edit_campaign(campaign_id):
    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT c.*, u.email as user_email
            FROM campaigns c
            JOIN users u ON u.id = c.user_id
            WHERE c.id = %s
        """, (campaign_id,))
        campaign = cur.fetchone()
        if not campaign:
            conn.close()
            flash("Campanha não encontrada.", "error")
            return redirect(url_for('admin_campaigns'))

        cur.execute("""
            SELECT step_number, step_label, message_template, delay_days, media_type
            FROM campaign_steps WHERE campaign_id = %s ORDER BY step_number
        """, (campaign_id,))
        steps = cur.fetchall()

        cur.execute("""
            SELECT i.id, i.name, i.status
            FROM campaign_instances ci
            JOIN instances i ON i.id = ci.instance_id
            WHERE ci.campaign_id = %s
        """, (campaign_id,))
        instances = cur.fetchall()
    conn.close()

    return render_template('admin/campaigns_edit.html',
                           campaign=campaign,
                           steps=steps,
                           instances=instances)


@app.route('/api/admin/campaigns/sync', methods=['GET'])
@login_required
@admin_required
def admin_sync_campaigns():
    """Sync contadores Uazapi para campanhas running. Agrupa por instância para reduzir chamadas API."""
    SYNC_TTL_SECONDS = 300  # 5 minutos

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT DISTINCT c.id FROM campaigns c WHERE c.status = 'running' AND c.use_uazapi_sender = TRUE"
            )
            campaign_ids = [r['id'] for r in cur.fetchall()]

        if not campaign_ids:
            return json.dumps({'campaigns': []})

        sends_by_token = {}
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT css.id, css.campaign_id, css.uazapi_folder_id, css.last_sync_at,
                       css.instance_id, i.apikey
                FROM campaign_stage_sends css
                JOIN instances i ON i.id = css.instance_id
                WHERE css.campaign_id = ANY(%s)
                  AND css.status = ANY(%s)
                  AND css.uazapi_folder_id IS NOT NULL
            """, (campaign_ids, list(INITIAL_CHUNK_ACTIVE_SEND_STATUSES)))
            sends = cur.fetchall()

        now = datetime.now()
        for send in sends:
            if send.get('last_sync_at'):
                elapsed = (now - send['last_sync_at']).total_seconds()
                if elapsed < SYNC_TTL_SECONDS:
                    continue
            token = send.get('apikey')
            if not token:
                continue
            sends_by_token.setdefault(token, []).append(send)

        uazapi = UazapiService()
        synced_campaign_ids = set()

        for token, token_sends in sends_by_token.items():
            try:
                folders_list = uazapi.list_folders(token)
                if not folders_list:
                    continue
                folders_by_id = {}
                for f in folders_list:
                    fid = str(f.get("id") or f.get("folder_id") or f.get("folderId") or "")
                    if fid:
                        folders_by_id[fid] = f

                for send in token_sends:
                    fid = str(send['uazapi_folder_id'])
                    folder_info = folders_by_id.get(fid)
                    if not folder_info:
                        continue
                    log_success = int(folder_info.get("log_sucess") or folder_info.get("log_success") or 0)
                    log_failed = int(folder_info.get("log_failed") or 0)
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE campaign_stage_sends SET success_count = %s, failed_count = %s, last_sync_at = NOW() WHERE id = %s",
                            (log_success, log_failed, send['id'])
                        )
                    synced_campaign_ids.add(send['campaign_id'])
            except Exception as e:
                print(f"[admin_sync] Erro ao sincronizar token: {e}")
                continue

        conn.commit()

        result = []
        if synced_campaign_ids:
            ids_list = list(synced_campaign_ids)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT campaign_id,
                           COUNT(*) as total_leads,
                           COUNT(*) FILTER (WHERE status = 'sent') as sent_count,
                           COUNT(*) FILTER (WHERE status = 'pending') as pending_count
                    FROM campaign_leads
                    WHERE campaign_id = ANY(%s)
                    GROUP BY campaign_id
                """, (ids_list,))
                leads_counts = {r['campaign_id']: r for r in cur.fetchall()}

                cur.execute(
                    "SELECT id, enable_cadence, use_uazapi_sender, uazapi_folder_id FROM campaigns WHERE id = ANY(%s)",
                    (ids_list,),
                )
                flags_by_id = {r["id"]: r for r in cur.fetchall()}

            for cid in ids_list:
                lc = leads_counts.get(cid, {})
                total = int(lc.get('total_leads') or 0)
                sent = int(lc.get('sent_count') or 0)
                pending_val = int(lc.get('pending_count') or 0)
                row = flags_by_id.get(cid)
                rec = None
                if row and row.get("enable_cadence") and row.get("use_uazapi_sender"):
                    rec = _reconciled_uazapi_cadence_counts_via_stage_progress(
                        conn, cid, dict(row), total
                    )
                elif (
                    row
                    and row.get("use_uazapi_sender")
                    and row.get("uazapi_folder_id")
                    and not row.get("enable_cadence")
                ):
                    rec = _reconciled_uazapi_single_folder_list_folders(cid, dict(row), total)
                if rec:
                    sent = rec["sent"]
                    pending_val = rec["pending"]
                result.append({
                    'id': cid,
                    'total_leads': total,
                    'sent_count': sent,
                    'pending_count': pending_val,
                    'last_sync': now.isoformat(),
                })

        return json.dumps({'campaigns': result})
    except Exception as e:
        print(f"[admin_sync] Erro: {e}")
        return json.dumps({'error': str(e)}), 500
    finally:
        conn.close()


@app.route('/api/admin/campaigns/<int:campaign_id>/update', methods=['POST'])
@login_required
@admin_required
def admin_update_campaign(campaign_id):
    """Edição admin com regras ADR-5 por status."""
    data = request.get_json(silent=True)
    if not data:
        return json.dumps({'error': 'JSON body é obrigatório'}), 400

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM campaigns WHERE id = %s", (campaign_id,))
            campaign = cur.fetchone()
        if not campaign:
            return json.dumps({'error': 'Campanha não encontrada'}), 404

        status = campaign['status']
        if status == 'completed':
            return json.dumps({'error': 'Campanha concluída não pode ser editada'}), 400

        always_fields = {'name'}
        paused_fields = always_fields | {'send_hour_start', 'send_hour_end', 'send_saturday', 'send_sunday', 'delay_min_minutes', 'delay_max_minutes'}
        pending_fields = paused_fields | {'message_templates', 'scheduled_start', 'enable_cadence', 'steps'}

        if status == 'running':
            allowed = always_fields
        elif status == 'paused':
            allowed = paused_fields
        else:
            allowed = pending_fields

        blocked = set(data.keys()) - allowed - {'user_id'}
        if blocked:
            if status == 'running':
                return json.dumps({'error': f'Pause a campanha para editar: {", ".join(sorted(blocked))}'}), 400
            return json.dumps({'error': f'Campos não permitidos para status {status}: {", ".join(sorted(blocked))}'}), 400

        with conn.cursor() as cur:
            if 'name' in data:
                cur.execute("UPDATE campaigns SET name = %s WHERE id = %s", (data['name'], campaign_id))
            if 'send_hour_start' in data and 'send_hour_start' in allowed:
                cur.execute("UPDATE campaigns SET send_hour_start = %s WHERE id = %s", (int(data['send_hour_start']), campaign_id))
            if 'send_hour_end' in data and 'send_hour_end' in allowed:
                cur.execute("UPDATE campaigns SET send_hour_end = %s WHERE id = %s", (int(data['send_hour_end']), campaign_id))
            if 'send_saturday' in data and 'send_saturday' in allowed:
                cur.execute("UPDATE campaigns SET send_saturday = %s WHERE id = %s", (bool(data['send_saturday']), campaign_id))
            if 'send_sunday' in data and 'send_sunday' in allowed:
                cur.execute("UPDATE campaigns SET send_sunday = %s WHERE id = %s", (bool(data['send_sunday']), campaign_id))
            if 'delay_min_minutes' in data and 'delay_min_minutes' in allowed:
                cur.execute("UPDATE campaigns SET delay_min_minutes = %s WHERE id = %s", (data['delay_min_minutes'], campaign_id))
            if 'delay_max_minutes' in data and 'delay_max_minutes' in allowed:
                cur.execute("UPDATE campaigns SET delay_max_minutes = %s WHERE id = %s", (data['delay_max_minutes'], campaign_id))
            if 'message_templates' in data and 'message_templates' in allowed:
                templates = json.dumps(data['message_templates'])
                cur.execute("UPDATE campaigns SET message_template = %s WHERE id = %s", (templates, campaign_id))
            if 'scheduled_start' in data and 'scheduled_start' in allowed:
                val = data['scheduled_start']
                if val:
                    try:
                        parsed = datetime.fromisoformat(val.replace('Z', '+00:00'))
                        if parsed.tzinfo is None:
                            parsed = pytz.timezone('America/Sao_Paulo').localize(parsed)
                        val = parsed.astimezone(pytz.UTC).replace(tzinfo=None)
                    except Exception:
                        return json.dumps({'error': f'Data scheduled_start inválida: {data["scheduled_start"]}'}), 400
                cur.execute("UPDATE campaigns SET scheduled_start = %s WHERE id = %s", (val, campaign_id))
            if 'enable_cadence' in data and 'enable_cadence' in allowed:
                cur.execute("UPDATE campaigns SET enable_cadence = %s WHERE id = %s", (bool(data['enable_cadence']), campaign_id))

            if 'steps' in data and 'steps' in allowed:
                for step in data['steps']:
                    step_number = step.get('step_number', 1)
                    step_label = step.get('step_label', '')
                    step_messages = step.get('message_templates', [])
                    delay_days = step.get('delay_days', 0)
                    step_template_json = json.dumps(step_messages)
                    cur.execute("""
                        INSERT INTO campaign_steps (campaign_id, step_number, step_label, message_template, delay_days)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (campaign_id, step_number) DO UPDATE SET
                            step_label = EXCLUDED.step_label,
                            message_template = EXCLUDED.message_template,
                            delay_days = EXCLUDED.delay_days
                    """, (campaign_id, step_number, step_label, step_template_json, delay_days))

        conn.commit()
        return json.dumps({'success': True})
    except Exception as e:
        conn.rollback()
        return json.dumps({'error': str(e)}), 500
    finally:
        conn.close()


@app.route(
    "/api/admin/campaigns/<int:campaign_id>/flush-stale-initial-chunk",
    methods=["POST"],
)
@login_required
def admin_flush_stale_initial_chunk(campaign_id):
    """
    T10 / D3: flush manual de ``campaign_stage_sends`` initial Uazapi stale
    (``scheduled``, sem pasta, ``scheduled_for`` < now − TTL).

    Body JSON opcional: ``csrf_token`` (ou header ``X-CSRF-Token``), ``dry_run``,
    ``force`` (permite campanha fora de running/pending/completed), ``mode``
    (``recovery`` | ``mark_failed``), ``max_rows`` (1–200).
    """
    if not getattr(current_user, "is_admin", False):
        return jsonify({"error": "forbidden", "message": "Apenas administradores."}), 403
    csrf_err = _verify_json_csrf()
    if csrf_err:
        return csrf_err
    if not _admin_stale_flush_rate_allow(current_user.id):
        return (
            jsonify(
                {
                    "error": "rate_limited",
                    "message": "Muitas requisições. Tente novamente em um minuto.",
                }
            ),
            429,
        )

    data = request.get_json(silent=True) or {}
    dry_run = bool(data.get("dry_run"))
    force = bool(data.get("force"))
    mode_raw = (data.get("mode") or "recovery").strip().lower()
    if mode_raw not in ("recovery", "mark_failed"):
        return (
            jsonify(
                {
                    "error": "invalid_mode",
                    "message": "mode deve ser recovery ou mark_failed.",
                }
            ),
            400,
        )

    max_rows = data.get("max_rows")
    try:
        max_rows = int(max_rows) if max_rows is not None else None
    except (TypeError, ValueError):
        return jsonify({"error": "invalid_max_rows"}), 400
    if max_rows is None:
        max_rows = 50
    max_rows = max(1, min(max_rows, 200))

    import worker_cadence as wc

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, status, user_id, COALESCE(use_uazapi_sender, false) AS use_uazapi_sender
                FROM campaigns WHERE id = %s
                """,
                (campaign_id,),
            )
            camp = cur.fetchone()
        if not camp:
            return jsonify({"error": "not_found", "message": "Campanha não encontrada."}), 404
        if not camp.get("use_uazapi_sender"):
            return (
                jsonify(
                    {
                        "error": "not_uazapi",
                        "message": "Campanha não usa envio Uazapi.",
                    }
                ),
                400,
            )
        eligible = camp["status"] in ("running", "pending", "completed")
        if not eligible and not force:
            return (
                jsonify(
                    {
                        "error": "campaign_not_eligible",
                        "message": "Só campanhas running, pending ou completed; use force=true se necessário.",
                        "status": camp["status"],
                    }
                ),
                400,
            )

        stats = wc._recover_stale_scheduled_initial_uazapi_sends(
            conn,
            only_campaign_id=campaign_id,
            respect_recovery_env=False,
            dry_run=dry_run,
            return_stats=True,
            force_any_campaign_status=bool(force),
            recovery_mode=mode_raw,
            max_rows_override=max_rows,
        )
        if stats is None:
            stats = {}

        bumped = list(stats.get("bumped_send_ids") or [])
        failed = list(stats.get("failed_send_ids") or [])
        dry_ids = list(stats.get("dry_run_stale_send_ids") or [])
        updated = len(bumped) + len(failed)

        extra_audit = {
            "skipped_disabled": stats.get("skipped_disabled"),
            "ttl_minutes_env": os.environ.get("UAZAPI_STALE_RECOVERY_TTL_MINUTES", "90"),
            "campaign_owner_user_id": camp.get("user_id"),
        }
        if stats.get("error"):
            extra_audit["worker_error"] = stats["error"]

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO admin_uazapi_stale_flush_audit
                (admin_user_id, campaign_id, dry_run, recovery_mode, force_any_campaign_status,
                 bumped_send_ids, failed_send_ids, dry_run_stale_send_ids, extra)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                """,
                (
                    current_user.id,
                    campaign_id,
                    dry_run,
                    mode_raw,
                    bool(force),
                    bumped,
                    failed,
                    dry_ids,
                    json.dumps(extra_audit),
                ),
            )
        conn.commit()

        return jsonify(
            {
                "ok": True,
                "campaign_id": campaign_id,
                "dry_run": dry_run,
                "mode": mode_raw,
                "updated": updated,
                "bumped_send_ids": bumped,
                "failed_send_ids": failed,
                "dry_run_stale_send_ids": dry_ids if dry_run else [],
                "next_scheduled_for": None,
            }
        )
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[admin_flush_stale] {e}")
        return jsonify({"error": "server_error", "message": str(e)}), 500
    finally:
        conn.close()


def _uazapi_instance_status_summary(payload):
    """Retorna (label, code) para a UI: connected|disconnected|unknown."""
    if not payload or not isinstance(payload, dict):
        return "unknown", None
    inst = payload.get("instance")
    if isinstance(inst, dict):
        st = (inst.get("status") or inst.get("state") or "").lower()
    else:
        st = (payload.get("status") or payload.get("state") or "").lower()
    if st in ("disconnected", "close", "closed", "logout", "offline"):
        return "disconnected", st or None
    if st in ("connected", "open", "ready", "synchronized"):
        return "connected", st or None
    if st in ("connecting",):
        return "connecting", st or None
    return "unknown", st or None


@app.route("/api/admin/campaigns/force-initial-chunk", methods=["POST"])
@login_required
@admin_required
def admin_force_initial_chunk():
    """
    Admin: verifica get_status das instâncias Uazapi vinculadas e chama
    ``_continue_initial_chunk_core`` (cancel_scheduled opcional) para forçar novo chunk
    com leads pendentes ainda sem envio na etapa inicial.
    """
    csrf_err = _verify_json_csrf()
    if csrf_err:
        return csrf_err
    data = request.get_json(silent=True) or {}
    try:
        campaign_id = int(data.get("campaign_id") or 0)
    except (TypeError, ValueError):
        campaign_id = 0
    if campaign_id < 1:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "invalid_campaign_id",
                    "message": "Informe o ID numérico da campanha.",
                }
            ),
            400,
        )
    cancel_scheduled = data.get("cancel_scheduled", True) is not False
    confirm_name = (data.get("confirm_name") or "").strip()

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT c.id, c.name, c.user_id FROM campaigns c WHERE c.id = %s",
                (campaign_id,),
            )
            camp_row = cur.fetchone()
    finally:
        conn.close()

    if not camp_row:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "not_found",
                    "message": "Campanha não encontrada.",
                }
            ),
            404,
        )
    if confirm_name and (camp_row.get("name") or "").strip() != confirm_name:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "name_mismatch",
                    "message": "O nome completo da campanha não confere (confirmação de segurança).",
                }
            ),
            400,
        )

    uazapi = UazapiService()
    instance_checks = []
    conn2 = get_db_connection()
    try:
        with conn2.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT i.id AS instance_id, i.name AS instance_name, i.apikey
                FROM campaign_instances ci
                JOIN instances i ON i.id = ci.instance_id
                WHERE ci.campaign_id = %s
                  AND COALESCE(i.api_provider, 'megaapi') = 'uazapi'
                  AND i.apikey IS NOT NULL
                ORDER BY i.id ASC
                """,
                (campaign_id,),
            )
            insts = cur.fetchall() or []
    finally:
        conn2.close()
    for ins in insts:
        token = (ins.get("apikey") or "").strip()
        st_payload = uazapi.get_status(token) if token else None
        label, code = _uazapi_instance_status_summary(st_payload)
        instance_checks.append(
            {
                "instance_id": ins.get("instance_id"),
                "instance_name": ins.get("instance_name"),
                "connection": label,
                "status_code": code,
            }
        )

    owner_id = int(camp_row["user_id"])
    r = _continue_initial_chunk_core(
        campaign_id,
        owner_id,
        log_label="admin-force-initial-chunk",
        cancel_scheduled=cancel_scheduled,
    )
    body = r.get("body") or {}
    ok = bool(r.get("ok"))
    sc = int(r.get("status_code") or 500)
    print(
        f"[admin-force-initial-chunk] campaign_id={campaign_id} admin_user={current_user.id} "
        f"ok={ok} http={sc} cancel_scheduled={cancel_scheduled} result_keys={list(body.keys()) if isinstance(body, dict) else 'n/a'}"
    )
    return (
        jsonify(
            {
                "ok": ok,
                "status_code": sc,
                "campaign_id": campaign_id,
                "campaign_name": camp_row.get("name"),
                "cancel_scheduled": cancel_scheduled,
                "instance_status": instance_checks,
                "result": body,
            }
        ),
        sc,
    )


@app.route('/api/admin/campaigns/<int:campaign_id>/leads', methods=['GET'])
@login_required
@admin_required
def admin_get_campaign_leads(campaign_id):
    """Leads paginados de qualquer campanha (admin, sem filtro user_id).

    Inclui ``status`` bruto de ``campaign_leads`` e ``ui_send_status`` derivado no servidor
    (inferência limitada à BD: ``campaign_leads`` + EXISTS em ``campaign_message_outbox`` com
    ``status='sent'`` + sinais ``last_sent_stage`` / ``last_message_sent_at``). O filtro
    ``?status=`` continua a aplicar-se só à coluna ``campaign_leads.status``. Sem inferência
    a partir de agregados UAZAPI/listfolders nesta rota.
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM campaigns WHERE id = %s", (campaign_id,))
            if not cur.fetchone():
                return json.dumps({'error': 'Campanha não encontrada'}), 404

        page = request.args.get('page', 1, type=int)
        per_page = 50
        offset = (page - 1) * per_page

        name_filter = request.args.get('name', '')
        phone_filter = request.args.get('phone', '')
        status_filter = request.args.get('status', '')

        outbox_sent_expr = sql_expr_campaign_lead_has_outbox_sent("campaign_leads")

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            base_query = "FROM campaign_leads WHERE campaign_id = %s"
            params = [campaign_id]

            if name_filter:
                base_query += " AND name ILIKE %s"
                params.append(f"%{name_filter}%")
            if phone_filter:
                base_query += " AND phone ILIKE %s"
                params.append(f"%{phone_filter}%")
            if status_filter:
                base_query += " AND status = %s"
                params.append(status_filter)

            cur.execute(f"SELECT COUNT(*) as count {base_query}", tuple(params))
            total = cur.fetchone()['count']

            query = f"""
                SELECT id, phone, name, whatsapp_link, status, log, sent_at,
                       last_sent_stage, last_message_sent_at, current_step, cadence_status,
                       ({outbox_sent_expr}) AS outbox_has_sent
                {base_query}
                ORDER BY COALESCE(csv_row_order, id) ASC, id ASC
                LIMIT %s OFFSET %s
            """
            params.extend([per_page, offset])
            cur.execute(query, tuple(params))
            leads = cur.fetchall()

        serialized_leads = []
        for l in leads:
            row = dict(l)
            row["sent_at"] = row["sent_at"].isoformat() if row.get("sent_at") else None
            row["last_message_sent_at"] = (
                row["last_message_sent_at"].isoformat() if row.get("last_message_sent_at") else None
            )
            row["ui_send_status"] = compute_ui_send_status_for_lead_row(l)
            row.pop("outbox_has_sent", None)
            serialized_leads.append(row)

        return json.dumps({
            'leads': serialized_leads,
            'total': total,
            'page': page,
            'pages': (total + per_page - 1) // per_page
        }, default=str)
    except Exception as e:
        return json.dumps({'error': str(e)}), 500
    finally:
        conn.close()


@app.route('/admin/users')
@login_required
@admin_required
def admin_users():
    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Buscar usuários com info de licença - usando JOIN LATERAL ou DISTINCT ON
        # A duplicação ocorria pois usuários tinham multiplas instancias. Vamos pegar a mais recente.
        cur.execute("""
            SELECT u.id, u.email, u.is_admin, u.created_at,
                   l.license_type, l.status as license_status, l.expires_at,
                   i.name as instance_name, i.status as instance_status, i.apikey as instance_apikey
            FROM users u
            LEFT JOIN (
                SELECT DISTINCT ON (user_id) *
                FROM licenses
                ORDER BY user_id, created_at DESC
            ) l ON u.id = l.user_id
            LEFT JOIN (
                SELECT DISTINCT ON (user_id) *
                FROM instances
                ORDER BY user_id, updated_at DESC
            ) i ON u.id = i.user_id
            ORDER BY u.created_at DESC
        """)
        users = cur.fetchall()
    conn.close()
    return render_template('admin/users.html', users=users)


@app.route('/api/admin/users/list')
@login_required
@admin_required
def admin_users_list_api():
    """Superadmin: lista usuários com licença ativa ou instância vinculada (dropdown cascata)."""
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT u.id, u.email
                FROM users u
                WHERE EXISTS (
                    SELECT 1 FROM licenses l WHERE l.user_id = u.id AND l.status = 'active' AND (l.expires_at IS NULL OR l.expires_at > NOW())
                ) OR EXISTS (
                    SELECT 1 FROM instances i WHERE i.user_id = u.id
                )
                ORDER BY u.email ASC
            """)
            users = cur.fetchall()
        conn.close()
        return json.dumps([
            {'id': u['id'], 'email': u['email'], 'daily_limit': get_user_daily_limit(u['id'])}
            for u in users
        ], default=str)
    except Exception as e:
        return json.dumps({'error': str(e)}), 500


@app.route('/api/admin/users/<int:user_id>/instances')
@login_required
@admin_required
def admin_user_instances_api(user_id):
    """Superadmin: instâncias Uazapi de um usuário (dropdown cascata)."""
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, name, status, COALESCE(api_provider, 'megaapi') as api_provider
                FROM instances
                WHERE user_id = %s AND COALESCE(api_provider, 'megaapi') = 'uazapi'
                ORDER BY id ASC
            """, (user_id,))
            instances = cur.fetchall()
        conn.close()
        return json.dumps([dict(i) for i in instances], default=str)
    except Exception as e:
        return json.dumps({'error': str(e)}), 500


@app.route('/api/admin/users/<int:user_id>/scraping-jobs')
@login_required
@admin_required
def admin_user_scraping_jobs_api(user_id):
    """Superadmin: scraping jobs completados de um usuário (dropdown cascata)."""
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, keyword, locations, total_results, lead_count, results_path, created_at
                FROM scraping_jobs
                WHERE user_id = %s AND status = 'completed'
                ORDER BY created_at DESC
            """, (user_id,))
            jobs = cur.fetchall()
        conn.close()
        result = []
        for j in jobs:
            d = dict(j, created_at=j['created_at'].isoformat())
            d['has_csv'] = bool(j.get('results_path') and os.path.exists(j['results_path']))
            d.pop('results_path', None)
            result.append(d)
        return json.dumps(result, default=str)
    except Exception as e:
        return json.dumps({'error': str(e)}), 500


@app.route('/api/admin/scraping-jobs/<int:job_id>/download-csv')
@login_required
@admin_required
def admin_download_job_csv(job_id):
    """Superadmin: download do CSV de um scraping job."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT results_path FROM scraping_jobs WHERE id = %s", (job_id,))
            job = cur.fetchone()

        if not job or not job.get('results_path'):
            return jsonify(error='Job não encontrado ou sem arquivo de resultados'), 404

        file_path = job['results_path']
        if not os.path.exists(file_path):
            return jsonify(error='Arquivo CSV não encontrado no servidor'), 404

        return send_file(file_path, as_attachment=True, download_name=os.path.basename(file_path))
    except Exception as e:
        return jsonify(error=str(e)), 500
    finally:
        if conn:
            conn.close()


@app.route('/admin/users/<int:user_id>/toggle_admin', methods=['POST'])
@login_required
@admin_required
def admin_toggle_admin(user_id):
    if user_id == current_user.id:
        flash("Você não pode alterar seu próprio status de admin.", "error")
        return redirect(url_for('admin_users'))

    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("UPDATE users SET is_admin = NOT is_admin WHERE id = %s", (user_id,))
    conn.commit()
    conn.close()
    
    flash("Status de admin atualizado com sucesso!", "success")
    return redirect(url_for('admin_users'))

@app.route('/admin/licenses/create', methods=['POST'])
@login_required
@admin_required
def admin_create_license():
    user_id = request.form.get('user_id')
    license_type_input = (request.form.get('license_type') or '').strip().lower()
    license_type = resolve_license_type(license_type_input, allow_legacy_fallback=False)
    
    if not user_id or not license_type_input:
        flash("Dados inválidos.", "error")
        return redirect(url_for('admin_users'))

    if not license_type or license_type not in ACTIVE_LICENSE_TYPES:
        allowed_plans = ", ".join(ACTIVE_LICENSE_TYPES)
        flash(f"Plano inválido: '{license_type_input}'. Use apenas: {allowed_plans}.", "error")
        return redirect(url_for('admin_users'))
        
    # Validar user_id
    user = User.get_by_id(user_id)
    if not user:
        flash("Usuário não encontrado.", "error")
        return redirect(url_for('admin_users'))

    try:
        # Criar licença manual
        import datetime
        from datetime import datetime
        
        # Revogar licenças anteriores se houver
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("UPDATE licenses SET status = 'cancelled' WHERE user_id = %s", (user.id,))
        conn.commit()
        conn.close()

        # Gerar IDs fictícios para compra manual
        purchase_id = f"MANUAL-{secrets.token_hex(8)}"
        product_id = "MANUAL-GRANT"
        purchase_date = datetime.utcnow().isoformat()
        
        License.create(user.id, purchase_id, product_id, license_type, purchase_date)
        
        flash(f"Plano {license_type} definido para {user.email}.", "success")
    except Exception as e:
        print(f"Erro ao criar licença manual: {e}")
        flash("Erro ao criar licença.", "error")
        
    return redirect(url_for('admin_users'))

@app.route('/admin/licenses/<int:license_id>/revoke', methods=['POST'])
@login_required
@admin_required
def admin_revoke_license(license_id):
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("UPDATE licenses SET status = 'cancelled' WHERE id = %s", (license_id,))
    conn.commit()
    conn.close()
    
    flash("Licença revogada com sucesso!", "success")
    return redirect(url_for('admin_users'))

@app.route('/admin/users/<int:user_id>/reset_password', methods=['POST'])
@login_required
@admin_required
def admin_reset_password(user_id):
    user = User.get_by_id(user_id)
    if not user:
        return {"error": "User not found"}, 404
        
    try:
        # Gerar nova senha
        new_password = secrets.token_urlsafe(10)
        password_hash = generate_password_hash(new_password)
        
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET password_hash = %s WHERE id = %s", (password_hash, user_id))
        conn.commit()
        conn.close()
        
        # TODO: Enviar email via SMTP (Simulado no print por enquanto)
        print(f"PASSWORD RESET FOR {user.email}: {new_password}")
        
        # Em produção, usar Flask-Mail aqui
        # msg = Message("Sua nova senha - Leads Infinitos", recipients=[user.email])
        # msg.body = f"Sua senha foi resetada. Nova senha: {new_password}"
        # mail.send(msg)
        
        return {"success": True, "message": "Senha resetada e enviada por email (simulado).", "new_password": new_password}
    except Exception as e:
        print(f"Erro no reset de senha: {e}")
        return {"error": str(e)}, 500

@app.route('/admin/users/<int:user_id>/delete', methods=['POST'])
@login_required
@admin_required
def admin_delete_user(user_id):
    if user_id == current_user.id:
        return {"error": "Você não pode excluir a si mesmo."}, 400
        
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Excluir dependências MANUALMENTE (Cascata)
            
            # 1. Obter IDs das campanhas do usuário
            cur.execute("SELECT id FROM campaigns WHERE user_id = %s", (user_id,))
            campaign_ids = [row[0] for row in cur.fetchall()]
            
            if campaign_ids:
                # 2. Excluir leads das campanhas
                cur.execute("DELETE FROM campaign_leads WHERE campaign_id = ANY(%s)", (campaign_ids,))
                # Excluir steps das campanhas (se existir tabela, garantindo limpeza)
                # cur.execute("DELETE FROM campaign_steps WHERE campaign_id = ANY(%s)", (campaign_ids,))
            
            # 3. Excluir campanhas
            cur.execute("DELETE FROM campaigns WHERE user_id = %s", (user_id,))
            
            # 4. Outras dependências diretas
            cur.execute("DELETE FROM licenses WHERE user_id = %s", (user_id,))
            cur.execute("DELETE FROM instances WHERE user_id = %s", (user_id,))
            cur.execute("DELETE FROM scraping_jobs WHERE user_id = %s", (user_id,))
            cur.execute("DELETE FROM password_resets WHERE user_id = %s", (user_id,))
            # Fix: monthly_usage_history and message_templates constraints
            cur.execute("DELETE FROM monthly_usage_history WHERE user_id = %s", (user_id,))
            cur.execute("DELETE FROM message_templates WHERE user_id = %s", (user_id,))
            
            # 5. Excluir o usuário
            cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
            
        conn.commit()
        conn.close()
        return {"success": True}
    except Exception as e:
        print(f"Erro ao excluir usuário: {e}")
        conn.rollback() if 'conn' in locals() and conn else None
        return {"error": str(e)}, 500

@app.route('/admin/users/<int:user_id>/details')
@login_required
@admin_required
def admin_user_details(user_id):
    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Info básica do usuário.
        cur.execute("""
            SELECT u.id, u.email, u.created_at, u.is_admin
            FROM users u
            WHERE u.id = %s
        """, (user_id,))
        user = cur.fetchone()
        
        if not user:
            conn.close()
            return {"error": "User not found"}, 404
            
        # Info licença
        cur.execute("""
            SELECT * FROM licenses 
            WHERE user_id = %s AND status = 'active' 
            ORDER BY created_at DESC LIMIT 1
        """, (user_id,))
        license = cur.fetchone()

        # Lista completa de instâncias (Task 9)
        cur.execute(
            """
            SELECT id, name, status, apikey, COALESCE(api_provider, 'megaapi') AS api_provider,
                   daily_sends_per_instance, updated_at
            FROM instances
            WHERE user_id = %s
            ORDER BY id ASC
            """,
            (user_id,),
        )
        instances = cur.fetchall() or []
        
    conn.close()
    
    return {
        "user": {
            "id": user['id'],
            "email": user['email'],
            "created_at": user['created_at'].isoformat() if user['created_at'] else None,
            "is_admin": user['is_admin'],
        },
        "license": {
            "type": license['license_type'] if license else None,
            "expires_at": license['expires_at'].isoformat() if license and license['expires_at'] else None
        } if license else None,
        "instances": [
            {
                "id": inst["id"],
                "name": inst["name"],
                "status": inst["status"],
                "apikey": inst["apikey"],
                "api_provider": inst["api_provider"],
                "daily_sends_per_instance": inst["daily_sends_per_instance"],
                "updated_at": inst["updated_at"].isoformat() if inst.get("updated_at") else None,
            }
            for inst in instances
        ],
    }


@app.route('/admin/users/<int:user_id>/instances', methods=['POST'])
@login_required
@admin_required
def admin_add_user_instance(user_id):
    data = request.get_json(silent=True) or request.form or {}
    raw_name = (data.get("instance_name") or "").strip()
    safe_name = "".join(c for c in raw_name if c.isalnum() or c in ('-', '_'))
    if not safe_name:
        safe_name = f"instance_{user_id}_{int(time.time())}"

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            snapshot = _get_user_plan_snapshot_for_limit(cur, user_id)
            if not snapshot:
                conn.rollback()
                return {"error": "Usuário não encontrado."}, 404

            if snapshot["current_instances"] >= snapshot["instance_limit"]:
                conn.rollback()
                return {"error": INSTANCE_LIMIT_REACHED_MESSAGE}, 400

            uazapi = UazapiService()
            result = uazapi.create_instance(safe_name)
            if not result:
                conn.rollback()
                return {"error": "Falha ao criar instância na Uazapi."}, 500

            instance_key = result.get('token') or (result.get('instance') or {}).get('token')
            if not instance_key:
                conn.rollback()
                return {"error": "Falha ao obter token da instância. Resposta da API inválida."}, 500

            cur.execute(
                """
                INSERT INTO instances (user_id, name, apikey, status, api_provider)
                VALUES (%s, %s, %s, 'disconnected', 'uazapi')
                RETURNING id, name, status, apikey, api_provider
                """,
                (user_id, safe_name, instance_key),
            )
            created = cur.fetchone()

        conn.commit()
        return {
            "success": True,
            "instance": {
                "id": created["id"],
                "name": created["name"],
                "status": created["status"],
                "apikey": created["apikey"],
                "api_provider": created["api_provider"],
            }
        }
    except Exception as e:
        conn.rollback()
        print(f"Erro ao adicionar instância para usuário {user_id}: {e}")
        return {"error": "Erro ao adicionar instância via Uazapi."}, 500
    finally:
        conn.close()


@app.route('/admin/users/<int:user_id>/instances/<int:instance_id>/token', methods=['POST'])
@login_required
@admin_required
def admin_set_instance_uazapi_token(user_id, instance_id):
    """
    Define ou substitui o token Uazapi de uma instância já existente no Leads Infinitos.
    Útil quando a instância foi criada sem token válido / precisa religar token da Uazapi.
    """
    data = request.get_json(silent=True) or request.form or {}
    token = (data.get("token") or data.get("apikey") or "").strip()
    if not token:
        return {"error": "Informe o token da instância Uazapi."}, 400
    if len(token) < 8:
        return {"error": "Token inválido (muito curto)."}, 400

    provider = (data.get("api_provider") or "uazapi").strip().lower() or "uazapi"
    if provider != "uazapi":
        return {"error": "Apenas api_provider=uazapi é suportado neste endpoint."}, 400

    new_status = None
    remote_jid = None
    try:
        uazapi = UazapiService()
        result = uazapi.get_status(token)
        if result:
            instance_data = result.get("instance") or result
            status_val = (
                instance_data.get("status", "disconnected")
                if isinstance(instance_data, dict)
                else "disconnected"
            )
            new_status = (
                status_val
                if status_val in ("connected", "connecting", "disconnected")
                else "disconnected"
            )
            if isinstance(result, dict):
                remote_jid = result.get("id") or result.get("me")
                if not remote_jid and result.get("instance_data"):
                    remote_jid = (
                        result["instance_data"].get("phone")
                        or result["instance_data"].get("user")
                        or result["instance_data"].get("jid")
                    )
    except Exception as e:
        print(f"[admin_set_instance_uazapi_token] get_status falhou (segue salvando): {e}")

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, name, status, apikey, COALESCE(api_provider, 'megaapi') AS api_provider
                FROM instances
                WHERE id = %s AND user_id = %s
                FOR UPDATE
                """,
                (instance_id, user_id),
            )
            row = cur.fetchone()
            if not row:
                conn.rollback()
                return {"error": "Instância não encontrada para este usuário."}, 404

            if new_status:
                cur.execute(
                    """
                    UPDATE instances
                    SET apikey = %s,
                        api_provider = 'uazapi',
                        status = %s
                    WHERE id = %s AND user_id = %s
                    RETURNING id, name, status, apikey, COALESCE(api_provider, 'uazapi') AS api_provider
                    """,
                    (token, new_status, instance_id, user_id),
                )
            else:
                cur.execute(
                    """
                    UPDATE instances
                    SET apikey = %s,
                        api_provider = 'uazapi'
                    WHERE id = %s AND user_id = %s
                    RETURNING id, name, status, apikey, COALESCE(api_provider, 'uazapi') AS api_provider
                    """,
                    (token, instance_id, user_id),
                )
            updated = cur.fetchone()
        conn.commit()
        return {
            "success": True,
            "instance": {
                "id": updated["id"],
                "name": updated["name"],
                "status": updated["status"],
                "apikey": updated["apikey"],
                "api_provider": updated["api_provider"],
            },
            "remote_jid": remote_jid,
            "status_checked": bool(new_status),
        }
    except Exception as e:
        conn.rollback()
        print(f"Erro ao salvar token Uazapi instance={instance_id} user={user_id}: {e}")
        return {"error": "Erro ao salvar token da instância."}, 500
    finally:
        conn.close()


@app.route('/admin/whatsapp/check_status/<instance_apikey>', methods=['POST'])
@login_required
@admin_required
def admin_check_whatsapp_status(instance_apikey):
    """Admin endpoint to check/update status of a whatsapp instance"""
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(api_provider, 'megaapi') as api_provider FROM instances WHERE apikey = %s", (instance_apikey,))
        prov_row = cur.fetchone()
    conn.close()
    
    if not prov_row:
        return {"error": "Instance not found"}, 404
    api_provider = prov_row[0]
    
    if api_provider == 'uazapi':
        uazapi = UazapiService()
        result = uazapi.get_status(instance_apikey)
        if not result:
            return {"error": "Failed to verify status"}, 400
        instance_data = result.get('instance') or result
        status_val = instance_data.get('status', 'disconnected') if isinstance(instance_data, dict) else 'disconnected'
        new_status = status_val if status_val in ('connected', 'connecting', 'disconnected') else 'disconnected'
        remote_jid = None
        if isinstance(result, dict):
            remote_jid = result.get('id') or result.get('me')
            if not remote_jid and result.get('instance_data'):
                remote_jid = result['instance_data'].get('phone') or result['instance_data'].get('user') or result['instance_data'].get('jid')
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("UPDATE instances SET status = %s WHERE apikey = %s", (new_status, instance_apikey))
        conn.commit()
        conn.close()
        print(f"Admin Checked Status for {instance_apikey}: {new_status} (Uazapi)")
        return {"status": new_status, "result": result, "remote_jid": remote_jid}

    return (
        json.dumps({"error": "Instância legada. Crie uma nova instância Uazapi."}),
        400,
        {"Content-Type": "application/json"},
    )


@app.route('/admin/users/create', methods=['POST'])
@login_required
@admin_required
def admin_create_user():
    data = request.json
    email = data.get('email')
    password = data.get('password')
    instance_name = data.get('instance_name')
    
    if not email or not password:
        return {"error": "Email e senha são obrigatórios"}, 400
        
    # Check if user exists
    user = User.get_by_email(email)
    if user:
        return {"error": "Email já cadastrado"}, 400
        
    try:
        # 1. Create User
        user = User.create(email, password)
        
        # 2. Create Instance (Optional)
        if instance_name:
            safe_name = "".join(c for c in instance_name if c.isalnum() or c in ('-', '_'))
            if safe_name:
                conn = get_db_connection()
                try:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        snapshot = _get_user_plan_snapshot_for_limit(cur, user.id)
                        if not snapshot:
                            conn.rollback()
                            return {"error": "Usuário não encontrado após criação."}, 404
                        if snapshot["current_instances"] >= snapshot["instance_limit"]:
                            conn.rollback()
                            return {"error": INSTANCE_LIMIT_REACHED_MESSAGE}, 400

                        uazapi = UazapiService()
                        result = uazapi.create_instance(safe_name)
                        if not result:
                            conn.rollback()
                            return {"error": "Falha ao criar instância na Uazapi."}, 500

                        instance_key = result.get('token') or (result.get('instance') or {}).get('token')
                        if not instance_key:
                            conn.rollback()
                            return {"error": "Falha ao obter token da instância. Resposta da API inválida."}, 500

                        cur.execute(
                            """
                            INSERT INTO instances (user_id, name, apikey, status, api_provider)
                            VALUES (%s, %s, %s, 'disconnected', 'uazapi')
                            """,
                            (user.id, safe_name, instance_key)
                        )
                    conn.commit()
                    print(f"✅ Instância Uazapi {safe_name} criada para usuário {user.id}")
                finally:
                    conn.close()

        return {"success": True}
    except Exception as e:
        return {"error": str(e)}, 500


@app.route('/admin/users/<int:user_id>/update', methods=['POST'])
@login_required
@admin_required
def admin_update_user(user_id):
    data = request.json
    email = data.get('email')
    password = data.get('password') # Optional
    
    if not email:
        return {"error": "Email é obrigatório"}, 400
        
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Check for email conflict
            cur.execute("SELECT id FROM users WHERE email = %s AND id != %s", (email, user_id))
            if cur.fetchone():
                return {"error": "Email já está em uso por outro usuário"}, 400
            
            # Update Email
            cur.execute("UPDATE users SET email = %s WHERE id = %s", (email, user_id))
            
            # Update Password if provided
            if password:
                password_hash = generate_password_hash(password)
                cur.execute("UPDATE users SET password_hash = %s WHERE id = %s", (password_hash, user_id))
                
        conn.commit()
        return {"success": True}
    except Exception as e:
        conn.rollback()
        return {"error": str(e)}, 500
    finally:
        conn.close()
@app.route('/campaigns/new')
@login_required
def new_campaign():
    # Fetch user's instances for multi-instance selection
    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id, name, status, COALESCE(api_provider, 'megaapi') as api_provider FROM instances WHERE user_id = %s ORDER BY id ASC", (current_user.id,))
        user_instances = cur.fetchall()
    conn.close()
    
    plan_daily_limit = get_user_daily_limit(current_user.id)
    
    return render_template('campaigns_new.html',
                           instances=user_instances,
                           is_super_admin=is_super_admin(),
                           plan_daily_limit=plan_daily_limit,
                           use_message_outbox=USE_MESSAGE_OUTBOX)

@app.route('/api/scraping-jobs')
@login_required
def api_scraping_jobs():
    """Retorna jobs completados para o select na UI de Campanhas"""
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, keyword, locations, total_results, lead_count, created_at 
                FROM scraping_jobs 
                WHERE user_id = %s AND status = 'completed' 
                ORDER BY created_at DESC
                """,
                (current_user.id,)
            )
            jobs = cur.fetchall()
        conn.close()
        return json.dumps([dict(j, created_at=j['created_at'].isoformat()) for j in jobs], default=str)
    except Exception as e:
        return json.dumps({'error': str(e)}), 500

def _campaign_has_message_outbox_rows(campaign_id: int) -> bool:
    """True se a campanha usa fila ``campaign_message_outbox`` (envio /send/text)."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM campaign_message_outbox WHERE campaign_id = %s LIMIT 1",
                (campaign_id,),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def _campaign_has_chunk_stage_sends(campaign_id: int) -> bool:
    """True se há chunks Uazapi em ``campaign_stage_sends`` (legado por pasta, com ou sem cadência UI)."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM campaign_stage_sends WHERE campaign_id = %s LIMIT 1",
                (campaign_id,),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def _uazapi_resume_flexible_folder_continue(
    campaign_id: int,
    user_id: int,
    *,
    folder_id: Optional[str],
    admin_mode: bool,
    trigger_initial_chunk: bool,
    log_label: str,
    extra: dict,
) -> tuple[bool, Optional[str], dict]:
    """
    Retoma no BD; envio initial via outbox (sem ``continue`` na pasta legada).
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(_sql_resume_running_clear_system_disconnect_pause(), (campaign_id,))
            if cur.rowcount == 0:
                conn.rollback()
                return False, "Estado da campanha mudou; recarregue.", extra
        conn.commit()
    finally:
        conn.close()

    if trigger_initial_chunk:
        _unlock_initial_chunks_before_force(campaign_id, f"{log_label}-pre-chunk")
        cres = _continue_initial_chunk_core(
            campaign_id,
            user_id,
            log_label=log_label,
            cancel_scheduled=True,
        )
        extra["initial_chunk"] = _initial_chunk_outbox_extra_from_result(cres)
    return True, None, extra


def _uazapi_control_folder(campaign_id: int, user_id: int, folder_id: str, action: str):
    """
    Helper: executa action (stop|continue) em um folder_id Uazapi.
    Usado para rollover follow-ups. Retorna (success, error_msg).
    """
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id FROM campaigns WHERE id = %s AND user_id = %s",
                (campaign_id, user_id)
            )
            if not cur.fetchone():
                return False, "Campanha não encontrada"
            cur.execute("""
                SELECT i.apikey FROM campaign_instances ci
                JOIN instances i ON i.id = ci.instance_id
                WHERE ci.campaign_id = %s AND COALESCE(i.api_provider, 'megaapi') = 'uazapi'
                LIMIT 1
            """, (campaign_id,))
            inst = cur.fetchone()
        conn.close()
        if not inst or not inst.get('apikey'):
            return False, "Instância Uazapi não encontrada"
        uazapi = UazapiService()
        result = uazapi.edit_campaign(inst['apikey'], folder_id, action)
        return bool(result), None if result else "Falha ao comunicar com API Uazapi"
    except Exception as e:
        return False, str(e)


def _resume_campaign_after_pause(
    campaign_id: int,
    user_id: int,
    *,
    admin_mode: bool = False,
    trigger_initial_chunk: bool = True,
) -> tuple[bool, Optional[str], dict]:
    """
    Retoma campanha ``paused`` → ``running``.

    Uazapi com outbox/cadência/chunks: retoma no BD e enfileira initial via outbox
    (sem ``continue`` na pasta legada ``uazapi_folder_id``).

    Campanha pasta única legada (sem outbox/chunks/cadência): exige ``edit_campaign`` continue.
    """
    conn = get_db_connection()
    extra: dict = {}
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if admin_mode:
                cur.execute(
                    """
                    SELECT id, status, use_uazapi_sender, uazapi_folder_id, enable_cadence
                    FROM campaigns WHERE id = %s
                    """,
                    (campaign_id,),
                )
            else:
                cur.execute(
                    """
                    SELECT id, status, use_uazapi_sender, uazapi_folder_id, enable_cadence
                    FROM campaigns WHERE id = %s AND user_id = %s
                    """,
                    (campaign_id, user_id),
                )
            campaign = cur.fetchone()
        if not campaign:
            return False, "Campanha não encontrada", extra
        if (campaign.get("status") or "").strip() != "paused":
            return False, "Só é possível retomar campanha pausada.", extra

        use_uaz = bool(campaign.get("use_uazapi_sender"))
        folder_id = campaign.get("uazapi_folder_id")
        enable_cadence = bool(campaign.get("enable_cadence"))
        has_outbox = _campaign_has_message_outbox_rows(campaign_id)
        has_chunks = _campaign_has_chunk_stage_sends(campaign_id)

        if use_uaz and has_outbox:
            with conn.cursor() as cur:
                cur.execute(_sql_resume_running_clear_system_disconnect_pause(), (campaign_id,))
                if cur.rowcount == 0:
                    conn.rollback()
                    return False, "Estado da campanha mudou; recarregue.", extra
            conn.commit()
            if trigger_initial_chunk:
                _unlock_initial_chunks_before_force(campaign_id, "resume-outbox-pre-chunk")
                cres = _continue_initial_chunk_core(
                    campaign_id,
                    user_id,
                    log_label="resume-outbox+initial-chunk",
                    cancel_scheduled=True,
                )
                extra["initial_chunk"] = _initial_chunk_outbox_extra_from_result(cres)
            return True, None, extra

        if use_uaz and (enable_cadence or has_chunks):
            return _uazapi_resume_flexible_folder_continue(
                campaign_id,
                user_id,
                folder_id=folder_id,
                admin_mode=admin_mode,
                trigger_initial_chunk=trigger_initial_chunk,
                log_label="resume-chunks+initial-chunk"
                if has_chunks and not enable_cadence
                else "resume-cadence+initial-chunk",
                extra=extra,
            )

        if use_uaz and folder_id:
            success, err = _uazapi_control_campaign(
                campaign_id, user_id, "continue", admin_mode=admin_mode
            )
            if not success:
                return False, err or "Falha ao comunicar com API Uazapi", extra
            return True, None, extra

        with conn.cursor() as cur:
            cur.execute(_sql_resume_running_clear_system_disconnect_pause(), (campaign_id,))
            if cur.rowcount == 0:
                conn.rollback()
                return False, "Estado da campanha mudou; recarregue.", extra
        conn.commit()
        return True, None, extra
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return False, str(e), extra
    finally:
        conn.close()


def _sql_resume_running_clear_system_disconnect_pause():
    """SET status=running e limpa metadados de pausa sistema por desconexão (Task 6)."""
    return """
        UPDATE campaigns SET
            status = 'running',
            pause_origin = CASE
                WHEN pause_origin = 'system' AND pause_reason_code = 'instance_disconnected' THEN NULL
                ELSE pause_origin END,
            pause_reason_code = CASE
                WHEN pause_origin = 'system' AND pause_reason_code = 'instance_disconnected' THEN NULL
                ELSE pause_reason_code END,
            system_paused_at = CASE
                WHEN pause_origin = 'system' AND pause_reason_code = 'instance_disconnected' THEN NULL
                ELSE system_paused_at END
        WHERE id = %s AND status = 'paused'
    """


def _truthy_confirm_resume(body) -> bool:
    if not body or not isinstance(body, dict):
        return False
    v = body.get("confirm_resume_while_disconnected")
    if v is True:
        return True
    if isinstance(v, str) and v.strip().lower() in ("1", "true", "yes", "on"):
        return True
    return False


def _guard_resume_after_whatsapp_disconnect(conn, campaign_id: int, body) -> Optional[tuple]:
    """
    Task 6: se a campanha foi pausada por ``instance_disconnected``, valida get_status
    nas instâncias Uazapi da campanha; se ainda desligado ou estado indeterminado, exige
    ``confirm_resume_while_disconnected`` no JSON.

    Retorna None se pode continuar, ou (response, status_code) para devolver imediatamente.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT pause_reason_code FROM campaigns WHERE id = %s
            """,
            (campaign_id,),
        )
        row = cur.fetchone()
    if not row:
        return (jsonify({"error": "not_found", "message": "Campanha não encontrada."}), 404)
    pr = (row.get("pause_reason_code") or "").strip()
    if pr != "instance_disconnected":
        return None

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT DISTINCT i.id AS instance_id, i.apikey
            FROM campaign_instances ci
            JOIN instances i ON i.id = ci.instance_id
            WHERE ci.campaign_id = %s
              AND COALESCE(i.api_provider, 'megaapi') = 'uazapi'
              AND i.apikey IS NOT NULL
              AND TRIM(i.apikey) <> ''
            """,
            (campaign_id,),
        )
        instances = cur.fetchall() or []

    needs_confirm = False
    if not instances:
        needs_confirm = True
    else:
        uazapi = UazapiService()
        for inst in instances:
            iid = int(inst["instance_id"])
            token = (inst.get("apikey") or "").strip()
            st = get_instance_status_cached(uazapi, iid, token)
            if st is None:
                needs_confirm = True
                break
            if is_instance_disconnected_status(st):
                needs_confirm = True
                break

    if not needs_confirm:
        return None
    if _truthy_confirm_resume(body):
        return None

    msg = (
        "A instância WhatsApp ainda parece desligada ou o estado não pôde ser verificado. "
        "Retomar agora pode gerar falhas de envio até reconectar. Confirme para continuar."
    )
    return (
        jsonify(
            {
                "error": "instance_not_ready",
                "message": msg,
                "requires_confirmation": True,
            }
        ),
        409,
    )


def _uazapi_control_campaign(campaign_id: int, user_id: int, action: str, admin_mode: bool = False):
    """
    Helper: executa action (stop|continue|delete) na Uazapi para campanha com use_uazapi_sender.
    Retorna (success, error_msg). Em caso de delete, já remove do DB.
    admin_mode: se True, não valida user_id (para admin deletar qualquer campanha).
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if admin_mode:
                cur.execute(
                    "SELECT c.id, c.use_uazapi_sender, c.uazapi_folder_id, c.user_id FROM campaigns c WHERE c.id = %s",
                    (campaign_id,)
                )
            else:
                cur.execute(
                    "SELECT c.id, c.use_uazapi_sender, c.uazapi_folder_id, c.user_id FROM campaigns c WHERE c.id = %s AND c.user_id = %s",
                    (campaign_id, user_id)
                )
            campaign = cur.fetchone()
        if not campaign or not campaign.get('use_uazapi_sender') or not campaign.get('uazapi_folder_id'):
            return False, "Campanha não usa envio Uazapi ou folder_id ausente"
        folder_id = campaign['uazapi_folder_id']
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT i.apikey FROM campaign_instances ci
                JOIN instances i ON i.id = ci.instance_id
                WHERE ci.campaign_id = %s AND COALESCE(i.api_provider, 'megaapi') = 'uazapi'
                LIMIT 1
            """, (campaign_id,))
            inst = cur.fetchone()
        conn.close()
        if not inst or not inst.get('apikey'):
            return False, "Instância Uazapi não encontrada para esta campanha"
        token = inst['apikey']
        uazapi = UazapiService()
        result = uazapi.edit_campaign(token, folder_id, action)
        if not result:
            return False, "Falha ao comunicar com API Uazapi"
        conn = get_db_connection()
        with conn.cursor() as cur:
            if action == 'stop':
                cur.execute("UPDATE campaigns SET status = 'paused' WHERE id = %s", (campaign_id,))
            elif action == 'continue':
                cur.execute(_sql_resume_running_clear_system_disconnect_pause(), (campaign_id,))
            elif action == 'delete':
                cur.execute("DELETE FROM campaign_leads WHERE campaign_id = %s", (campaign_id,))
                cur.execute("DELETE FROM campaign_instances WHERE campaign_id = %s", (campaign_id,))
                cur.execute("DELETE FROM campaigns WHERE id = %s", (campaign_id,))
        conn.commit()
        conn.close()
        return True, None
    except Exception as e:
        if conn:
            try:
                conn.rollback()
                conn.close()
            except Exception:
                pass
        return False, str(e)


@app.route('/api/campaigns/<int:campaign_id>/uazapi-control', methods=['POST'])
@login_required
def uazapi_control(campaign_id):
    """
    Controla campanha Uazapi: stop, continue ou delete.
    Body: { "action": "stop" | "continue" | "delete" }
    """
    data = request.get_json() or {}
    action = (data.get('action') or '').strip().lower()
    if action not in ('stop', 'continue', 'delete'):
        return json.dumps({'error': 'action deve ser stop, continue ou delete'}), 400
    success, err = _uazapi_control_campaign(campaign_id, current_user.id, action)
    if success:
        return json.dumps({'success': True, 'action': action})
    return json.dumps({'error': err or 'Erro ao controlar campanha'}), 500


@app.route('/api/campaigns/<int:campaign_id>/pause-rollover/<int:step>', methods=['POST'])
@login_required
def pause_rollover(campaign_id, step):
    """
    Pausa ou continua uma sub-campanha de follow-up (rollover).
    step: 1=principal (uazapi_folder_id), 2=FU1, 3=FU2, 4=Despedida
    Body: { "action": "stop" | "continue" }
    """
    data = request.get_json() or {}
    action = (data.get('action') or '').strip().lower()
    if action not in ('stop', 'continue'):
        return json.dumps({'error': 'action deve ser stop ou continue'}), 400

    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT use_uazapi_sender, uazapi_folder_id, cadence_config FROM campaigns WHERE id = %s AND user_id = %s",
            (campaign_id, current_user.id)
        )
        row = cur.fetchone()
    conn.close()
    if not row:
        return json.dumps({'error': 'Campanha não encontrada'}), 404

    folder_id = None
    if step == 1:
        if row.get('use_uazapi_sender') and row.get('uazapi_folder_id'):
            folder_id = row['uazapi_folder_id']
            success, err = _uazapi_control_campaign(campaign_id, current_user.id, action)
            if success:
                return json.dumps({'success': True, 'action': action})
            return json.dumps({'error': err or 'Erro ao controlar campanha'}), 500
        return json.dumps({'error': 'Campanha principal não usa Uazapi'}), 400

    cfg = row.get('cadence_config') or {}
    if isinstance(cfg, str):
        try:
            cfg = json.loads(cfg) if cfg else {}
        except json.JSONDecodeError:
            cfg = {}
    if step == 2:
        fu1_ids = iter_fu1_folder_ids(cfg)
        if not fu1_ids:
            return json.dumps({'error': 'Sub-campanha 2 (FU1) ainda não criada ou sem folder_id'}), 404
        last_err = None
        ok_any = False
        for fid in fu1_ids:
            success, err = _uazapi_control_folder(campaign_id, current_user.id, str(fid), action)
            if success:
                ok_any = True
            else:
                last_err = err
        if ok_any:
            return json.dumps({'success': True, 'action': action, 'folders_touched': len(fu1_ids)})
        return json.dumps({'error': last_err or 'Erro ao controlar follow-up'}), 500

    key_map = {3: 'rollover_fu2_folder_id', 4: 'rollover_fu3_folder_id'}
    folder_id = cfg.get(key_map.get(step, ''))
    if not folder_id:
        return json.dumps({'error': f'Sub-campanha {step} ainda não criada ou sem folder_id'}), 404

    success, err = _uazapi_control_folder(campaign_id, current_user.id, str(folder_id), action)
    if success:
        return json.dumps({'success': True, 'action': action})
    return json.dumps({'error': err or 'Erro ao controlar follow-up'}), 500


@app.route('/api/campaigns/<int:campaign_id>/uazapi-messages', methods=['GET'])
@login_required
def uazapi_messages(campaign_id):
    """
    Lista mensagens da campanha na Uazapi.
    Query params: messageStatus (Scheduled|Sent|Failed), page, pageSize.
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT use_uazapi_sender, uazapi_folder_id FROM campaigns WHERE id = %s AND user_id = %s",
                (campaign_id, current_user.id)
            )
            campaign = cur.fetchone()
        if not campaign or not campaign.get('use_uazapi_sender') or not campaign.get('uazapi_folder_id'):
            return json.dumps({'error': 'Campanha não usa envio Uazapi ou folder_id ausente'}), 404
        folder_id = campaign['uazapi_folder_id']
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT i.apikey FROM campaign_instances ci
                JOIN instances i ON i.id = ci.instance_id
                WHERE ci.campaign_id = %s AND COALESCE(i.api_provider, 'megaapi') = 'uazapi'
                LIMIT 1
            """, (campaign_id,))
            inst = cur.fetchone()
        conn.close()
        if not inst or not inst.get('apikey'):
            return json.dumps({'error': 'Instância Uazapi não encontrada para esta campanha'}), 404
        token = inst['apikey']
        message_status = request.args.get('messageStatus')
        page = request.args.get('page', type=int)
        page_size = request.args.get('pageSize', type=int)
        uazapi = UazapiService()
        result = uazapi.list_messages(
            token, folder_id,
            message_status=message_status if message_status else None,
            page=page if page is not None else None,
            page_size=page_size if page_size is not None else None
        )
        if result is None:
            return json.dumps({'error': 'Falha ao obter mensagens da Uazapi'}), 500
        return json.dumps(result, default=str)
    except Exception as e:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        print(f"Erro ao listar mensagens Uazapi: {e}")
        return json.dumps({'error': str(e)}), 500


@app.route('/api/campaigns/<int:campaign_id>/toggle_pause', methods=['POST'])
@login_required
def toggle_campaign_pause(campaign_id):
    try:
        data = request.get_json(silent=True) or {}
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT status, use_uazapi_sender, uazapi_folder_id, enable_cadence, cadence_config FROM campaigns WHERE id = %s AND user_id = %s", (campaign_id, current_user.id))
            campaign = cur.fetchone()
        conn.close()

        if not campaign:
            return json.dumps({"error": "Campanha não encontrada"}), 404

        current_status = campaign['status']
        new_status = None

        if current_status == 'running':
            new_status = 'paused'
        elif current_status == 'paused':
            new_status = 'running'
        elif current_status == 'pending':
            new_status = 'paused'  # Allow pausing pending campaigns too
        else:
            return json.dumps({"error": f"Não é possível pausar/continuar campanha com status '{current_status}'"}), 400

        action = 'stop' if new_status == 'paused' else 'continue'

        if action == 'continue':
            conn_g = get_db_connection()
            try:
                gr = _guard_resume_after_whatsapp_disconnect(conn_g, campaign_id, data)
            finally:
                conn_g.close()
            if gr:
                return gr
            if campaign.get("use_uazapi_sender") and campaign.get("enable_cadence"):
                success, err, extra = _resume_campaign_after_pause(
                    campaign_id,
                    current_user.id,
                    admin_mode=False,
                    trigger_initial_chunk=True,
                )
                if not success:
                    return json.dumps({"error": err or "Erro ao retomar campanha"}), 500
                resp_body = {"success": True, "new_status": "running"}
                if extra.get("initial_chunk"):
                    resp_body["initial_chunk"] = extra["initial_chunk"]
                return json.dumps(resp_body)

        # Se use_uazapi_sender, delegar para Uazapi API (principal + follow-ups se cadência)
        if campaign.get('use_uazapi_sender') and campaign.get('uazapi_folder_id'):
            success, err = _uazapi_control_campaign(campaign_id, current_user.id, action)
            if not success:
                return json.dumps({"error": err or "Erro ao controlar campanha Uazapi"}), 500
            # Pausar/continuar também os follow-ups de rollover (cadence)
            if campaign.get('enable_cadence'):
                cfg = campaign.get('cadence_config') or {}
                if isinstance(cfg, str):
                    try:
                        cfg = json.loads(cfg) if cfg else {}
                    except json.JSONDecodeError:
                        cfg = {}
                for fid in iter_fu1_folder_ids(cfg):
                    _uazapi_control_folder(campaign_id, current_user.id, str(fid), action)
                for key in ('rollover_fu2_folder_id', 'rollover_fu3_folder_id'):
                    fid = cfg.get(key)
                    if fid:
                        _uazapi_control_folder(campaign_id, current_user.id, str(fid), action)
            # Despausar: mesmo fluxo do botão Continuar (próximo chunk inicial na hora)
            resp_body = {"success": True, "new_status": new_status}
            if new_status == "running" and campaign.get("enable_cadence"):
                cres = _continue_initial_chunk_core(
                    campaign_id, current_user.id, log_label="toggle-start+initial-chunk"
                )
                resp_body["initial_chunk"] = _initial_chunk_outbox_extra_from_result(cres)
            return json.dumps(resp_body)

        # Comportamento atual para campanhas sem Uazapi principal
        conn = get_db_connection()
        with conn.cursor() as cur:
            if new_status == 'running':
                cur.execute(_sql_resume_running_clear_system_disconnect_pause(), (campaign_id,))
            else:
                cur.execute("UPDATE campaigns SET status = %s WHERE id = %s", (new_status, campaign_id))
            if new_status == 'running' and cur.rowcount == 0:
                conn.rollback()
                conn.close()
                return json.dumps({"error": "Estado da campanha mudou; recarregue."}), 409
        conn.commit()
        conn.close()

        # Se cadência ativa, pausar/continuar também os follow-ups de rollover
        if campaign.get('enable_cadence'):
            cfg = campaign.get('cadence_config') or {}
            if isinstance(cfg, str):
                try:
                    cfg = json.loads(cfg) if cfg else {}
                except json.JSONDecodeError:
                    cfg = {}
            for fid in iter_fu1_folder_ids(cfg):
                _uazapi_control_folder(campaign_id, current_user.id, str(fid), action)
            for key in ('rollover_fu2_folder_id', 'rollover_fu3_folder_id'):
                fid = cfg.get(key)
                if fid:
                    _uazapi_control_folder(campaign_id, current_user.id, str(fid), action)

        return json.dumps({"success": True, "new_status": new_status})

    except Exception as e:
        print(f"Erro ao alternar pausa da campanha: {e}")
        return json.dumps({"error": str(e)}), 500

@app.route('/api/ai/generate-copy', methods=['POST'])
@login_required
def generate_ai_copy():
    """Gera mensagem persuasiva usando IA para campanhas WhatsApp"""
    try:
        data = request.json
        business_context = data.get('business_context', '').strip()
        
        if not business_context:
            return json.dumps({'error': 'Por favor, descreva seu negócio e produto/serviço'}), 400
        
        # Configurar cliente OpenAI
        api_key = os.environ.get('OPENAI_API_KEY')
        if not api_key:
            return json.dumps({'error': 'API Key do OpenAI não configurada'}), 500
        
        client = OpenAI(api_key=api_key)
        
        # Prompt otimizado para cold-outreach no WhatsApp
        prompt = f"""Você é um especialista em copywriting para WhatsApp cold-outreach B2B.

CONTEXTO DO NEGÓCIO:
{business_context}

INSTRUÇÕES:
1. Crie uma mensagem de prospecção curta e direta (máximo 3-4 linhas)
2. Use linguagem natural, informal mas profissional (você/tu, não "vossa empresa")
3. OBRIGATÓRIO: Use {{nome}} no início para personalização
4. Foque em despertar curiosidade ou oferecer valor imediato
5. Evite palavras "spam" como "promoção", "desconto imperdível", "clique já"
6. Termine com uma pergunta ou CTA sutil
7. NÃO use emojis excessivos (máximo 1)
8. Seja específico sobre o benefício para o prospect

Retorne APENAS a mensagem, sem explicações ou aspas."""

        # Chamar API do OpenAI
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Você é um expert em copywriting para WhatsApp B2B. Suas mensagens são curtas, naturais e altamente conversíveis."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.8,  # Criatividade balanceada
            max_tokens=150
        )
        
        generated_message = response.choices[0].message.content.strip()
        
        # Garantir que {nome} está presente
        if '{nome}' not in generated_message.lower():
            # Adicionar {nome} no início se não estiver presente
            generated_message = f"Olá {{nome}}, {generated_message[0].lower()}{generated_message[1:]}"
        
        return json.dumps({'message': generated_message})
        
    except Exception as e:
        print(f"Erro ao gerar copy com IA: {e}")
        return json.dumps({'error': f'Erro na geração de IA: {str(e)}'}), 500

@app.route('/api/upload-csv-leads', methods=['POST'])
@login_required
def upload_csv_leads():
    if 'file' not in request.files:
        return json.dumps({'error': 'Nenhum arquivo enviado'}), 400
        
    file = request.files['file']
    if file.filename == '':
        return json.dumps({'error': 'Arquivo vazio'}), 400
        
    if not file.filename.endswith('.csv'):
        return json.dumps({'error': 'Apenas arquivos .csv são permitidos'}), 400
        
    try:
        # Salvar arquivo
        user_dir = os.path.join(os.environ.get("STORAGE_DIR", "storage"), str(current_user.id), "Uploads")
        os.makedirs(user_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"upload_{timestamp}_{file.filename}"
        filepath = os.path.join(user_dir, filename)
        
        file.save(filepath)
        
        # Analisar o arquivo para contar leads (encoding para CSVs com acentos)
        try:
            df = pd.read_csv(filepath, dtype=str, encoding='utf-8', encoding_errors='replace')
        except Exception:
            df = pd.read_csv(filepath, dtype=str)
        
        # Adicionar coluna 'status' se não existir (valor 1 = pronto para envio)
        if 'status' not in [c.lower() for c in df.columns]:
            df['status'] = 1
            # Salvar novamente com a coluna status
            df.to_csv(filepath, index=False)
        
        # Tentar identificar colunas
        cols = [c.lower() for c in df.columns]
        name_col = next((c for c in cols if 'name' in c or 'nome' in c), None)
        
        count = 0
        if name_col:
             # Contar válidos usando a coluna 'name' como referência
             count = int(df[df.columns[cols.index(name_col)]].notna().sum())
        else:
             # Se não tiver coluna name, usar o total de linhas
             count = int(len(df))
        
        # Criar registro de Job "Fake" para rastreabilidade
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO scraping_jobs 
                (user_id, keyword, locations, total_results, status, results_path, progress, completed_at)
                VALUES (%s, %s, %s, %s, 'completed', %s, 100, NOW())
                RETURNING id
                """,
                (current_user.id, f"Upload: {file.filename}", "Arquivo Local", count, filepath)
            )
            job_id = cur.fetchone()[0]
        conn.commit()
        conn.close()

        val = None
        try:
            from utils.validate_job_csv import validate_job_csv
            val = validate_job_csv(job_id, current_user.id)
        except Exception as e:
            print(f"[upload_csv_leads] validate_job_csv failed job_id={job_id}: {e}")

        resp = {
            'success': True,
            'job_id': job_id,
            'total_leads': int(count),
            'validated': bool(val),
            'valid': val['valid'] if val else int(count),
            'invalid': val['invalid'] if val else 0,
        }
        return json.dumps(resp)

    except Exception as e:
        print(f"Erro no upload: {e}")
        return json.dumps({'error': str(e)}), 500


def _enqueue_uazapi_initial_outbox(
    campaign_id: int,
    leads: list,
    allowed_instances: list,
    *,
    rotation_mode: str = "single",
    scheduled_start=None,
    flow: str = "create_campaign_core",
) -> tuple[int, datetime]:
    """
    Enfileira etapa ``initial`` em ``campaign_message_outbox`` (worker → ``/send/text``).
    Retorna (n_rows, next_run_at).
    """
    parsed_ss = (
        _parse_iso_datetime_local(scheduled_start) if scheduled_start else None
    )
    now_utc = datetime.utcnow()
    if parsed_ss:
        next_run_at_val = max(now_utc, parsed_ss)
    else:
        next_run_at_val = now_utc
    n_allowed = len(allowed_instances)
    conn_o = get_db_connection()
    try:
        with conn_o.cursor() as cur:
            for i, lead in enumerate(leads):
                if rotation_mode == "round_robin":
                    inst = allowed_instances[i % n_allowed]
                else:
                    inst = allowed_instances[0]
                lead_id = int(lead["id"])
                instance_id = int(inst["instance_id"])
                idempotency_key = f"campaign-{campaign_id}-lead-{lead_id}-initial"
                payload_summary = json.dumps(
                    {
                        "stage": "initial",
                        "enqueue": flow,
                        "rotation_mode": rotation_mode,
                    }
                )
                queued_at_val = now_utc + timedelta(
                    seconds=i // 1_000_000, microseconds=i % 1_000_000
                )
                cur.execute(
                    """
                    INSERT INTO campaign_message_outbox (
                        campaign_id, campaign_lead_id, instance_id,
                        stage, step_priority, status, queued_at,
                        next_run_at, idempotency_key, payload_summary
                    )
                    VALUES (
                        %s, %s, %s, 'initial', 0, 'pending',
                        %s, %s, %s, %s::jsonb
                    )
                    """,
                    (
                        campaign_id,
                        lead_id,
                        instance_id,
                        queued_at_val,
                        next_run_at_val,
                        idempotency_key,
                        payload_summary,
                    ),
                )
            lead_ids_out = [int(x["id"]) for x in leads]
            cur.execute(
                """
                UPDATE campaign_leads
                SET current_step = 1
                WHERE campaign_id = %s AND id = ANY(%s)
                """,
                (campaign_id, lead_ids_out),
            )
            cur.execute(
                "UPDATE campaigns SET status = 'running' WHERE id = %s",
                (campaign_id,),
            )
        conn_o.commit()
    finally:
        conn_o.close()
    return len(leads), next_run_at_val


def _enqueue_uazapi_stage_outbox(
    campaign_id: int,
    stage: str,
    leads: list,
    allowed_instances: list,
    *,
    rotation_mode: str = "single",
    next_run_at: Optional[datetime] = None,
    flow: str = "create_stage_campaign",
) -> int:
    """Enfileira follow1/follow2/breakup em ``campaign_message_outbox`` (sem ``campaign_stage_sends``)."""
    if not leads or not allowed_instances:
        return 0
    from worker_message_outbox import enqueue_outbox_rows_for_leads

    cid = int(campaign_id)
    n_allowed = len(allowed_instances)
    nr = next_run_at if next_run_at is not None else datetime.utcnow()
    conn_o = get_db_connection()
    total = 0
    try:
        for i, lead in enumerate(leads):
            if rotation_mode == "round_robin":
                inst = allowed_instances[i % n_allowed]
            else:
                inst = allowed_instances[0]
            lead_id = int(lead["id"] if isinstance(lead, dict) else lead)
            instance_id = int(inst["instance_id"])
            total += enqueue_outbox_rows_for_leads(
                conn_o,
                campaign_id=cid,
                stage=stage,
                lead_ids=[lead_id],
                instance_id=instance_id,
                enqueue_source=flow,
                next_run_at=nr,
            )
        if total:
            with conn_o.cursor() as cur:
                cur.execute(
                    "UPDATE campaigns SET status = 'running' WHERE id = %s",
                    (cid,),
                )
        conn_o.commit()
    finally:
        conn_o.close()
    return total


_CAMPAIGN_DAILY_LIMIT_PACKAGES = (5, 10, 15, 20, 25, 30)


def _normalize_campaign_daily_limit(submitted, plan_limit: int) -> int:
    """Pacotes {5..30 step 5}; clamp ao plano; mínimo 5."""
    cap = max(5, int(plan_limit))
    allowed = [p for p in _CAMPAIGN_DAILY_LIMIT_PACKAGES if p <= cap]
    if not allowed:
        return 5
    if submitted is None:
        raw = cap
    else:
        try:
            raw = max(5, min(int(submitted), cap))
        except (ValueError, TypeError):
            raw = cap
    return min(allowed, key=lambda p: (abs(p - raw), -p))


def _create_campaign_core(user_id, data, admin_id=None):
    """Cria campanha para ``user_id``. Se ``admin_id`` veio do painel admin, grava ``created_by_admin_id`` (auditoria). Com ``USE_MESSAGE_OUTBOX``, enfileira ``campaign_message_outbox`` (envio unitário ``/send/text``) em vez de ``create_advanced_campaign``."""
    def extract_phone_from_whatsapp_link(link):
        """Helper to extract phone from whatsapp link"""
        if not link: return None
        import re
        # Patterns
        patterns = [r'wa\.me/([0-9]+)', r'phone=([0-9]+)', r'whatsapp\.com/send\?phone=([0-9]+)']
        for pattern in patterns:
            match = re.search(pattern, str(link))
            if match: return match.group(1)
        # Fallback: just digits if long enough
        digits = re.sub(r'\D', '', str(link))
        if len(digits) >= 10: return digits
        return None

    name = data.get('name')
    job_id = data.get('job_id')
    # Pode receber 'message_template' (string única) ou 'message_templates' (lista)
    # Vamos padronizar salvando como JSON se for lista, ou string se for único.
    # Mas para "rotação", o ideal seria salvar uma lista JSON.
    message_templates = data.get('message_templates', [])
    if not message_templates and data.get('message_template'):
        message_templates = [data.get('message_template')]
        
    # Serializar para salvar no banco
    message_template_json = json.dumps(message_templates)
    
    # NEW: Get scheduled_start from request (optional)
    scheduled_start = data.get('scheduled_start')  # ISO format string or None
    
    # NEW: Get instance_ids and rotation_mode (multi-instance)
    instance_ids = data.get('instance_ids', [])  # list of instance IDs
    rotation_mode = data.get('rotation_mode', 'single')  # 'single' or 'round_robin'

    # Uazapi: inferido pelas instâncias (sem toggle) — basta haver ao menos uma Uazapi selecionada
    delay_min_minutes = data.get('delay_min_minutes')
    delay_max_minutes = data.get('delay_max_minutes')

    # Horário comercial configurável (faixa de horários + sábado/domingo)
    send_hour_start = data.get('send_hour_start', 8)
    send_hour_end = data.get('send_hour_end', 20)
    send_saturday = bool(data.get('send_saturday', False))
    send_sunday = bool(data.get('send_sunday', False))
    # Validar faixa de horário (0-23)
    if send_hour_start is not None:
        send_hour_start = max(0, min(23, int(send_hour_start)))
    if send_hour_end is not None:
        send_hour_end = max(0, min(23, int(send_hour_end)))
    
    # Validate rotation_mode
    if rotation_mode not in ('single', 'round_robin'):
        rotation_mode = 'single'
    
    # NEW: Validate scheduled_start if provided
    if scheduled_start:
        try:
            # Validar formato ISO (datetime já importado no topo do módulo)
            datetime.fromisoformat(scheduled_start.replace('Z', ''))
        except Exception as e:
            return json.dumps({'error': f'Data inválida: {str(e)}'}), 400
    
    if not name or not job_id:
        return json.dumps({'error': 'Nome e Job são obrigatórios'}), 400

    if not instance_ids:
        return json.dumps({'error': 'Selecione pelo menos uma instância de WhatsApp.'}), 400

    conn_check = get_db_connection()
    with conn_check.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM instances WHERE user_id = %s AND id = ANY(%s) AND COALESCE(api_provider, 'megaapi') = 'uazapi'",
            (user_id, instance_ids),
        )
        uazapi_selected_count = cur.fetchone()[0]
    conn_check.close()
    if uazapi_selected_count == 0:
        return json.dumps({'error': 'Selecione pelo menos uma instância Uazapi.'}), 400
    if uazapi_selected_count != len(instance_ids):
        return json.dumps({'error': 'Apenas instâncias Uazapi são permitidas na campanha.'}), 400
    use_uazapi_sender = True

    try:
        # 1. Obter leads do Job
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT results_path FROM scraping_jobs WHERE id = %s AND user_id = %s", (job_id, user_id))
            job = cur.fetchone()
        conn.close()
        
        if not job or not job['results_path'] or not os.path.exists(job['results_path']):
            return json.dumps({'error': 'Arquivo de leads não encontrado'}), 404
            
        # 2. Ler arquivo de resultados (CSV ou XLSX, mas o path aponta pra CSV geralmente)
        # Fix: O scraper salva em CSV, não JSON.
        file_path = job['results_path']
        valid_leads = []
        
        try:
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path, dtype=str)
            elif file_path.endswith('.xlsx'):
                df = pd.read_excel(file_path, dtype=str)
            else:
                 # Fallback: tentar ler como CSV
                df = pd.read_csv(file_path, dtype=str)
            
            # Normalizar colunas
            # O scraper gera: name, phone_number, etc.
            # O CSV pode ter colunas diferentes se vier de upload.
            
            # Map columns to expected: name, phone
            # Scraper output: 'name', 'phone_number'
            # Upload possible: 'nome', 'telefone', 'celular', 'phone'
            
            cols = [c.lower() for c in df.columns]
            df.columns = cols
            
            # Identificar coluna de telefone
            phone_col = next((c for c in cols if 'phone' in c or 'tel' in c or 'cel' in c), None)
            # Não incluir 'whatsapp' genérico na busca de phone_col, pois whatsapp_link é separado
            name_col = next((c for c in cols if 'name' in c or 'nome' in c or 'title' in c), None)
            whatsapp_link_col = next((c for c in cols if c == 'whatsapp_link'), None)
            status_col = next((c for c in cols if c == 'status'), None)
            
            # Enrichment Columns
            address_col = next((c for c in cols if 'address' in c or 'endereço' in c), None)
            website_col = next((c for c in cols if 'website' in c or 'site' in c), None)
            category_col = next((c for c in cols if 'category' in c or 'categoria' in c), None)
            location_col = next((c for c in cols if 'location' in c or 'localização' in c), None)
            reviews_count_col = next((c for c in cols if 'reviews_count' in c or 'avaliações' in c), None)
            reviews_rating_col = next((c for c in cols if 'reviews_average' in c or 'rating' in c or 'nota' in c), None)
            latitude_col = next((c for c in cols if 'latitude' in c or 'lat' == c), None)
            longitude_col = next((c for c in cols if 'longitude' in c or 'lon' == c or 'lng' == c), None)
            
            # Check availability: Need either phone_col OR whatsapp_link_col
            if not phone_col and not whatsapp_link_col:
                 return json.dumps({'error': 'Nenhuma coluna de telefone ou link de WhatsApp encontrada no arquivo'}), 400
            
            # Filtrar apenas leads com status = 1 (ou sem coluna status)
            # Filtrar apenas leads com status = 1 (ou sem coluna status)
            if status_col:
                # Convert to numeric to be safe or compare with string '1'
                # Since we used dtype=str, it should be '1'. 
                # Handling both cases safely:
                df_filtered = df[df[status_col].astype(str).str.strip() == '1']
            else:
                df_filtered = df
                 
            for _, row in df_filtered.iterrows():
                raw_phone = str(row[phone_col]) if phone_col and pd.notna(row[phone_col]) else ""
                raw_name = str(row[name_col]) if name_col and pd.notna(row[name_col]) else "Visitante"
                raw_whatsapp_link = str(row[whatsapp_link_col]) if whatsapp_link_col and pd.notna(row[whatsapp_link_col]) else None
                
                final_phone = None
                
                # 1. Try to extract from WhatsApp Link FIRST (Priority)
                if raw_whatsapp_link:
                    extracted = extract_phone_from_whatsapp_link(raw_whatsapp_link)
                    if extracted:
                        final_phone = extracted
                
                # 2. If not found, try Phone column
                if not final_phone and raw_phone:
                     clean_p = re.sub(r'\D', '', raw_phone)
                     if len(clean_p) >= 10:
                        final_phone = clean_p
                
                # 3. Add if valid
                if final_phone:
                    valid_leads.append({
                        'phone': final_phone,
                        'name': raw_name,
                        'whatsapp_link': raw_whatsapp_link,
                        'address': str(row[address_col]) if address_col and pd.notna(row[address_col]) else None,
                        'website': str(row[website_col]) if website_col and pd.notna(row[website_col]) else None,
                        'category': str(row[category_col]) if category_col and pd.notna(row[category_col]) else None,
                        'location': str(row[location_col]) if location_col and pd.notna(row[location_col]) else None,
                        'reviews_count': str(row[reviews_count_col]) if reviews_count_col and pd.notna(row[reviews_count_col]) else None,
                        'reviews_rating': str(row[reviews_rating_col]) if reviews_rating_col and pd.notna(row[reviews_rating_col]) else None,
                        'latitude': str(row[latitude_col]) if latitude_col and pd.notna(row[latitude_col]) else None,
                        'longitude': str(row[longitude_col]) if longitude_col and pd.notna(row[longitude_col]) else None
                    })

        except Exception as e:
            print(f"Erro ao ler arquivo: {e}")
            return json.dumps({'error': f'Erro ao ler arquivo: {str(e)}'}), 500
        
        if not valid_leads:
            return json.dumps({'error': 'Nenhum lead válido encontrado na lista'}), 400

        # 4. Criar Campanha
        # NEW: Create campaign with scheduled_start, dynamic status, rotation_mode, use_uazapi_sender
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Determine initial status based on scheduled_start
            # use_uazapi_sender: Uazapi gerencia envio; status 'running' após API call
            initial_status = 'pending' if scheduled_start else 'running'
            
            plan_limit = get_user_daily_limit(user_id)
            daily_limit = _normalize_campaign_daily_limit(data.get('daily_limit'), plan_limit)

            # ADR-2 / fila outbox: intervalo de cooldown (s) sorteado na criação e persistido.
            # Produto: 600–900 s (10–15 min). Legado delay_min/max_minutes permanece em minutos para chunks/pacing antigo.
            _dlo = random.randint(600, 900)
            _dhi = random.randint(600, 900)
            outbox_delay_min_seconds, outbox_delay_max_seconds = min(_dlo, _dhi), max(_dlo, _dhi)

            cur.execute(
                """
                INSERT INTO campaigns (user_id, name, message_template, daily_limit, scheduled_start, status, rotation_mode, use_uazapi_sender, delay_min_minutes, delay_max_minutes, send_hour_start, send_hour_end, send_saturday, send_sunday, created_by_admin_id, outbox_delay_min_seconds, outbox_delay_max_seconds)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id, created_at
                """,
                (
                    user_id,
                    name,
                    message_template_json,
                    daily_limit,
                    scheduled_start,
                    initial_status,
                    rotation_mode,
                    use_uazapi_sender,
                    delay_min_minutes,
                    delay_max_minutes,
                    send_hour_start,
                    send_hour_end,
                    send_saturday,
                    send_sunday,
                    admin_id,
                    outbox_delay_min_seconds,
                    outbox_delay_max_seconds,
                ),
            )
            row = cur.fetchone()
            campaign_id = row[0]
            created_at = row[1]
            
            # NEW: Insert campaign_instances associations
            if instance_ids:
                # Validate that all instance_ids belong to this user
                cur.execute("SELECT id FROM instances WHERE user_id = %s AND id = ANY(%s)", 
                           (user_id, instance_ids))
                valid_ids = [r[0] for r in cur.fetchall()]
                for inst_id in valid_ids:
                    cur.execute(
                        "INSERT INTO campaign_instances (campaign_id, instance_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (campaign_id, inst_id)
                    )
            else:
                # Backward compatible: auto-associate user's default instance
                cur.execute("SELECT id FROM instances WHERE user_id = %s ORDER BY updated_at DESC LIMIT 1", 
                           (user_id,))
                default_inst = cur.fetchone()
                if default_inst:
                    cur.execute(
                        "INSERT INTO campaign_instances (campaign_id, instance_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (campaign_id, default_inst[0])
                    )
            
            # ============================================================
            # CADENCE: Save steps, media, and flags
            # ============================================================
            enable_cadence = data.get('enable_cadence', False)
            terms_accepted = data.get('terms_accepted', False)
            steps = data.get('steps', [])

            # Uazapi-only: permite escolher se follow-ups serão configurados agora
            # ou mais tarde via botão "Gerar Campanha" no Kanban.
            cadence_setup_mode = str(data.get('cadence_setup_mode') or '').strip().lower()
            if cadence_setup_mode not in ('now', 'kanban_later'):
                cadence_setup_mode = 'now'
            if not use_uazapi_sender:
                cadence_setup_mode = 'now'

            if enable_cadence:
                if use_uazapi_sender:
                    cadence_config = {'cadence_setup_mode': cadence_setup_mode}
                else:
                    rollover_time = data.get('rollover_time', '23:00')
                    if rollover_time and not re.match(r'^\d{1,2}:\d{2}$', str(rollover_time)):
                        rollover_time = '23:00'
                    rollover_test_mode = bool(data.get('rollover_test_mode', False))
                    rollover_test_delay = int(data.get('rollover_test_delay_minutes', 5))
                    rollover_test_delay = max(1, min(60, rollover_test_delay))
                    cadence_config = {
                        'rollover_time': str(rollover_time),
                        'rollover_test_mode': rollover_test_mode,
                        'rollover_test_delay_minutes': rollover_test_delay,
                        'cadence_setup_mode': cadence_setup_mode,
                    }
                cadence_config_json = json.dumps(cadence_config)
                cur.execute(
                    """UPDATE campaigns SET enable_cadence = TRUE, terms_accepted = %s,
                       cadence_config = COALESCE(cadence_config, '{}')::jsonb || %s::jsonb
                       WHERE id = %s""",
                    (terms_accepted, cadence_config_json, campaign_id)
                )

            if enable_cadence and steps:
                # Media storage directory
                media_dir = os.path.join('storage', str(user_id), 'campaign_media')
                os.makedirs(media_dir, exist_ok=True)
                
                for step in steps:
                    step_number = step.get('step_number', 1)
                    step_label = step.get('step_label', '')
                    step_messages = step.get('message_templates', [])
                    delay_days = step.get('delay_days', 0)
                    media_base64 = step.get('media_base64')
                    media_name = step.get('media_name')
                    media_type = step.get('media_type')  # 'image' or 'video'
                    
                    media_path = None
                    if media_base64 and media_name:
                        import base64, uuid
                        # Generate unique filename
                        ext = os.path.splitext(media_name)[1] or '.bin'
                        unique_name = f"camp_{campaign_id}_step_{step_number}_{uuid.uuid4().hex[:8]}{ext}"
                        media_path = os.path.join(media_dir, unique_name)
                        
                        # Decode and save
                        try:
                            with open(media_path, 'wb') as f:
                                f.write(base64.b64decode(media_base64))
                        except Exception as e:
                            print(f"⚠️ Failed to save media for step {step_number}: {e}")
                            media_path = None
                    
                    # Serialize step messages as JSON
                    step_template_json = json.dumps(step_messages)
                    
                    cur.execute(
                        """
                        INSERT INTO campaign_steps (campaign_id, step_number, step_label, message_template, media_path, media_type, delay_days)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (campaign_id, step_number) DO UPDATE SET
                            step_label = EXCLUDED.step_label,
                            message_template = EXCLUDED.message_template,
                            media_path = EXCLUDED.media_path,
                            media_type = EXCLUDED.media_type,
                            delay_days = EXCLUDED.delay_days
                        """,
                        (campaign_id, step_number, step_label, step_template_json, media_path, media_type, delay_days)
                    )
            # ============================================================
            # END CADENCE
            # ============================================================
            
        conn.commit()
        conn.close()
        
        # 5. Adicionar Leads
        CampaignLead.add_leads(campaign_id, valid_leads)
        
        # 5b. Uazapi: atribuir send_batch aos pendentes (para follow-up cadence)
        # Batch = daily_limit leads por lote (ex: daily_limit=5 → batch 1 = leads 1-5, batch 2 = 6-10)
        if use_uazapi_sender:
            per_instance_limit = daily_limit
            conn = get_db_connection()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM campaign_leads WHERE campaign_id = %s AND status = 'pending' ORDER BY COALESCE(csv_row_order, id) ASC, id ASC",
                    (campaign_id,)
                )
                pending_ids = [r[0] for r in cur.fetchall()]
                for i, lead_id in enumerate(pending_ids):
                    batch_num = (i // per_instance_limit) + 1
                    cur.execute("UPDATE campaign_leads SET send_batch = %s WHERE id = %s", (batch_num, lead_id))
            conn.commit()
            conn.close()
        
        # 6. Uazapi: se use_uazapi_sender, distribuir leads em chunks de 30 por instância (assíncronas).
        # ``USE_MESSAGE_OUTBOX``: enfileirar ``campaign_message_outbox`` (worker → ``/send/text``)
        # sem ``create_advanced_campaign``; ``next_run_at`` inicial respeita ``scheduled_start``.
        if use_uazapi_sender:
            use_message_outbox = USE_MESSAGE_OUTBOX
            use_outbox_enqueue = use_message_outbox
            from utils.limits import can_create_campaign_today
            from utils.sync_uazapi import _normalize_phone_for_api

            instances = _get_uazapi_instances_for_campaign(campaign_id, user_id)
            allowed_instances = [inst for inst in instances if can_create_campaign_today(inst['instance_id'])]
            if not allowed_instances:
                print(
                    f"⚠️ [Uazapi] Campanha {campaign_id}: nenhuma instância disponível "
                    f"(limite diário ou sem Uazapi). use_message_outbox={use_message_outbox}"
                )
            else:
                from utils.campaign_send_policy import uazapi_initial_chunk_distribution_limits

                n_inst = len(allowed_instances)
                per_instance_limit, total_limit = uazapi_initial_chunk_distribution_limits(
                    daily_limit, n_inst
                )

                conn = get_db_connection()
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # Step 1 media (superadmin only) para envio com mídia
                    step1_media_path = None
                    step1_media_type = 'image'
                    if admin_id is not None and enable_cadence:
                        cur.execute(
                            "SELECT media_path, media_type FROM campaign_steps WHERE campaign_id = %s AND step_number = 1 LIMIT 1",
                            (campaign_id,)
                        )
                        step1 = cur.fetchone()
                        if step1 and step1.get('media_path'):
                            mp = step1['media_path']
                            user_storage = os.path.abspath(os.path.join('storage', str(user_id)))
                            if mp and os.path.isfile(mp) and os.path.abspath(mp).startswith(user_storage):
                                step1_media_path = mp
                                step1_media_type = step1.get('media_type') or 'image'
                conn.close()

                # Parse message_templates (lista de variações)
                try:
                    variations = json.loads(message_template_json)
                    if isinstance(variations, str):
                        variations = [variations]
                    if not variations:
                        variations = ["Olá!"]
                except Exception:
                    variations = [message_template_json or "Olá!"]

                # Base64 da mídia (se step 1 tem mídia e superadmin)
                media_file_data = None
                if step1_media_path:
                    try:
                        import base64
                        with open(step1_media_path, 'rb') as f:
                            b64 = base64.b64encode(f.read()).decode('utf-8')
                        ext = os.path.splitext(step1_media_path)[1].lower()
                        mime_map = {'.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png', '.gif': 'image/gif', '.mp4': 'video/mp4', '.webm': 'video/webm'}
                        mime = mime_map.get(ext, 'application/octet-stream')
                        media_file_data = f"data:{mime};base64,{b64}"
                    except Exception as e:
                        print(f"⚠️ [UAZAPI] Erro ao ler mídia step 1: {e}")

                # Obter leads pendentes (chunk 30 por instância)
                conn = get_db_connection()
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        """SELECT id, phone, whatsapp_link, name FROM campaign_leads
                           WHERE campaign_id = %s AND status = 'pending'
                           ORDER BY COALESCE(send_batch, 999) ASC, COALESCE(csv_row_order, id) ASC, id ASC LIMIT %s""",
                        (campaign_id, total_limit)
                    )
                    leads = cur.fetchall()
                conn.close()

                if use_outbox_enqueue and leads:
                    n_rows, next_run_at_val = _enqueue_uazapi_initial_outbox(
                        campaign_id,
                        leads,
                        allowed_instances,
                        rotation_mode=rotation_mode,
                        scheduled_start=scheduled_start,
                        flow="create_campaign_core",
                    )
                    print(
                        f"[UAZAPI] Outbox enqueue campaign_id={campaign_id} "
                        f"rows={n_rows} next_run_at={next_run_at_val.isoformat()} "
                        f"(create_advanced_campaign skipped)"
                    )
                else:

                    def _chunk(lst, n):
                        return [lst[i:i + n] for i in range(0, len(lst), n)]

                    lead_chunks = _chunk(leads, per_instance_limit)
                    from utils.uazapi_pacing import default_inter_message_delay_range_minutes

                    d_lo, d_hi = default_inter_message_delay_range_minutes()
                    delay_min_sec = int(d_lo * 60)
                    delay_max_sec = int(d_hi * 60)
                    scheduled_for_param = None
                    if scheduled_start:
                        try:
                            dt = datetime.fromisoformat(scheduled_start.replace('Z', '+00:00'))
                            now = datetime.now(dt.tzinfo) if dt.tzinfo else datetime.now()
                            delta_min = (dt - now).total_seconds() / 60
                            if delta_min > 0:
                                scheduled_for_param = max(1, int(delta_min))
                        except Exception:
                            pass

                    uazapi = UazapiService()
                    sends_created = []
                    errors = []
                    for idx, chunk in enumerate(lead_chunks):
                        if idx >= len(allowed_instances):
                            break
                        inst = allowed_instances[idx]
                        token = inst.get('apikey')
                        if not token:
                            continue

                        messages = []
                        lead_ids = []
                        for lead in chunk:
                            msg_text = random.choice(variations)
                            if lead.get('name'):
                                msg_text = msg_text.replace("{nome}", lead['name']).replace("{name}", lead['name']).replace("{{nome}}", lead['name']).replace("{{name}}", lead['name'])
                            raw = lead.get('phone') or lead.get('whatsapp_link')
                            phone = _normalize_phone_for_api(raw)
                            if not phone:
                                continue
                            if media_file_data and step1_media_path:
                                messages.append({"number": phone, "type": step1_media_type, "file": media_file_data, "text": msg_text})
                            else:
                                messages.append({"number": phone, "type": "text", "text": msg_text})
                            lead_ids.append(lead['id'])

                        if not messages:
                            continue

                        payload_summary = {"campaign_id": campaign_id, "leads": len(messages), "instance_id": inst['instance_id'], "use_message_outbox": use_message_outbox}
                        print(f"[UAZAPI] create_advanced_campaign payload: {json.dumps(payload_summary)}")
                        _t_adv = time.monotonic()
                        result = uazapi.create_advanced_campaign(
                            token, delay_min_sec, delay_max_sec, messages,
                            info=name, scheduled_for=scheduled_for_param
                        )
                        _lat_adv = int((time.monotonic() - _t_adv) * 1000)
                        if result and result.get('folder_id'):
                            print(f"[UAZAPI] create_advanced_campaign OK campaign_id={campaign_id} inst={inst['instance_id']} folder_id={result['folder_id']}")
                            try:
                                append_dispatch_audit_event(
                                    user_id=int(user_id),
                                    campaign_id=int(campaign_id),
                                    event={
                                        "stage": "initial",
                                        "outcome": "folder_created",
                                        "latency_ms": _lat_adv,
                                        "http_status": 200,
                                        "request": {
                                            "kind": "legacy_advanced_campaign",
                                            "flow": "create_campaign_core",
                                            "instance_id": int(inst["instance_id"]),
                                            "lead_ids_in_order": lead_ids,
                                            "message_count": len(messages),
                                            "delay_min_sec": delay_min_sec,
                                            "delay_max_sec": delay_max_sec,
                                            "scheduled_for": scheduled_for_param,
                                            "campaign_name_preview": (name or "")[:200],
                                        },
                                        "response": result if isinstance(result, dict) else result,
                                    },
                                )
                            except Exception:
                                pass
                            instance_remote_jid = _resolve_uazapi_remote_jid(uazapi, token)
                            sends_created.append({
                                "instance_id": inst['instance_id'],
                                "instance_remote_jid": instance_remote_jid,
                                "folder_id": result['folder_id'],
                                "lead_ids": lead_ids,
                                "planned_count": len(lead_ids),
                            })
                        else:
                            errors.append(f"Instância {inst['instance_id']}: falha ao criar campanha")

                    if sends_created:
                        all_lead_ids = [lid for s in sends_created for lid in s['lead_ids']]
                        first_folder_id = sends_created[0]['folder_id']
                        conn = get_db_connection()
                        with conn.cursor(cursor_factory=RealDictCursor) as cur:
                            for send in sends_created:
                                cur.execute(
                                    "INSERT INTO uazapi_instance_sends (instance_id, campaign_id) VALUES (%s, %s)",
                                    (send['instance_id'], campaign_id)
                                )
                                cur.execute(
                                    """INSERT INTO campaign_stage_sends
                                       (campaign_id, stage, instance_id, instance_remote_jid, uazapi_folder_id, status, planned_count, lead_ids)
                                       VALUES (%s, 'initial', %s, %s, %s, 'running', %s, %s)""",
                                    (
                                        campaign_id,
                                        send['instance_id'],
                                        send.get('instance_remote_jid'),
                                        send['folder_id'],
                                        send['planned_count'],
                                        json.dumps(send['lead_ids']),
                                    ),
                                )
                            cur.execute(
                                "UPDATE campaigns SET uazapi_folder_id = %s, uazapi_last_send_lead_ids = %s, status = 'running' WHERE id = %s",
                                (first_folder_id, json.dumps(all_lead_ids) if all_lead_ids else None, campaign_id)
                            )
                            # UAZAPI: create_advanced_campaign só cria a pasta na fila — não confirma entrega.
                            # Nunca marcar status=sent em lote aqui (bug: UI "Enviado" para todos com chunk partial).
                            # campaign_leads ficam pending até sync + message_find (ou fluxo legado explícito).
                            cur.execute(
                                """UPDATE campaign_leads SET current_step = 1 WHERE campaign_id = %s AND id = ANY(%s)""",
                                (campaign_id, all_lead_ids)
                            )
                        conn.commit()
                        conn.close()
                        if errors:
                            print(f"⚠️ [UAZAPI] Campanha {campaign_id}: {len(sends_created)} sub-campanhas criadas; erros: {errors}")
                    else:
                        print(f"⚠️ [UAZAPI] Campanha {campaign_id}: nenhuma sub-campanha criada. Erros: {errors}")
        
        return json.dumps({'success': True, 'campaign_id': campaign_id, 'leads_count': len(valid_leads)})
        
    except Exception as e:
        print(f"Erro ao criar campanha: {e}")
        return json.dumps({'error': str(e)}), 500


@app.route('/api/campaigns', methods=['POST'])
@login_required
def create_campaign():
    """Criador normal (Minhas Campanhas). Outbox quando ``USE_MESSAGE_OUTBOX`` (sem gate superadmin)."""
    return _create_campaign_core(current_user.id, request.json)


@app.route("/dashboard")
@login_required
def dashboard():
    """Página de dashboard geral"""
    conn = get_db_connection()
    try:
        reconnect_alerts = fetch_reconnect_inapp_alerts_for_user(conn, current_user.id)
    finally:
        conn.close()
    return render_template("dashboard.html", reconnect_alerts=reconnect_alerts)


@app.route("/jobs")
@login_required
def jobs():
    """Página para visualizar jobs de scraping"""
    user_jobs = ScrapingJob.get_by_user_id(current_user.id, limit=20)
    
    # Processar jobs para o template
    processed_jobs = []
    for job in user_jobs:
        job_dict = dict(job)
        # Parse locations JSON
        try:
            locations = json.loads(job['locations']) if job['locations'] else []
            job_dict['locations_count'] = len(locations)
        except:
            job_dict['locations_count'] = 0
        processed_jobs.append(job_dict)
    
    return render_template("jobs.html", jobs=processed_jobs)


@app.route("/api/job/<int:job_id>")
@login_required
def get_job_status(job_id):
    """API para obter status de um job"""
    job = ScrapingJob.get_by_id(job_id)
    if not job or job['user_id'] != current_user.id:
        return {"error": "Job not found"}, 404
    
    return {
        "id": job['id'],
        "status": job['status'],
        "progress": job['progress'],
        "current_location": job['current_location'],
        "error_message": job['error_message'],
        "results_path": job['results_path'],
        "created_at": job['created_at'],
        "started_at": job['started_at'],
        "completed_at": job['completed_at']
    }


@app.route("/api/job/<int:job_id>/cancel", methods=["POST"])
@login_required
def cancel_job(job_id):
    """API para cancelar um job (extração ou validação). Worker verifica status e interrompe."""
    job = ScrapingJob.get_by_id(job_id)
    if not job or job['user_id'] != current_user.id:
        return json.dumps({"error": "Job não encontrado"}), 404, {"Content-Type": "application/json"}
    if job['status'] in ['completed', 'failed', 'cancelled']:
        return json.dumps({"error": "Job já finalizado"}), 400, {"Content-Type": "application/json"}
    ScrapingJob.update_status(job_id, 'cancelled', error_message='Cancelado pelo usuário')
    return json.dumps({"status": "cancelled"})

@app.route("/api/job/<int:job_id>", methods=["DELETE"])
@login_required
def delete_job(job_id):
    """API para excluir um job"""
    job = ScrapingJob.get_by_id(job_id)
    if not job or job['user_id'] != current_user.id:
        return {"error": "Job not found"}, 404
        
    # Excluir arquivos se existirem
    if job['results_path'] and os.path.exists(job['results_path']):
        try:
            os.remove(job['results_path'])
            # Tentar remover .xlsx também se houver
            xlsx_path = job['results_path'].replace('.csv', '.xlsx')
            if os.path.exists(xlsx_path):
                os.remove(xlsx_path)
        except:
            pass
            
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM scraping_jobs WHERE id = %s", (job_id,))
    conn.commit()
    conn.close()
    
    return {"status": "deleted"}

@app.route("/download")
@login_required
def download_file():
    path = request.args.get('path')
    if not path:
        return "Path required", 400
        
    if not os.path.exists(path):
        return "File not found", 404
        
    return send_file(path, as_attachment=True)


@app.route("/whatsapp")
@login_required
def whatsapp_config():
    """Page to configure WhatsApp instance(s)"""
    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM instances WHERE user_id = %s ORDER BY id ASC", (current_user.id,))
        instances = cur.fetchall()
        cur.execute(
            """
            SELECT license_type
            FROM licenses
            WHERE user_id = %s AND status = 'active' AND expires_at > NOW()
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (current_user.id,),
        )
        license_row = cur.fetchone() or {}
    conn.close()
    
    # Backward compatibility: pass first instance as 'instance' for non-super-admin template
    instance = instances[0] if instances else None
    active_license_type = resolve_license_type(license_row.get("license_type")) or "starter"
    plan_policy = get_plan_policy(active_license_type)
    instance_limit = int(plan_policy["instance_limit"])
    current_instances_count = len(instances)
    can_add_instance = current_instances_count < instance_limit
    return render_template("whatsapp_config.html", 
                           instance=instance, 
                           instances=instances,
                           is_super_admin=is_super_admin(),
                           active_license_type=active_license_type,
                           instance_limit=instance_limit,
                           current_instances_count=current_instances_count,
                           can_add_instance=can_add_instance)


@app.route("/api/whatsapp/init", methods=["POST"])
@login_required
def init_whatsapp():
    """API to initialize a WhatsApp instance"""
    if not current_user.has_active_license():
        return {"error": "Sua licença expirou ou não está ativa. Renove sua licença para criar novas instâncias."}, 403

    payload = request.get_json(silent=True) or {}
    instance_name = payload.get("instance_name") or ""

    # Sanitize if provided
    safe_name = ""
    if instance_name:
        safe_name = "".join(c for c in instance_name if c.isalnum() or c in ('-', '_'))
    
    try:
        conn = get_db_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                snapshot = _get_user_plan_snapshot_for_limit(cur, current_user.id)
                if not snapshot:
                    conn.rollback()
                    return {"error": "Usuário não encontrado."}, 404

                if snapshot["current_instances"] >= snapshot["instance_limit"]:
                    conn.rollback()
                    return {"error": INSTANCE_LIMIT_REACHED_MESSAGE}, 400

                # Forçado: criação sempre via Uazapi para todos os usuários.
                uazapi = UazapiService()
                result = uazapi.create_instance(safe_name if safe_name else "instance")
                if not result:
                    conn.rollback()
                    return {"error": "Falha ao criar instância na Uazapi."}, 500
                instance_key = result.get('token') or (result.get('instance') or {}).get('token')
                if not instance_key:
                    print(f"Warning: No token from Uazapi. Result: {result}")
                    conn.rollback()
                    return {"error": "Falha ao obter token da instância. Resposta da API inválida."}, 500

                cur.execute(
                    """
                    INSERT INTO instances (user_id, name, apikey, status, api_provider)
                    VALUES (%s, %s, %s, 'disconnected', %s)
                    """,
                    (current_user.id, instance_name or safe_name or "instance", instance_key, 'uazapi')
                )

            conn.commit()
            return {"status": "success", "key": instance_key, "data": result}
        finally:
            conn.close()
    except Exception as e:
        print(f"Error in init_whatsapp: {e}")
        return {"error": f"Erro interno: {str(e)}"}, 500


@app.route("/api/whatsapp/qr/<instance_key>")
@login_required
def get_whatsapp_qr(instance_key):
    """API to get QR code"""
    # Verify ownership
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT user_id, COALESCE(api_provider, 'megaapi') as api_provider FROM instances WHERE apikey = %s", (instance_key,))
        row = cur.fetchone()
    conn.close()
    
    if not row or row[0] != current_user.id:
        return {"error": "Unauthorized"}, 403
    
    api_provider = row[1]
    
    if api_provider == 'uazapi':
        uazapi = UazapiService()
        result = uazapi.connect(instance_key)
        if not result:
            return {"error": "Falha ao obter QR code da Uazapi."}, 500
        instance_data = result.get('instance') or {}
        if isinstance(instance_data, dict) and instance_data.get('status') in ('connected', 'open'):
            return {"error": "Instância já está conectada! Não é necessário escanear QR Code."}, 200
        qrcode_val = result.get('qrcode') or instance_data.get('qrcode')
        if qrcode_val and isinstance(qrcode_val, str) and len(qrcode_val) > 50:
            return {"base64": qrcode_val}
        return {"error": "QR Code não disponível. Tente novamente em alguns segundos."}, 500

    return (
        json.dumps({"error": "Instância legada. Crie uma nova instância Uazapi."}),
        400,
        {"Content-Type": "application/json"},
    )


@app.route("/api/whatsapp/status/<instance_key>")
@login_required
def get_whatsapp_status(instance_key):
    """API to get status"""
    # Verify ownership
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT id, user_id, COALESCE(api_provider, 'megaapi') as api_provider FROM instances WHERE apikey = %s", (instance_key,))
        row = cur.fetchone()
    conn.close()
    
    if not row or row[1] != current_user.id:
        return {"error": "Unauthorized"}, 403
    
    api_provider = row[2]
    
    if api_provider == 'uazapi':
        uazapi = UazapiService()
        result = uazapi.get_status(instance_key)
        if not result:
            return {"error": "Failed to get status"}, 500
        instance_data = result.get('instance') or result
        status_val = instance_data.get('status', 'disconnected') if isinstance(instance_data, dict) else 'disconnected'
        new_status = status_val if status_val in ('connected', 'connecting', 'disconnected') else 'disconnected'
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("UPDATE instances SET status = %s, updated_at = NOW() WHERE id = %s", (new_status, row[0]))
        conn.commit()
        conn.close()
        print(f"Status checked for instance {instance_key} (User {current_user.id}): {new_status} (Uazapi)")
        return result

    return (
        json.dumps({"error": "Instância legada. Crie uma nova instância Uazapi."}),
        400,
        {"Content-Type": "application/json"},
    )


@app.route("/api/whatsapp/delete/<instance_key>", methods=["POST"])
@login_required
def delete_whatsapp_instance(instance_key):
    """API to delete instance"""
    print(f"🗑️ Deleting instance {instance_key} for user {current_user.id}...")
    
    # Verify ownership
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT id, user_id, COALESCE(api_provider, 'megaapi') as api_provider FROM instances WHERE apikey = %s", (instance_key,))
        row = cur.fetchone()
    conn.close()
    
    if not row or row[1] != current_user.id:
        print(f"❌ Unauthorized delete attempt for {instance_key}")
        return {"error": "Unauthorized"}, 403
    
    api_provider = row[2]
    
    if api_provider == 'uazapi':
        uazapi = UazapiService()
        success, status_code = uazapi.delete_instance(instance_key)
        print(f"🗑️ Uazapi Delete Result: success={success}, status={status_code}")
        # Sempre remover do DB: se Uazapi falhou (ex: instância já deletada lá),
        # o usuário ainda deve conseguir remover da UI
        print(f"🗑️ Removing from database...")
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM instances WHERE id = %s", (row[0],))
        conn.commit()
        conn.close()
        return {"status": "success", "message": "Instance deleted"}

    return (
        json.dumps({"error": "Instância legada. Crie uma nova instância Uazapi."}),
        400,
        {"Content-Type": "application/json"},
    )




@app.route("/api/campaigns/<int:campaign_id>/stats")
@login_required
def get_campaign_stats(campaign_id):
    """API para obter estatísticas de uma campanha"""
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Verificar se a campanha pertence ao usuário e se usa Uazapi
            cur.execute(
                "SELECT id, closed_deals, use_uazapi_sender, uazapi_folder_id, status, enable_cadence FROM campaigns WHERE id = %s AND user_id = %s",
                (campaign_id, current_user.id)
            )
            campaign = cur.fetchone()
            
            if not campaign:
                conn.close()
                return {"error": "Campaign not found"}, 404
            
            closed_deals = campaign['closed_deals'] or 0
            
            # Buscar estatísticas de leads (campaign_leads)
            cur.execute(
                """
                SELECT 
                    COUNT(*) as total_leads,
                    COUNT(CASE WHEN status = 'sent' THEN 1 END) as sent,
                    COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending,
                    COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed,
                    COUNT(CASE WHEN status = 'invalid' THEN 1 END) as invalid,
                    COUNT(CASE WHEN status = 'pending' AND current_step = 1 AND COALESCE(removed_from_funnel, FALSE) = FALSE THEN 1 END) as pending_initial,
                    MIN(sent_at) as started_at,
                    MAX(sent_at) as last_sent_at
                FROM campaign_leads
                WHERE campaign_id = %s
                """,
                (campaign_id,)
            )
            stats = cur.fetchone()
        
        conn.close()
        
        sent = stats['sent'] or 0
        pending = stats['pending'] or 0
        failed = stats['failed'] or 0
        total_leads = stats['total_leads'] or 0
        uazapi_debug = {}
        uazapi_scheduled = 0

        # Pré-carregar stage_progress (campaign_stage_sends) — usado para cadência e headline.
        # Importante: reconciliação cadência usa a mesma conn — não fechar antes de
        # ``_reconciled_uazapi_cadence_counts_via_stage_progress`` (evita "connection already closed").
        conn_stage = get_db_connection()
        cadence_rec = None
        try:
            stage_progress = _get_campaign_stage_progress(conn_stage, campaign_id)

            has_scheduled_chunk = False
            if campaign.get('use_uazapi_sender') and campaign.get('enable_cadence'):
                with conn_stage.cursor() as cur_sched:
                    cur_sched.execute(
                        "SELECT 1 FROM campaign_stage_sends WHERE campaign_id = %s AND stage = 'initial' AND status IN ('scheduled', 'waiting_reconnect') LIMIT 1",
                        (campaign_id,),
                    )
                    has_scheduled_chunk = cur_sched.fetchone() is not None

            if campaign.get('enable_cadence') and campaign.get('use_uazapi_sender'):
                cadence_rec = _reconciled_uazapi_cadence_counts_via_stage_progress(
                    conn_stage, campaign_id, dict(campaign), total_leads
                )
        finally:
            conn_stage.close()

        stages_payload = (stage_progress or {}).get("stages") or {}

        # Agregar cadence_aggregate (todas as stages — para breakdown por etapa).
        agg_p = agg_s = agg_f = 0
        for _sk, sv in stages_payload.items():
            agg_p += int(sv.get("planned_count") or 0)
            agg_s += int(sv.get("success_count") or 0)
            agg_f += int(sv.get("failed_count") or 0)

        # --- Fonte de verdade para sent/failed/pending ---
        # Campanhas com cadência + Uazapi: mesmo núcleo que admin (SSOT ``_reconciled_uazapi_cadence_counts_via_stage_progress``).
        if cadence_rec:
            sent = cadence_rec["sent"]
            failed = cadence_rec["failed"]
            pending = cadence_rec["pending"]
            uazapi_scheduled = max(
                0,
                cadence_rec["initial_planned"]
                - cadence_rec["initial_sent_raw"]
                - cadence_rec["initial_failed_raw"],
            )
            uazapi_debug = {
                "source": "campaign_stage_sends_initial",
                "initial_sent": cadence_rec["initial_sent_raw"],
                "initial_failed": cadence_rec["initial_failed_raw"],
                "initial_planned": cadence_rec["initial_planned"],
            }

        # Outbox: contadores vêm de campaign_leads (já carregados acima); pasta legada ignorada.
        elif _campaign_has_message_outbox_rows(campaign_id):
            uazapi_debug = {"source": "campaign_leads_outbox"}

        # Campanhas Uazapi SEM cadência: manter list_folders live no folder principal.
        elif campaign.get('use_uazapi_sender') and campaign.get('uazapi_folder_id') and not campaign.get('enable_cadence'):
            try:
                rec2 = _reconciled_uazapi_single_folder_list_folders(
                    campaign_id, dict(campaign), total_leads
                )
                if rec2:
                    sent = rec2["sent"]
                    failed = rec2["failed"]
                    pending = rec2["pending"]
                    uazapi_scheduled = rec2["uazapi_scheduled"]
                    uazapi_debug = {
                        "uazapi_sent": rec2["raw_log_sent"],
                        "uazapi_failed": rec2["raw_log_failed"],
                        "uazapi_scheduled": uazapi_scheduled,
                        "source": "list_folders",
                    }
                elif campaign.get('status') == 'running' and total_leads > 0:
                    now_ts = time.time()
                    last = _stats_uazapi_warning_last.get(campaign_id, 0)
                    if now_ts - last >= STATS_UAZAPI_WARNING_COOLDOWN:
                        print(
                            f"⚠️ [Stats] Campanha {campaign_id} Uazapi: list_folders não devolveu a pasta ou API falhou. Verificar API/token."
                        )
                        _stats_uazapi_warning_last[campaign_id] = now_ts
            except Exception as e:
                uazapi_debug = {"uazapi_error": str(e)}
                print(f"⚠️ [Stats] Erro ao buscar stats Uazapi para campanha {campaign_id}: {e}")

        if total_leads > 0:
            try:
                sent = min(int(sent or 0), int(total_leads))
            except (TypeError, ValueError):
                pass

        conversion_rate = round((closed_deals / sent * 100), 1) if sent > 0 else 0

        result = {
            "total_leads": total_leads,
            "sent": sent,
            "pending": pending,
            "pending_initial": int(stats.get('pending_initial') or 0),
            "has_scheduled_chunk": has_scheduled_chunk,
            "failed": failed,
            "invalid": stats['invalid'] or 0,
            "closed_deals": closed_deals,
            "conversion_rate": conversion_rate,
            "started_at": stats['started_at'].isoformat() if stats['started_at'] else None,
            "last_sent_at": stats['last_sent_at'].isoformat() if stats['last_sent_at'] else None,
            "enable_cadence": bool(campaign.get("enable_cadence")),
            "stage_progress": stage_progress,
            "last_sync_at": stage_progress.get("last_sync_at"),
            "cadence_aggregate": {"planned": agg_p, "success": agg_s, "failed": agg_f},
        }
        if campaign.get('use_uazapi_sender') and (campaign.get('uazapi_folder_id') or campaign.get('enable_cadence')):
            result["scheduled"] = uazapi_scheduled

        if request.args.get('debug') == '1':
            result["debug"] = {
                "source": "campaign_stage_sends_initial" if (campaign.get('enable_cadence') and campaign.get('use_uazapi_sender')) else ("uazapi" if campaign.get('use_uazapi_sender') else "db"),
                "campaign_status": campaign.get('status'),
                "uazapi_folder_id": campaign.get('uazapi_folder_id'),
                **uazapi_debug,
            }

        return result
        
    except Exception as e:
        print(f"Erro ao obter stats da campanha: {e}")
        return {"error": str(e)}, 500


@app.route("/api/campaigns/<int:campaign_id>/messages-debug")
@login_required
def get_campaign_messages_debug(campaign_id):
    """
    Debug: retorna fontes de mensagens do step 1 (campaign_steps e campaigns.message_template).
    Útil para verificar se o Continuar consegue puxar mensagens.
    """
    campaign = Campaign.get_by_id(campaign_id, current_user.id)
    if not campaign:
        return json.dumps({"error": "Campanha não encontrada"}), 404
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id, step_number, message_template FROM campaign_steps WHERE campaign_id = %s AND step_number = 1 LIMIT 1",
                (campaign_id,),
            )
            step1 = cur.fetchone()
            cur.execute(
                "SELECT id, message_template FROM campaigns WHERE id = %s LIMIT 1",
                (campaign_id,),
            )
            camp = cur.fetchone()
        step1_raw = step1.get("message_template") if step1 else None
        camp_raw = camp.get("message_template") if camp else None
        step1_parsed = []
        camp_parsed = []
        for raw, out in [(step1_raw, step1_parsed), (camp_raw, camp_parsed)]:
            if not raw or not str(raw).strip():
                continue
            try:
                p = json.loads(raw)
                lst = p if isinstance(p, list) else [p]
                out.extend(str(x).strip() for x in lst if str(x).strip())
            except Exception:
                if isinstance(raw, str) and raw.strip():
                    out.append(raw.strip())
        used_source = "campaign_steps" if step1_parsed else ("campaigns" if camp_parsed else None)
        return jsonify({
            "campaign_id": campaign_id,
            "campaign_steps_step1": {
                "exists": step1 is not None,
                "raw_preview": (step1_raw[:200] + "...") if step1_raw and len(str(step1_raw)) > 200 else step1_raw,
                "parsed_count": len(step1_parsed),
            },
            "campaigns_message_template": {
                "exists": bool(camp_raw),
                "raw_preview": (str(camp_raw)[:200] + "...") if camp_raw and len(str(camp_raw)) > 200 else camp_raw,
                "parsed_count": len(camp_parsed),
            },
            "used_source": used_source,
            "continuar_would_use": len(step1_parsed) or len(camp_parsed),
        })
    finally:
        conn.close()


@app.route("/api/campaigns/<int:campaign_id>/sync-uazapi", methods=["POST"])
@login_required
def sync_campaign_uazapi_stats(campaign_id):
    """
    Sincroniza campaign_leads com a API Uazapi (list_messages).
    Atualiza status sent/failed no DB a partir das mensagens retornadas.
    Útil quando list_folders/list_messages para stats retornam 0 ou estão desatualizados.
    """
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """SELECT c.id, c.uazapi_folder_id, c.use_uazapi_sender
                   FROM campaigns c
                   WHERE c.id = %s AND c.user_id = %s""",
                (campaign_id, current_user.id)
            )
            campaign = cur.fetchone()
        conn.close()
        if not campaign or not campaign.get('use_uazapi_sender'):
            return json.dumps({"error": "Campanha não usa Uazapi"}), 400

        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT i.apikey FROM campaign_instances ci
                JOIN instances i ON i.id = ci.instance_id
                WHERE ci.campaign_id = %s AND COALESCE(i.api_provider, 'megaapi') = 'uazapi'
                LIMIT 1
            """, (campaign_id,))
            inst = cur.fetchone()
        conn.close()
        if not inst or not inst.get('apikey'):
            return json.dumps({"error": "Instância Uazapi não encontrada"}), 404

        from utils.sync_uazapi import sync_campaign_leads_from_uazapi
        uazapi = UazapiService()
        token = inst['apikey']
        folder_id = campaign.get('uazapi_folder_id')
        conn = get_db_connection()
        try:
            result = sync_campaign_leads_from_uazapi(conn, campaign_id, token, folder_id, uazapi)
        finally:
            conn.close()

        return json.dumps({
            "success": True,
            "synced": {"sent": result["sent"], "failed": result["failed"]},
            "updated": {"sent": result["updated_sent"], "failed": result["updated_failed"]}
        })
    except Exception as e:
        print(f"Erro ao sincronizar stats Uazapi campanha {campaign_id}: {e}")
        return json.dumps({"error": str(e)}), 500


def _initial_chunk_materialization_outcomes(created_ids):
    """
    AC13: após materializar ``force_send_ids``, resume estado por ``send_id``/``instance_id``.
    Retorna (per_send, partial, all_failed).
    """
    if not created_ids:
        return [], False, False
    from utils.continue_initial_chunk_report import summarize_initial_chunk_materialization_rows

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, instance_id, status, uazapi_folder_id
                FROM campaign_stage_sends
                WHERE id = ANY(%s)
                ORDER BY instance_id NULLS LAST, id
                """,
                (list(created_ids),),
            )
            rows = cur.fetchall() or []
    finally:
        conn.close()

    return summarize_initial_chunk_materialization_rows([dict(r) for r in rows])


def _unlock_initial_chunks_before_force(campaign_id: int, log_label: str) -> tuple[int, int]:
    """
    Destrava instância antes de forçar chunk (checkbox admin ``cancel_scheduled``):
    - ``scheduled`` / ``waiting_reconnect`` → ``failed``
    - ``running`` / ``partial`` / ``queued`` com ``success_count + failed_count = 0`` → ``failed``
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE campaign_stage_sends SET status = 'failed', updated_at = NOW()
                WHERE campaign_id = %s AND stage = 'initial'
                  AND status IN ('scheduled', 'waiting_reconnect')
                """,
                (campaign_id,),
            )
            n_sched = cur.rowcount
            cur.execute(
                """
                UPDATE campaign_stage_sends
                SET status = 'failed',
                    last_materialize_error = COALESCE(
                        NULLIF(TRIM(last_materialize_error), ''),
                        'force_unlock: chunk ativo sem envios (success+failed=0)'
                    ),
                    updated_at = NOW()
                WHERE campaign_id = %s AND stage = 'initial'
                  AND status IN ('running', 'partial', 'queued')
                  AND COALESCE(success_count, 0) + COALESCE(failed_count, 0) = 0
                """,
                (campaign_id,),
            )
            n_stuck = cur.rowcount
        conn.commit()
        if n_sched or n_stuck:
            print(
                f"[UAZAPI] {log_label} campaign_id={campaign_id}: unlock "
                f"scheduled/waiting_reconnect={n_sched} stuck_active_zero_progress={n_stuck}"
            )
        return n_sched, n_stuck
    finally:
        conn.close()


_INITIAL_OUTBOX_SKIP_MESSAGES = {
    "campaign_not_active": "Campanha não está em status running ou pending.",
    "daily_quota_exceeded": "Limite diário de envios iniciais atingido para hoje (BRT).",
    "initial_outbox_sending": "Há envio initial em andamento (sending) na outbox.",
    "initial_outbox_in_flight": "Já há envios initial pendentes ou em envio na outbox.",
    "no_eligible_pending_leads": "Nenhum lead pendente sem linha na outbox (initial).",
    "no_pending_leads": "Nenhum lead pendente no passo 1.",
    "no_user_id": "Campanha sem user_id associado.",
    "not_uazapi_sender": "Campanha não usa envio Uazapi (use_uazapi_sender).",
    "no_uazapi_instance": "Nenhuma instância Uazapi vinculada.",
    "no_valid_send_window": "Janela de envio inválida na campanha.",
    "outbox_disabled": "Fila outbox desabilitada (USE_MESSAGE_OUTBOX).",
    "persist_failed": "Falha ao gravar fila outbox.",
}


def _initial_outbox_skip_status_code(skip_reason: Optional[str]) -> int:
    if skip_reason in ("daily_quota_exceeded",):
        return 429
    if skip_reason in ("initial_outbox_sending", "initial_outbox_in_flight"):
        return 409
    if skip_reason in ("outbox_disabled",):
        return 503
    return 400


def _initial_chunk_outbox_extra_from_result(cres: dict) -> dict:
    ib = cres.get("body") if isinstance(cres.get("body"), dict) else {}
    return {
        "success": bool(cres.get("ok")),
        "message": ib.get("message") or ib.get("error"),
        "status_code": cres.get("status_code"),
        "mode": ib.get("mode", "message_outbox"),
        "rows_enqueued": ib.get("rows_enqueued", 0),
        "skip_reason": ib.get("skip_reason") or ib.get("code"),
        "diagnostics": ib.get("diagnostics"),
    }


def _force_uazapi_initial_chunk_no_cadence(
    campaign_id, user_id, log_label, cancel_scheduled, campaign
):
    """
    Enfileira lote initial via ``campaign_message_outbox`` (``schedule_next_initial_outbox_batch``,
    ``force=True``). Usado por force, resume e ``continue-initial-chunk`` (com ou sem cadência).
    """
    from worker_message_outbox import (
        diagnose_initial_outbox_enqueue,
        schedule_next_initial_outbox_batch,
    )

    if cancel_scheduled:
        _unlock_initial_chunks_before_force(campaign_id, log_label)

    camp_row = dict(campaign)
    camp_row["user_id"] = user_id
    conn_o = get_db_connection()
    try:
        n_rows, skip_reason = schedule_next_initial_outbox_batch(
            conn_o, camp_row, force=True
        )
    finally:
        conn_o.close()

    if n_rows <= 0:
        conn_diag = get_db_connection()
        try:
            diag = diagnose_initial_outbox_enqueue(conn_diag, campaign_id)
        finally:
            conn_diag.close()
        return {
            "ok": False,
            "status_code": _initial_outbox_skip_status_code(skip_reason),
            "body": {
                "success": False,
                "error": _INITIAL_OUTBOX_SKIP_MESSAGES.get(
                    skip_reason,
                    "Não foi possível enfileirar outbox (cota diária ou sem leads elegíveis).",
                ),
                "code": skip_reason or "outbox_enqueue_empty",
                "mode": "message_outbox",
                "rows_enqueued": 0,
                "skip_reason": skip_reason,
                "diagnostics": diag,
            },
        }

    print(
        f"[UAZAPI] {log_label} outbox enqueue campaign_id={campaign_id} "
        f"rows={n_rows} (schedule_next_initial_outbox_batch force=True)"
    )
    return {
        "ok": True,
        "status_code": 200,
        "body": {
            "success": True,
            "message": f"{n_rows} envio(s) enfileirado(s) (outbox /send/text).",
            "mode": "message_outbox",
            "rows_enqueued": n_rows,
            "skip_reason": None,
        },
    }


def _continue_initial_chunk_core(campaign_id, user_id, log_label="continue-initial-chunk", cancel_scheduled=False):
    """
    Enfileira lote initial via ``campaign_message_outbox`` (``schedule_next_initial_outbox_batch``,
    ``force=True``). Retorna dict: ok (bool), status_code, body (dict JSON-serializável).
    """
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """SELECT c.id, c.name, c.message_template, c.use_uazapi_sender, c.enable_cadence,
                          c.delay_min_minutes, c.delay_max_minutes, c.daily_limit,
                          c.send_hour_start, c.send_hour_end, c.send_saturday, c.send_sunday,
                          c.scheduled_start, c.rotation_mode, c.status
                   FROM campaigns c
                   WHERE c.id = %s AND c.user_id = %s""",
                (campaign_id, user_id),
            )
            campaign = cur.fetchone()
        conn.close()
        if not campaign:
            return {"ok": False, "status_code": 404, "body": {"error": "Campanha não encontrada"}}
        if not campaign.get("use_uazapi_sender"):
            return {"ok": False, "status_code": 400, "body": {"error": "Campanha não usa Uazapi"}}
        return _force_uazapi_initial_chunk_no_cadence(
            campaign_id, user_id, log_label, cancel_scheduled, campaign
        )
    except Exception as e:
        print(f"Erro ao continuar chunk inicial campanha {campaign_id}: {e}")
        return {"ok": False, "status_code": 500, "body": {"error": str(e)}}


@app.route("/api/campaigns/<int:campaign_id>/continue-initial-chunk", methods=["POST"])
@login_required
def continue_initial_chunk(campaign_id):
    """
    Próximo lote initial: enfileira ``campaign_message_outbox`` (``/send/text``).
    Body: {"cancel_scheduled": true} — libera chunks legados agendados antes de enfileirar.
    """
    data = request.get_json(silent=True) or {}
    cancel_scheduled = data.get("cancel_scheduled") is True
    r = _continue_initial_chunk_core(
        campaign_id, current_user.id,
        log_label="continue-initial-chunk",
        cancel_scheduled=cancel_scheduled,
    )
    return json.dumps(r["body"]), r["status_code"]


def _get_uazapi_instance_for_campaign(campaign_id, user_id, admin_mode=False):
    """
    Retorna (token, instance_id) da primeira instância Uazapi vinculada à campanha.
    Retorna (None, None) se não encontrar.
    """
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if admin_mode:
                cur.execute(
                    "SELECT c.id FROM campaigns c WHERE c.id = %s", (campaign_id,)
                )
            else:
                cur.execute(
                    "SELECT c.id FROM campaigns c WHERE c.id = %s AND c.user_id = %s",
                    (campaign_id, user_id)
                )
            if not cur.fetchone():
                return None, None
            cur.execute("""
                SELECT i.id as instance_id, i.apikey
                FROM campaign_instances ci
                JOIN instances i ON i.id = ci.instance_id
                WHERE ci.campaign_id = %s AND COALESCE(i.api_provider, 'megaapi') = 'uazapi'
                LIMIT 1
            """, (campaign_id,))
            inst = cur.fetchone()
        if not inst or not inst.get('apikey'):
            return None, None
        return inst['apikey'], inst['instance_id']
    finally:
        conn.close()


def _get_uazapi_instances_for_campaign(campaign_id, user_id):
    """Retorna todas as instâncias Uazapi vinculadas à campanha e ao usuário."""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id FROM campaigns WHERE id = %s AND user_id = %s",
                (campaign_id, user_id),
            )
            if not cur.fetchone():
                return []
            cur.execute(
                """
                SELECT i.id AS instance_id, i.apikey
                FROM campaign_instances ci
                JOIN instances i ON i.id = ci.instance_id
                WHERE ci.campaign_id = %s
                  AND COALESCE(i.api_provider, 'megaapi') = 'uazapi'
                  AND i.apikey IS NOT NULL
                ORDER BY i.id ASC
                """,
                (campaign_id,),
            )
            rows = cur.fetchall() or []
        return rows
    finally:
        conn.close()


def _resolve_uazapi_remote_jid(uazapi, token):
    """
    Resolve remote_jid ativo da instância no momento do envio.
    Não bloqueia fluxo: retorna None em falha.
    """
    try:
        result = uazapi.get_status(token)
        if not isinstance(result, dict):
            return None
        remote_jid = result.get('id') or result.get('me')
        if not remote_jid and isinstance(result.get('instance_data'), dict):
            instance_data = result.get('instance_data') or {}
            remote_jid = instance_data.get('phone') or instance_data.get('user') or instance_data.get('jid')
        return remote_jid
    except Exception:
        return None


def _stage_label_from_step(step):
    return {1: 'initial', 2: 'follow1', 3: 'follow2', 4: 'breakup'}.get(step)


def _parse_iso_datetime_local(raw_value):
    """
    Parse de datetime ISO para UTC naive (armazenamento consistente no backend/DB).
    - Se vier sem timezone (caso do input datetime-local do navegador), assume America/Sao_Paulo.
    - Se vier com timezone explícito, respeita e converte para UTC.
    Retorna None quando valor ausente/inválido.
    """
    if not raw_value:
        return None
    try:
        if isinstance(raw_value, datetime):
            parsed = raw_value
        else:
            text = str(raw_value).strip()
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            # Inputs do modal chegam sem timezone; assumir horário de Brasília.
            parsed = BRAZIL_TZ.localize(parsed)
        return parsed.astimezone(pytz.UTC).replace(tzinfo=None)
    except Exception:
        return None


def _status_rank_for_stage_send(status):
    # Maior valor = estado mais "avançado" na resolução da etapa.
    ranks = {"failed": 1, "scheduled": 2, "running": 3, "partial": 4, "inconsistent": 5, "done": 6}
    return ranks.get((status or "").lower(), 0)


def _get_campaign_stage_progress(conn, campaign_id):
    """
    Agrega progresso por etapa e por instância a partir de campaign_stage_sends.
    Retorna payload serializável para Kanban/lista.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT stage, instance_id, instance_remote_jid, uazapi_folder_id, status,
                   planned_count, success_count, failed_count, last_sync_at, scheduled_for
            FROM campaign_stage_sends
            WHERE campaign_id = %s
            ORDER BY stage ASC, instance_id ASC, created_at ASC
            """,
            (campaign_id,),
        )
        rows = cur.fetchall() or []

    stage_keys = ("initial", "follow1", "follow2", "breakup")
    stage_data = {
        k: {
            "status": "idle",
            "planned_count": 0,
            "success_count": 0,
            "failed_count": 0,
            "last_sync_at": None,
            "instances": [],
        }
        for k in stage_keys
    }
    grouped = {}
    global_last_sync = None

    for row in rows:
        stage = row.get("stage")
        if stage not in stage_data:
            continue
        key = (stage, int(row.get("instance_id") or 0))
        bucket = grouped.get(key)
        if not bucket:
            bucket = {
                "instance_id": int(row.get("instance_id") or 0),
                "instance_remote_jid": row.get("instance_remote_jid"),
                "status": row.get("status") or "scheduled",
                "planned_count": 0,
                "success_count": 0,
                "failed_count": 0,
                "last_sync_at": row.get("last_sync_at"),
                "scheduled_for": row.get("scheduled_for"),
            }
            grouped[key] = bucket

        bucket["planned_count"] += int(row.get("planned_count") or 0)
        bucket["success_count"] += int(row.get("success_count") or 0)
        bucket["failed_count"] += int(row.get("failed_count") or 0)

        current_rank = _status_rank_for_stage_send(bucket.get("status"))
        new_rank = _status_rank_for_stage_send(row.get("status"))
        if new_rank > current_rank:
            bucket["status"] = row.get("status")

        last_sync = row.get("last_sync_at")
        if last_sync and (not bucket.get("last_sync_at") or last_sync > bucket["last_sync_at"]):
            bucket["last_sync_at"] = last_sync
        sched = row.get("scheduled_for")
        if sched and (not bucket.get("scheduled_for") or sched > bucket["scheduled_for"]):
            bucket["scheduled_for"] = sched
        if last_sync and (global_last_sync is None or last_sync > global_last_sync):
            global_last_sync = last_sync

    stage_rows = {}
    for (stage, _instance_id), bucket in grouped.items():
        stage_rows.setdefault(stage, []).append(bucket)

    for stage, instances in stage_rows.items():
        instances.sort(key=lambda x: x.get("instance_id") or 0)
        stage_total = stage_data[stage]
        for inst in instances:
            stage_total["planned_count"] += int(inst.get("planned_count") or 0)
            stage_total["success_count"] += int(inst.get("success_count") or 0)
            stage_total["failed_count"] += int(inst.get("failed_count") or 0)
            inst_sync = inst.get("last_sync_at")
            if inst_sync and (stage_total["last_sync_at"] is None or inst_sync > stage_total["last_sync_at"]):
                stage_total["last_sync_at"] = inst_sync
            inst["last_sync_at"] = inst_sync.isoformat() if inst_sync else None
            inst_sched = inst.get("scheduled_for")
            inst["scheduled_for"] = inst_sched.isoformat() if inst_sched else None
            stage_total["instances"].append(inst)

        stage_status = "idle"
        if instances:
            statuses = [(i.get("status") or "scheduled").lower() for i in instances]
            if all(s == "done" for s in statuses):
                stage_status = "done"
            elif any(s == "inconsistent" for s in statuses):
                stage_status = "inconsistent"
            elif any(s == "running" for s in statuses):
                stage_status = "running"
            elif any(s == "partial" for s in statuses):
                stage_status = "partial"
            elif any(s == "scheduled" for s in statuses):
                stage_status = "scheduled"
            elif any(s == "failed" for s in statuses):
                stage_status = "failed"
            else:
                stage_status = statuses[0]
        stage_total["status"] = stage_status
        stage_total["last_sync_at"] = stage_total["last_sync_at"].isoformat() if stage_total["last_sync_at"] else None

    return {
        "stages": stage_data,
        "last_sync_at": global_last_sync.isoformat() if global_last_sync else None,
    }


def _reconciled_uazapi_cadence_counts_via_stage_progress(conn, campaign_id, campaign_row, total_leads):
    """
    SSOT com ``/api/campaigns/<id>/stats`` (Minhas Campanhas): enviados/pendentes/falhas da etapa
    ``initial`` via ``_get_campaign_stage_progress`` — não usar ``SUM(success_count)`` cru em SQL
    (agregação por instância + cap ``min(sent, total_leads)`` alinhados à UI do utilizador).
    """
    if not (campaign_row.get("enable_cadence") and campaign_row.get("use_uazapi_sender")):
        return None
    stage_progress = _get_campaign_stage_progress(conn, campaign_id)
    stages_payload = (stage_progress or {}).get("stages") or {}
    initial_data = stages_payload.get("initial") or {}
    initial_sent = int(initial_data.get("success_count") or 0)
    initial_failed = int(initial_data.get("failed_count") or 0)
    initial_planned = int(initial_data.get("planned_count") or 0)
    sent = initial_sent
    failed = initial_failed
    pending = max(0, int(total_leads or 0) - sent - failed)
    if total_leads and int(total_leads) > 0:
        try:
            sent = min(int(sent or 0), int(total_leads))
        except (TypeError, ValueError):
            pass
    return {
        "sent": sent,
        "pending": pending,
        "failed": failed,
        "initial_planned": initial_planned,
        "initial_sent_raw": initial_sent,
        "initial_failed_raw": initial_failed,
    }


def _reconciled_uazapi_single_folder_list_folders(campaign_id, campaign_row, total_leads):
    """
    SSOT com ``/api/campaigns/<id>/stats`` para Uazapi **sem** cadência: pasta principal
    em ``campaigns.uazapi_folder_id`` + ``list_folders`` (não usar só ``COUNT`` de leads em ``sent``).

    Campanhas migradas para ``campaign_message_outbox`` usam ``campaign_leads`` / outbox — a pasta
    legada em ``uazapi_folder_id`` fica desatualizada e não deve sobrescrever os contadores.
    """
    if _campaign_has_message_outbox_rows(campaign_id):
        return None
    if not (
        campaign_row.get("use_uazapi_sender")
        and campaign_row.get("uazapi_folder_id")
        and not campaign_row.get("enable_cadence")
    ):
        return None
    conn2 = get_db_connection()
    try:
        with conn2.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT i.apikey FROM campaign_instances ci
                JOIN instances i ON i.id = ci.instance_id
                WHERE ci.campaign_id = %s AND COALESCE(i.api_provider, 'megaapi') = 'uazapi'
                LIMIT 1
                """,
                (campaign_id,),
            )
            inst = cur.fetchone()
    finally:
        conn2.close()
    if not inst or not inst.get("apikey"):
        return None
    uazapi = UazapiService()
    folder_id = campaign_row.get("uazapi_folder_id")
    token = inst["apikey"]
    folders = uazapi.list_folders(token)
    if isinstance(folders, dict):
        folders = folders.get("folders") or folders.get("data") or folders.get("items") or []
    if not isinstance(folders, list):
        return None
    for f in folders:
        fid = f.get("id") or f.get("folder_id") or f.get("folderId")
        if str(fid) != str(folder_id):
            continue
        raw_sent = int(f.get("log_sucess", 0) or f.get("log_delivered", 0) or f.get("log_success", 0) or 0)
        raw_failed = int(f.get("log_failed", 0) or 0)
        log_total = int(f.get("log_total", 0) or 0)
        uazapi_scheduled = max(0, log_total - raw_sent - raw_failed)
        sent = raw_sent
        failed = raw_failed
        pending = max(0, int(total_leads or 0) - sent - failed) if total_leads else uazapi_scheduled
        if total_leads and int(total_leads) > 0:
            try:
                sent = min(int(sent or 0), int(total_leads))
            except (TypeError, ValueError):
                pass
        return {
            "sent": sent,
            "pending": pending,
            "failed": failed,
            "uazapi_scheduled": uazapi_scheduled,
            "raw_log_sent": raw_sent,
            "raw_log_failed": raw_failed,
        }
    return None


def _is_previous_stage_fully_done(campaign_id, step):
    """
    Regra fechada: próxima etapa só libera quando etapa anterior estiver done em todas as instâncias.
    Step 2 usa campanha inicial (folder principal); Step 3/4 usa campaign_stage_sends.
    Step 4 (Break-up): libera também se não houver sends FU2, se todos estiverem done, ou se não
    houver sub-campanha FU2 em scheduled/running (envio já concluído na API mas sync pendente).
    """
    if step <= 1:
        return True
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if step == 2:
                cur.execute(
                    """
                    SELECT
                        COUNT(*) AS total,
                        COALESCE(SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END), 0) AS done_count
                    FROM campaign_stage_sends
                    WHERE campaign_id = %s AND stage = 'initial'
                    """,
                    (campaign_id,),
                )
                stage_initial = cur.fetchone() or {}
                total_initial = int(stage_initial.get('total') or 0)
                done_initial = int(stage_initial.get('done_count') or 0)
                if total_initial > 0:
                    return total_initial == done_initial
                cur.execute(
                    "SELECT use_uazapi_sender, uazapi_folder_id FROM campaigns WHERE id = %s",
                    (campaign_id,),
                )
                row = cur.fetchone()
                return bool(row and row.get('use_uazapi_sender') and row.get('uazapi_folder_id'))
            prev_stage = _stage_label_from_step(step - 1)
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COALESCE(SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END), 0) AS done_count
                FROM campaign_stage_sends
                WHERE campaign_id = %s AND stage = %s
                """,
                (campaign_id, prev_stage),
            )
            row = cur.fetchone() or {}
            total = int(row.get('total') or 0)
            done_count = int(row.get('done_count') or 0)
            # Break-up (step 4): FU2 pode não ter linha em campaign_stage_sends (só rollover),
            # ou já ter enviado na API mas sync ainda não marcou todos como done.
            if step == 4:
                if total == 0:
                    return True
                if total > 0 and total == done_count:
                    return True
                cur.execute(
                    """
                    SELECT COUNT(*) AS n FROM campaign_stage_sends
                    WHERE campaign_id = %s AND stage = %s
                      AND status IN ('scheduled', 'running')
                    """,
                    (campaign_id, prev_stage),
                )
                active = int((cur.fetchone() or {}).get('n') or 0)
                if active == 0:
                    return True
                return False
            return total > 0 and total == done_count
    finally:
        conn.close()


def _create_stage_campaign(campaign_id):
    """Cria campanhas por etapa com 1 folder por instância Uazapi."""
    from utils.limits import can_create_campaign_today
    from utils.sync_uazapi import _normalize_phone_for_api

    data = request.get_json() or {}
    step = data.get('step')
    if step not in (2, 3, 4):
        return json.dumps({"error": "Use esta rota apenas para follow-ups (step 2-4). Etapa inicial nasce na criação da campanha."}), 400
    scheduled_for = _parse_iso_datetime_local(
        data.get("scheduled_for") or data.get("scheduled_at") or data.get("scheduled_start")
    )
    schedule_mode = bool(scheduled_for and scheduled_for > datetime.utcnow())
    requested_instance_ids = data.get("instance_ids") or []
    if not isinstance(requested_instance_ids, list):
        requested_instance_ids = []
    requested_instance_ids = {int(v) for v in requested_instance_ids if str(v).isdigit()}

    def _coerce_delay_minutes(value, default_value):
        try:
            parsed = int(value)
            if parsed < 1:
                return 1
            if parsed > 60:
                return 60
            return parsed
        except Exception:
            return default_value

    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """SELECT c.id, c.user_id, c.use_uazapi_sender, c.enable_cadence, c.cadence_config,
                      c.delay_min_minutes, c.delay_max_minutes, c.message_template
               FROM campaigns c WHERE c.id = %s AND c.user_id = %s""",
            (campaign_id, current_user.id),
        )
        campaign = cur.fetchone()
    conn.close()

    if not campaign:
        return json.dumps({"error": "Campanha não encontrada"}), 404
    if not campaign.get('use_uazapi_sender'):
        return json.dumps({"error": "Campanha não usa Uazapi"}), 400
    if not _is_previous_stage_fully_done(campaign_id, step):
        return json.dumps({"error": "Etapa anterior ainda não está concluída em todas as instâncias."}), 409

    instances = _get_uazapi_instances_for_campaign(campaign_id, current_user.id)
    if not instances:
        return json.dumps({"error": "Nenhuma instância Uazapi vinculada à campanha"}), 400

    allowed_instances = []
    for inst in instances:
        if can_create_campaign_today(inst['instance_id']):
            allowed_instances.append(inst)
    if requested_instance_ids:
        allowed_instances = [
            inst for inst in allowed_instances
            if int(inst.get("instance_id") or 0) in requested_instance_ids
        ]
    if not allowed_instances:
        if requested_instance_ids:
            return json.dumps({"error": "Nenhuma das instâncias selecionadas está disponível para envio nesta etapa."}), 400
        return json.dumps({"error": "Limite diário de criação por instância atingido (tente após meia-noite BRT)."}), 429

    per_instance_limit = 30
    total_limit = per_instance_limit * len(allowed_instances)
    stage = _stage_label_from_step(step)
    delay_min_minutes = _coerce_delay_minutes(
        data.get("delay_min_minutes"),
        int(campaign.get("delay_min_minutes") or 5),
    )
    delay_max_minutes = _coerce_delay_minutes(
        data.get("delay_max_minutes"),
        int(campaign.get("delay_max_minutes") or 15),
    )
    if delay_max_minutes < delay_min_minutes:
        delay_max_minutes = delay_min_minutes

    raw_variations = data.get("message_variations") or []
    if isinstance(raw_variations, str):
        raw_variations = [raw_variations]
    custom_variations = []
    if isinstance(raw_variations, list):
        custom_variations = [str(v).strip() for v in raw_variations if str(v).strip()]
    if len(custom_variations) > 5:
        custom_variations = custom_variations[:5]

    if schedule_mode:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """SELECT id FROM campaign_leads
                   WHERE campaign_id = %s
                     AND status = 'sent'
                     AND current_step = %s
                     AND COALESCE(removed_from_funnel, FALSE) = FALSE
                     AND COALESCE(cadence_status, 'active') NOT IN ('converted', 'lost')
                   ORDER BY COALESCE(send_batch, 999) ASC, COALESCE(csv_row_order, id) ASC, id ASC
                   LIMIT %s""",
                (campaign_id, step, total_limit),
            )
            sched_leads = cur.fetchall() or []
        conn.close()
        if not sched_leads:
            return json.dumps({"error": "Nenhum lead elegível para esta etapa"}), 400

        if USE_MESSAGE_OUTBOX:
            conn_rot = get_db_connection()
            try:
                with conn_rot.cursor() as cur:
                    cur.execute(
                        "SELECT COALESCE(NULLIF(TRIM(rotation_mode), ''), 'single') FROM campaigns WHERE id = %s",
                        (campaign_id,),
                    )
                    row_rot = cur.fetchone()
                rotation_mode = (row_rot[0] if row_rot else "single") or "single"
            finally:
                conn_rot.close()
            n_rows = _enqueue_uazapi_stage_outbox(
                campaign_id,
                stage,
                sched_leads,
                allowed_instances,
                rotation_mode=rotation_mode,
                next_run_at=scheduled_for,
                flow="create_stage_campaign_scheduled",
            )
            if n_rows <= 0:
                return json.dumps(
                    {"error": "Nenhum envio enfileirado (leads já na outbox ou duplicados)."}
                ), 409
            return json.dumps(
                {
                    "success": True,
                    "scheduled": True,
                    "step": step,
                    "stage": stage,
                    "mode": "message_outbox",
                    "rows_enqueued": n_rows,
                    "scheduled_for": scheduled_for.isoformat(),
                }
            )

        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """SELECT COUNT(*) AS total
                   FROM campaign_leads
                   WHERE campaign_id = %s
                     AND status = 'sent'
                     AND current_step = %s
                     AND COALESCE(removed_from_funnel, FALSE) = FALSE
                     AND COALESCE(cadence_status, 'active') NOT IN ('converted', 'lost')""",
                (campaign_id, step),
            )
            total_eligible = int((cur.fetchone() or {}).get("total") or 0)
            if total_eligible <= 0:
                conn.close()
                return json.dumps({"error": "Nenhum lead elegível para esta etapa"}), 400

            created = 0
            skipped = 0
            for inst in allowed_instances:
                cur.execute(
                    """SELECT id
                       FROM campaign_stage_sends
                       WHERE campaign_id = %s
                         AND stage = %s
                         AND instance_id = %s
                         AND scheduled_for = %s
                         AND status IN ('scheduled', 'running', 'partial')
                       LIMIT 1""",
                    (campaign_id, stage, inst["instance_id"], scheduled_for),
                )
                if cur.fetchone():
                    skipped += 1
                    continue
                cur.execute(
                    """INSERT INTO campaign_stage_sends
                       (campaign_id, stage, instance_id, scheduled_for, status, planned_count, lead_ids,
                        delay_min_minutes, delay_max_minutes, message_variations)
                       VALUES (%s, %s, %s, %s, 'scheduled', 0, '[]'::jsonb, %s, %s, %s)""",
                    (
                        campaign_id,
                        stage,
                        inst["instance_id"],
                        scheduled_for,
                        delay_min_minutes,
                        delay_max_minutes,
                        json.dumps(custom_variations),
                    ),
                )
                created += 1

            if created <= 0:
                conn.close()
                return json.dumps({"error": "Já existe agendamento desta etapa/instância para esta janela."}), 409

            cur.execute("UPDATE campaigns SET status = 'running' WHERE id = %s", (campaign_id,))
        conn.commit()
        conn.close()
        return json.dumps(
            {
                "success": True,
                "scheduled": True,
                "step": step,
                "stage": stage,
                "instances_used": created,
                "count": min(total_eligible, total_limit),
                "scheduled_for": scheduled_for.isoformat(),
                "warnings": [f"{skipped} instância(s) já possuíam agendamento nesta janela."] if skipped else [],
            }
        )

    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """SELECT id, phone, whatsapp_link, name FROM campaign_leads
               WHERE campaign_id = %s
                 AND status = 'sent'
                 AND current_step = %s
                 AND COALESCE(removed_from_funnel, FALSE) = FALSE
                 AND COALESCE(cadence_status, 'active') NOT IN ('converted', 'lost')
               ORDER BY COALESCE(send_batch, 999) ASC, COALESCE(csv_row_order, id) ASC, id ASC
               LIMIT %s""",
            (campaign_id, step, total_limit),
        )
        leads = cur.fetchall()
    conn.close()
    if not leads:
        return json.dumps({"error": "Nenhum lead elegível para esta etapa"}), 400

    if USE_MESSAGE_OUTBOX:
        conn_rot = get_db_connection()
        try:
            with conn_rot.cursor() as cur:
                cur.execute(
                    "SELECT COALESCE(NULLIF(TRIM(rotation_mode), ''), 'single') FROM campaigns WHERE id = %s",
                    (campaign_id,),
                )
                row_rot = cur.fetchone()
            rotation_mode = (row_rot[0] if row_rot else "single") or "single"
        finally:
            conn_rot.close()
        n_rows = _enqueue_uazapi_stage_outbox(
            campaign_id,
            stage,
            leads[:total_limit],
            allowed_instances,
            rotation_mode=rotation_mode,
            flow="create_stage_campaign",
        )
        if n_rows <= 0:
            return json.dumps(
                {"error": "Nenhum envio enfileirado (leads já na outbox ou duplicados)."}
            ), 409
        return json.dumps(
            {
                "success": True,
                "step": step,
                "stage": stage,
                "mode": "message_outbox",
                "rows_enqueued": n_rows,
                "count": min(len(leads), total_limit),
            }
        )

    try:
        variations = json.loads(campaign.get('message_template') or '[]')
        if isinstance(variations, str):
            variations = [variations]
    except Exception:
        variations = []

    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT message_template FROM campaign_steps WHERE campaign_id = %s AND step_number = %s LIMIT 1",
            (campaign_id, step),
        )
        step_row = cur.fetchone()
    conn.close()
    step_msgs = []
    if step_row and step_row.get('message_template'):
        try:
            parsed = json.loads(step_row.get('message_template') or '[]')
            if isinstance(parsed, list):
                step_msgs = [str(x) for x in parsed if str(x).strip()]
            elif isinstance(parsed, str) and parsed.strip():
                step_msgs = [parsed.strip()]
        except Exception:
            step_msgs = []
    if custom_variations:
        step_msgs = custom_variations
    if not step_msgs:
        step_msgs = variations
    if not step_msgs:
        conn.close()
        return json.dumps({
            "error": f"Nenhuma mensagem configurada para a etapa. Configure em campaign_steps (step {step}) ou campaigns.message_template."
        }), 400

    def _chunk(lst, n):
        return [lst[i:i + n] for i in range(0, len(lst), n)]

    lead_chunks = _chunk(leads[:total_limit], per_instance_limit)
    delay_min_sec = int(delay_min_minutes * 60)
    delay_max_sec = int(delay_max_minutes * 60)
    uazapi = UazapiService()

    # Task 6: reconciliar sends da mesma etapa antes de novo create_advanced_campaign (retomada FU).
    try:
        from utils.sync_uazapi import sync_campaign_stage_sends_before_new_chunk

        conn_fu = get_db_connection()
        try:
            sync_campaign_stage_sends_before_new_chunk(conn_fu, campaign_id, uazapi, stage=stage)
        finally:
            conn_fu.close()
    except Exception as sync_ex:
        print(f"[UAZAPI] _create_stage_campaign sync pré-chunk (Task 6) campaign_id={campaign_id}: {sync_ex}")

    sends_created = []
    errors = []
    for idx, chunk in enumerate(lead_chunks):
        if idx >= len(allowed_instances):
            break
        inst = allowed_instances[idx]
        token = inst.get('apikey')
        if not token:
            continue

        messages = []
        lead_ids = []
        for lead in chunk:
            raw = lead.get('phone') or lead.get('whatsapp_link')
            norm = _normalize_phone_for_api(raw)
            if not norm:
                continue
            msg_text = random.choice(step_msgs)
            if lead.get('name'):
                nm = lead['name']
                msg_text = msg_text.replace("{nome}", nm).replace("{name}", nm).replace("{{nome}}", nm).replace("{{name}}", nm)
            messages.append({"number": norm, "type": "text", "text": msg_text})
            lead_ids.append(lead['id'])
        if not messages:
            continue

        _t_st = time.monotonic()
        result = uazapi.create_advanced_campaign(
            token, delay_min_sec, delay_max_sec, messages, info=f"Campaign {campaign_id} {stage} inst {inst['instance_id']}"
        )
        _lat_st = int((time.monotonic() - _t_st) * 1000)
        if not result or not result.get('folder_id'):
            errors.append(f"Instância {inst['instance_id']}: falha ao criar campanha")
            continue

        folder_id = result['folder_id']
        try:
            append_dispatch_audit_event(
                user_id=int(campaign["user_id"]),
                campaign_id=int(campaign_id),
                event={
                    "stage": stage,
                    "outcome": "folder_created",
                    "latency_ms": _lat_st,
                    "http_status": 200,
                    "request": {
                        "kind": "legacy_advanced_campaign",
                        "flow": "create_stage_campaign",
                        "step": step,
                        "instance_id": int(inst["instance_id"]),
                        "lead_ids_in_order": lead_ids,
                        "message_count": len(messages),
                        "delay_min_sec": delay_min_sec,
                        "delay_max_sec": delay_max_sec,
                    },
                    "response": result if isinstance(result, dict) else result,
                },
            )
        except Exception:
            pass
        instance_remote_jid = _resolve_uazapi_remote_jid(uazapi, token)
        sends_created.append({
            "instance_id": inst['instance_id'],
            "instance_remote_jid": instance_remote_jid,
            "folder_id": folder_id,
            "lead_ids": lead_ids,
            "planned_count": len(lead_ids),
        })

    if not sends_created:
        return json.dumps({"error": "Nenhuma sub-campanha foi criada", "details": errors}), 502

    conn = get_db_connection()
    with conn.cursor() as cur:
        cfg = parse_cadence_config(campaign.get('cadence_config'))
        key_fid = f'rollover_fu{step-1}_folder_id'
        key_ids = f'rollover_fu{step-1}_lead_ids'
        for send in sends_created:
            cur.execute(
                """INSERT INTO campaign_stage_sends
                   (campaign_id, stage, instance_id, instance_remote_jid, uazapi_folder_id, status, planned_count, lead_ids,
                    delay_min_minutes, delay_max_minutes, message_variations)
                   VALUES (%s, %s, %s, %s, %s, 'running', %s, %s, %s, %s, %s)
                   RETURNING id""",
                (
                    campaign_id,
                    stage,
                    send['instance_id'],
                    send.get('instance_remote_jid'),
                    send['folder_id'],
                    send['planned_count'],
                    json.dumps(send['lead_ids']),
                    delay_min_minutes,
                    delay_max_minutes,
                    json.dumps(step_msgs),
                ),
            )
            send_row = cur.fetchone()
            send_db_id = send_row[0] if send_row else None
            if step == 2 and send_db_id is not None:
                cfg = merge_fu1_folder_into_config(cfg, str(send['folder_id']), str(send_db_id))
            cur.execute(
                "INSERT INTO uazapi_instance_sends (instance_id, campaign_id) VALUES (%s, %s)",
                (send['instance_id'], campaign_id),
            )

        cfg[key_fid] = sends_created[-1]['folder_id']
        cfg[key_ids] = [lid for s in sends_created for lid in s['lead_ids']]
        cur.execute(
            "UPDATE campaigns SET cadence_config = %s::jsonb, status = 'running' WHERE id = %s",
            (json.dumps(cfg), campaign_id),
        )
    conn.commit()
    conn.close()

    return json.dumps({
        "success": True,
        "step": step,
        "stage": stage,
        "instances_used": len(sends_created),
        "count": sum(s["planned_count"] for s in sends_created),
        "folders": [{"instance_id": s["instance_id"], "folder_id": s["folder_id"], "count": s["planned_count"]} for s in sends_created],
        "warnings": errors,
    })


@app.route("/api/campaigns/<int:campaign_id>/stage-campaign", methods=["POST"])
@login_required
def stage_campaign(campaign_id):
    try:
        return _create_stage_campaign(campaign_id)
    except Exception as e:
        print(f"Erro ao criar stage campaign: {e}")
        return json.dumps({"error": str(e)}), 500


@app.route("/api/campaigns/<int:campaign_id>/gerar-campanha", methods=["POST"])
@login_required
def gerar_campanha(campaign_id):
    """Compat: redireciona para stage-campaign (follow-ups)."""
    try:
        return _create_stage_campaign(campaign_id)
    except Exception as e:
        print(f"Erro ao gerar campanha: {e}")
        return json.dumps({"error": str(e)}), 500


@app.route("/api/campaigns/<int:campaign_id>/force-complete-stage", methods=["POST"])
@login_required
def force_complete_stage(campaign_id):
    """
    Força conclusão da etapa anterior quando houver divergência de reconciliação.
    Uso controlado para destravar operação sem falso positivo silencioso.
    """
    data = request.get_json() or {}
    try:
        step = int(data.get("step") or 0)
    except Exception:
        step = 0
    if step not in (2, 3, 4):
        return json.dumps({"error": "Parâmetro step inválido"}), 400

    prev_stage = _stage_label_from_step(step - 1)
    if not prev_stage:
        return json.dumps({"error": "Etapa anterior inválida"}), 400

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id FROM campaigns WHERE id = %s AND user_id = %s",
                (campaign_id, current_user.id),
            )
            if not cur.fetchone():
                return json.dumps({"error": "Campanha não encontrada"}), 404
            cur.execute(
                """
                UPDATE campaign_stage_sends
                SET failed_count = GREATEST(COALESCE(failed_count, 0), GREATEST(COALESCE(planned_count, 0) - COALESCE(success_count, 0), 0)),
                    status = 'done',
                    last_sync_at = NOW(),
                    updated_at = NOW()
                WHERE campaign_id = %s
                  AND stage = %s
                  AND status IN ('running', 'partial', 'inconsistent', 'failed', 'scheduled')
                """,
                (campaign_id, prev_stage),
            )
            affected = cur.rowcount
        conn.commit()
    finally:
        conn.close()

    if affected <= 0:
        return json.dumps({"error": "Nenhum envio elegível para forçar conclusão"}), 409

    print(
        f"⚠️ [Stage Force Complete] campaign={campaign_id} prev_stage={prev_stage} "
        f"forced_by_user={current_user.id} rows={affected}"
    )
    return json.dumps({"success": True, "forced_stage": prev_stage, "rows": affected})


@app.route("/api/campaigns/<int:campaign_id>/sync-debug", methods=["GET"])
@login_required
def sync_debug_campaign_uazapi(campaign_id):
    """
    Debug: compara phones da API Uazapi vs campaign_leads no DB.
    Útil para diagnosticar por que o sync não atualiza status.
    """
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """SELECT c.id, c.uazapi_folder_id, c.use_uazapi_sender
                   FROM campaigns c
                   WHERE c.id = %s AND c.user_id = %s""",
                (campaign_id, current_user.id)
            )
            campaign = cur.fetchone()
        conn.close()
        if not campaign or not campaign.get('use_uazapi_sender') or not campaign.get('uazapi_folder_id'):
            return json.dumps({"error": "Campanha não usa Uazapi ou sem folder_id"}), 400

        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT i.apikey FROM campaign_instances ci
                JOIN instances i ON i.id = ci.instance_id
                WHERE ci.campaign_id = %s AND COALESCE(i.api_provider, 'megaapi') = 'uazapi'
                LIMIT 1
            """, (campaign_id,))
            inst = cur.fetchone()
        conn.close()
        if not inst or not inst.get('apikey'):
            return json.dumps({"error": "Instância Uazapi não encontrada"}), 404

        from utils.sync_uazapi import fetch_all_phones_by_status
        uazapi = UazapiService()
        token = inst['apikey']
        folder_id = campaign['uazapi_folder_id']

        sent_phones = list(fetch_all_phones_by_status(uazapi, token, folder_id, "Sent"))
        failed_phones = list(fetch_all_phones_by_status(uazapi, token, folder_id, "Failed"))

        # Raw first message (structure)
        raw_sent = uazapi.list_messages(token, folder_id, message_status="Sent", page=1, page_size=1)
        first_msg = None
        if raw_sent:
            msgs = raw_sent.get("messages") or raw_sent.get("data")
            if isinstance(msgs, list) and msgs:
                first_msg = msgs[0]
            elif isinstance(msgs, dict):
                first_msg = msgs

        # DB leads: id, phone, whatsapp_link, status, normalized
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, phone, whatsapp_link, status, current_step,
                       regexp_replace(COALESCE(phone, ''), '[^0-9]', '', 'g') as phone_norm,
                       regexp_replace(COALESCE(whatsapp_link, ''), '[^0-9]', '', 'g') as wa_norm
                FROM campaign_leads
                WHERE campaign_id = %s
            """, (campaign_id,))
            db_leads = cur.fetchall()
        conn.close()

        # Match simulation
        def _match_params(ph):
            if len(ph) <= 11 and not ph.startswith("55"):
                return (ph, "55" + ph)
            return (ph, ph)

        matched = []
        unmatched_api = []
        for ph in sent_phones:
            p1, p2 = _match_params(ph)
            found = any(
                (lead.get("phone_norm") or "") in (p1, p2) or (lead.get("wa_norm") or "") in (p1, p2)
                for lead in db_leads
            )
            if found:
                matched.append(ph)
            else:
                unmatched_api.append(ph)

        unmatched_db = []
        for lead in db_leads:
            pn = (lead.get("phone_norm") or "").strip()
            wn = (lead.get("wa_norm") or "").strip()
            if not pn and not wn:
                continue
            found = any(
                pn == ph or wn == ph or pn == ("55" + ph) or wn == ("55" + ph)
                or ("55" + pn) == ph or ("55" + wn) == ph
                for ph in sent_phones
            )
            if not found and lead.get("status") != "sent":
                unmatched_db.append({"id": lead["id"], "phone": lead["phone"], "whatsapp_link": lead["whatsapp_link"], "phone_norm": pn or "(vazio)", "wa_norm": wn or "(vazio)", "status": lead["status"]})

        return json.dumps({
            "campaign_id": campaign_id,
            "folder_id": folder_id,
            "api": {
                "sent_phones": sent_phones,
                "failed_phones": failed_phones,
                "first_message_structure": first_msg,
            },
            "db_leads_sample": [{"id": l["id"], "phone": l["phone"], "whatsapp_link": l["whatsapp_link"], "phone_norm": l.get("phone_norm"), "wa_norm": l.get("wa_norm"), "status": l["status"]} for l in db_leads[:10]],
            "match": {
                "matched_count": len(matched),
                "unmatched_from_api": unmatched_api,
                "unmatched_from_db": unmatched_db[:10],
            },
        }, indent=2)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return json.dumps({"error": str(e)}), 500


@app.route("/api/campaigns/<int:campaign_id>/deal", methods=["POST"])
@login_required
def update_campaign_deal(campaign_id):
    """API para incrementar/decrementar negócios fechados de uma campanha"""
    try:
        data = request.json
        action = data.get('action')  # 'increment' ou 'decrement'
        
        if action not in ['increment', 'decrement']:
            return {"error": "Invalid action. Use 'increment' or 'decrement'"}, 400
        
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Verificar se a campanha pertence ao usuário
            cur.execute(
                "SELECT id FROM campaigns WHERE id = %s AND user_id = %s",
                (campaign_id, current_user.id)
            )
            campaign = cur.fetchone()
            
            if not campaign:
                conn.close()
                return {"error": "Campaign not found"}, 404
            
            # Incrementar ou decrementar
            if action == 'increment':
                cur.execute(
                    "UPDATE campaigns SET closed_deals = closed_deals + 1 WHERE id = %s RETURNING closed_deals",
                    (campaign_id,)
                )
            else:  # decrement
                cur.execute(
                    "UPDATE campaigns SET closed_deals = GREATEST(closed_deals - 1, 0) WHERE id = %s RETURNING closed_deals",
                    (campaign_id,)
                )
            
            result = cur.fetchone()
            new_value = result['closed_deals']
        
        conn.commit()
        conn.close()
        
        return {
            "success": True,
            "closed_deals": new_value
        }
        
    except Exception as e:
        print(f"Erro ao atualizar deals da campanha: {e}")
        return {"error": str(e)}, 500


@app.route("/api/dashboard/overview")
@login_required
def get_dashboard_overview():
    """API para obter visão geral do dashboard do usuário"""
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Total de campanhas
            cur.execute(
                "SELECT COUNT(*) as total FROM campaigns WHERE user_id = %s",
                (current_user.id,)
            )
            total_campaigns = cur.fetchone()['total']
            
            # Campanhas ativas (status='running')
            cur.execute(
                "SELECT COUNT(*) as active FROM campaigns WHERE user_id = %s AND status = 'running'",
                (current_user.id,)
            )
            active_campaigns = cur.fetchone()['active']
            
            # Leads extraídos HOJE (de scraping_jobs - CORRIGIDO)
            cur.execute(
                """
                SELECT COALESCE(SUM(lead_count), 0) as total
                FROM scraping_jobs
                WHERE user_id = %s 
                  AND status = 'completed'
                  AND status = 'completed'
                  AND EXTRACT(MONTH FROM created_at) = EXTRACT(MONTH FROM CURRENT_DATE)
                  AND EXTRACT(YEAR FROM created_at) = EXTRACT(YEAR FROM CURRENT_DATE)
                """,
                (current_user.id,)
            )
            today_leads = cur.fetchone()['total']
            
            # Mensagens enviadas no mês (campanhas não-API 2.0, fonte DB)
            cur.execute(
                """
                SELECT COUNT(*) as count FROM campaign_leads cl
                JOIN campaigns c ON cl.campaign_id = c.id
                WHERE c.user_id = %s 
                  AND COALESCE(c.use_uazapi_sender, FALSE) = FALSE
                  AND EXTRACT(MONTH FROM cl.sent_at) = EXTRACT(MONTH FROM CURRENT_DATE)
                  AND EXTRACT(YEAR FROM cl.sent_at) = EXTRACT(YEAR FROM CURRENT_DATE)
                  AND cl.status = 'sent'
                """,
                (current_user.id,)
            )
            month_sent = cur.fetchone()['count']

            # Somar envios iniciais das campanhas API 2.0, filtrado pelo mês corrente.
            cur.execute(
                """
                SELECT COALESCE(SUM(css.success_count), 0)::int AS uazapi_sent
                FROM campaign_stage_sends css
                JOIN campaigns c ON c.id = css.campaign_id
                WHERE c.user_id = %s
                  AND COALESCE(c.use_uazapi_sender, FALSE) = TRUE
                  AND css.stage = 'initial'
                  AND EXTRACT(MONTH FROM css.created_at) = EXTRACT(MONTH FROM CURRENT_DATE)
                  AND EXTRACT(YEAR FROM css.created_at) = EXTRACT(YEAR FROM CURRENT_DATE)
                """,
                (current_user.id,)
            )
            uazapi_row = cur.fetchone()
            month_sent += int(uazapi_row['uazapi_sent'] or 0) if uazapi_row else 0
            
            # Taxa de sucesso NO MÊS (ALTERADO de hoje para mês)
            cur.execute(
                """
                SELECT 
                    COUNT(CASE WHEN cl.status = 'sent' THEN 1 END) as sent,
                    COUNT(CASE WHEN cl.status IN ('failed', 'invalid') THEN 1 END) as failed
                FROM campaign_leads cl
                JOIN campaigns c ON cl.campaign_id = c.id
                WHERE c.user_id = %s 
                  AND EXTRACT(MONTH FROM cl.sent_at) = EXTRACT(MONTH FROM CURRENT_DATE)
                  AND EXTRACT(YEAR FROM cl.sent_at) = EXTRACT(YEAR FROM CURRENT_DATE)
                """,
                (current_user.id,)
            )
            success_data = cur.fetchone()
            sent_count = month_sent
            failed_count = success_data['failed'] or 0
            total_attempted = sent_count + failed_count
            success_rate = round((sent_count / total_attempted * 100), 1) if total_attempted > 0 else 0
            
            # Total de negócios fechados (todas as campanhas)
            cur.execute(
                "SELECT COALESCE(SUM(closed_deals), 0) as total FROM campaigns WHERE user_id = %s",
                (current_user.id,)
            )
            total_deals = cur.fetchone()['total']
            
            # Taxa de conversão geral (usando mensagens mensais)
            overall_conversion = round((total_deals / month_sent * 100), 1) if month_sent > 0 else 0
        
        conn.close()
        
        return {
            "today_leads_extracted": today_leads,
            "today_messages_sent": month_sent,  # Nome mantido para compatibilidade frontend
            "today_success_rate": success_rate,  # Nome mantido para compatibilidade frontend
            "total_closed_deals": total_deals,
            "overall_conversion_rate": overall_conversion,
            "active_campaigns": active_campaigns,
            "total_campaigns": total_campaigns
        }
        
    except Exception as e:
        print(f"Erro ao obter overview do dashboard: {e}")
        return {"error": str(e)}, 500


@app.route("/api/webhooks/hotmart", methods=["POST"])
def hotmart_webhook():
    """Recebe webhooks da Hotmart"""
    hottok = request.headers.get('X-Hotmart-Hottok')
    payload = request.json
    
    service = HotmartService()
    success = service.process_webhook(payload, hottok)
    
    if success:
        return {"status": "success"}, 200
    else:
        return {"status": "error"}, 400


@app.route('/campaigns/<int:campaign_id>/edit')
@login_required
def edit_campaign(campaign_id):
    campaign = Campaign.get_by_id(campaign_id, current_user.id)
    if not campaign:
        flash("Campanha não encontrada.", "error")
        return redirect(url_for('campaigns_list'))
    return render_template('campaigns_edit.html', campaign=campaign)


@app.route('/api/campaigns/<int:campaign_id>/leads')
@login_required
def get_campaign_leads(campaign_id):
    """
    Lista paginada de leads da campanha.

    Inclui ``status`` bruto de ``campaign_leads`` e ``ui_send_status`` derivado no servidor
    (``campaign_leads`` + EXISTS em ``campaign_message_outbox`` com ``status='sent'`` +
    sinais ``last_sent_stage`` / ``last_message_sent_at``). O filtro ``?status=`` continua a
    aplicar-se só à coluna ``campaign_leads.status``.
    """
    campaign = Campaign.get_by_id(campaign_id, current_user.id)
    if not campaign:
        return json.dumps({'error': 'Campanha não encontrada'}), 404
        
    page = request.args.get('page', 1, type=int)
    per_page = 50
    offset = (page - 1) * per_page
    
    # Filters
    name_filter = request.args.get('name', '')
    phone_filter = request.args.get('phone', '')
    status_filter = request.args.get('status', '')

    outbox_sent_expr = sql_expr_campaign_lead_has_outbox_sent("campaign_leads")

    conn = get_db_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Build query dynamically
        base_query = "FROM campaign_leads WHERE campaign_id = %s"
        params = [campaign_id]
        
        if name_filter:
            base_query += " AND name ILIKE %s"
            params.append(f"%{name_filter}%")
        if phone_filter:
            base_query += " AND phone ILIKE %s"
            params.append(f"%{phone_filter}%")
        if status_filter:
            base_query += " AND status = %s"
            params.append(status_filter)

        # Count total filtered
        cur.execute(f"SELECT COUNT(*) as count {base_query}", tuple(params))
        total = cur.fetchone()['count']
        
        # Fetch filtered leads (colunas extras para ui_send_status e depuração)
        query = f"""
            SELECT id, phone, name, whatsapp_link, status, log, sent_at,
                   last_sent_stage, last_message_sent_at, current_step, cadence_status,
                   ({outbox_sent_expr}) AS outbox_has_sent
            {base_query}
            ORDER BY COALESCE(csv_row_order, id) ASC, id ASC
            LIMIT %s OFFSET %s
        """
        params.extend([per_page, offset])
        
        cur.execute(query, tuple(params))
        leads = cur.fetchall()
    conn.close()

    serialized_leads = []
    for l in leads:
        row = dict(l)
        row["sent_at"] = row["sent_at"].isoformat() if row.get("sent_at") else None
        row["last_message_sent_at"] = (
            row["last_message_sent_at"].isoformat() if row.get("last_message_sent_at") else None
        )
        row["ui_send_status"] = compute_ui_send_status_for_lead_row(l)
        row.pop("outbox_has_sent", None)
        serialized_leads.append(row)

    return json.dumps({
        'leads': serialized_leads,
        'total': total,
        'page': page,
        'pages': (total + per_page - 1) // per_page
    }, default=str)


@app.route('/api/campaigns/<int:campaign_id>/leads/<int:lead_id>', methods=['DELETE'])
@login_required
def delete_campaign_lead(campaign_id, lead_id):
    """API para excluir um lead específico de uma campanha"""
    # 1. Verificar permissão (Campanha pertence ao usuário?)
    campaign = Campaign.get_by_id(campaign_id, current_user.id)
    if not campaign:
        return json.dumps({'error': 'Campanha não encontrada ou acesso negado'}), 404
        
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # 2. Verificar se o lead pertence à campanha
            cur.execute("SELECT id FROM campaign_leads WHERE id = %s AND campaign_id = %s", (lead_id, campaign_id))
            if not cur.fetchone():
                return json.dumps({'error': 'Lead não encontrado nesta campanha'}), 404
            
            # 3. Excluir lead
            cur.execute("DELETE FROM campaign_leads WHERE id = %s", (lead_id,))
            
        conn.commit()
    except Exception as e:
        if 'conn' in locals(): conn.rollback()
        return json.dumps({'error': str(e)}), 500
    finally:
        if 'conn' in locals(): conn.close()
        
    return json.dumps({'success': True})


@app.route('/api/templates', methods=['GET', 'POST'])
@login_required
def message_templates():
    if request.method == 'POST':
        data = request.json
        name = data.get('name')
        content = data.get('content')
        
        if not name or not content:
            return json.dumps({'error': 'Nome e conteúdo obrigatórios'}), 400
            
        tpl = MessageTemplate.create(current_user.id, name, content)
        return json.dumps({'id': tpl.id, 'name': tpl.name, 'content': tpl.content})
        
    else:
        templates = MessageTemplate.get_by_user(current_user.id)
        return json.dumps([vars(t) for t in templates], default=str)


@app.route('/api/campaigns/<int:campaign_id>/update', methods=['POST'])
@login_required
def update_campaign(campaign_id):
    campaign = Campaign.get_by_id(campaign_id, current_user.id)
    if not campaign:
        return json.dumps({'error': 'Campanha não encontrada'}), 404
    
    data = request.json
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            if 'name' in data:
                cur.execute("UPDATE campaigns SET name = %s WHERE id = %s", (data['name'], campaign_id))
            
            if 'scheduled_start' in data:
                val = data['scheduled_start']
                if val:
                    try:
                        parsed = datetime.fromisoformat(val.replace('Z', '+00:00'))
                        if parsed.tzinfo is None:
                            parsed = BRAZIL_TZ.localize(parsed)
                        val = parsed.astimezone(pytz.UTC).replace(tzinfo=None)
                    except Exception:
                        pass
                cur.execute("UPDATE campaigns SET scheduled_start = %s WHERE id = %s", (val, campaign_id))
                 
            if 'message_templates' in data:
                templates = json.dumps(data['message_templates'])
                cur.execute("UPDATE campaigns SET message_template = %s WHERE id = %s", (templates, campaign_id))
                
        conn.commit()
        return json.dumps({'success': True})
    except Exception as e:
        conn.rollback()
        return json.dumps({'error': str(e)}), 500
    finally:
        conn.close()


@app.route('/api/campaigns/<int:campaign_id>/steps/<int:step_number>', methods=['GET'])
@login_required
def get_step_template(campaign_id, step_number):
    """Retorna o template de mensagem de um passo específico"""
    campaign = Campaign.get_by_id(campaign_id, current_user.id)
    if not campaign:
        return json.dumps({'error': 'Campanha não encontrada'}), 404
        
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT message_template 
                FROM campaign_steps 
                WHERE campaign_id = %s AND step_number = %s
            """, (campaign_id, step_number))
            row = cur.fetchone()
            
            if not row:
                return json.dumps({'error': 'Passo não encontrado'}), 404
                
            # Parse template (handle string or list)
            tpl = row['message_template']
            if not tpl:
                tpl = []
            elif isinstance(tpl, str):
                try:
                    loaded = json.loads(tpl)
                    if isinstance(loaded, list):
                        tpl = loaded
                    else:
                        tpl = [tpl] # Legacy string format
                except:
                    tpl = [tpl] # Legacy plain string
            
            return json.dumps({'template': tpl})
            
    except Exception as e:
        return json.dumps({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/campaigns/<int:campaign_id>/steps/<int:step_number>', methods=['POST'])
@login_required
def update_step_template(campaign_id, step_number):
    """Atualiza o template de mensagem de um passo específico"""
    campaign = Campaign.get_by_id(campaign_id, current_user.id)
    if not campaign:
        return json.dumps({'error': 'Campanha não encontrada'}), 404
        
    data = request.json
    template_list = data.get('template')
    
    if not isinstance(template_list, list):
        return json.dumps({'error': 'Formato inválido. Esperado lista de mensagens.'}), 400
        
    # Filter empty strings
    template_list = [t.strip() for t in template_list if t and t.strip()]
    
    if not template_list:
        return json.dumps({'error': 'A mensagem não pode estar vazia.'}), 400
        
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Check if step exists
            cur.execute("SELECT 1 FROM campaign_steps WHERE campaign_id = %s AND step_number = %s", (campaign_id, step_number))
            if not cur.fetchone():
                return json.dumps({'error': 'Passo não encontrado'}), 404
            
            # Update
            json_tpl = json.dumps(template_list, ensure_ascii=False)
            cur.execute("""
                UPDATE campaign_steps 
                SET message_template = %s 
                WHERE campaign_id = %s AND step_number = %s
            """, (json_tpl, campaign_id, step_number))
            
        conn.commit()
        return json.dumps({'success': True})
            
    except Exception as e:
        conn.rollback()
        return json.dumps({'error': str(e)}), 500
    finally:
        conn.close()


@app.route('/api/campaigns/<int:campaign_id>/replace-leads', methods=['POST'])
@login_required
def replace_leads(campaign_id):
    campaign = Campaign.get_by_id(campaign_id, current_user.id)
    if not campaign:
        return json.dumps({'error': 'Campanha não encontrada'}), 404
        
    data = request.json
    job_id = data.get('job_id')
    
    if not job_id:
        return json.dumps({'error': 'Job ID obrigatório'}), 400
        
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT results_path FROM scraping_jobs WHERE id = %s AND user_id = %s", (job_id, current_user.id))
            job = cur.fetchone()
        
        if not job or not job['results_path'] or not os.path.exists(job['results_path']):
            conn.close()
            return json.dumps({'error': 'Arquivo de leads não encontrado'}), 404
            
        file_path = job['results_path']
        valid_leads = []
        
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path, dtype=str)
        elif file_path.endswith('.xlsx'):
            df = pd.read_excel(file_path, dtype=str)
        else:
             try:
                 df = pd.read_csv(file_path, dtype=str)
             except:
                 conn.close()
                 return json.dumps({'error': 'Formato de arquivo desconhecido'}), 400

        # Normalization Logic (Simplified version of create_campaign)
        cols = [c.lower() for c in df.columns]
        phone_col = next((c for c in cols if 'phone' in c or 'telefone' in c or 'celular' in c or 'whatsapp' in c), None)
        name_col = next((c for c in cols if 'name' in c or 'nome' in c), None)
        link_col = next((c for c in cols if 'link' in c or 'url' in c), None)
        status_col = next((c for c in cols if 'status' in c), None)
        
        # Helper to extract phone
        def extract_phone(link):
            if not link: return None
            import re
            patterns = [r'wa\.me/([0-9]+)', r'phone=([0-9]+)', r'whatsapp\.com/send\?phone=([0-9]+)']
            for pattern in patterns:
                match = re.search(pattern, str(link))
                if match: return match.group(1)
            digits = re.sub(r'\D', '', str(link))
            if len(digits) >= 10: return digits
            return None

        for _, row in df.iterrows():
            # Check Status if exists (1 = ready)
            # Safe comparison for string '1'
            if status_col:
                val = str(row.iloc[cols.index(status_col)]).strip()
                if val != '1':
                    continue
                
            lead_data = {}
            if name_col: lead_data['name'] = str(row.iloc[cols.index(name_col)])
            
            raw_phone = None
            raw_link = None
            
            if phone_col: raw_phone = str(row.iloc[cols.index(phone_col)])
            if link_col: raw_link = str(row.iloc[cols.index(link_col)])
            
            final_phone = extract_phone(raw_link) or extract_phone(raw_phone)
            
            if final_phone:
                lead_data['phone'] = final_phone
                lead_data['whatsapp_link'] = raw_link if raw_link else f"https://wa.me/{final_phone}"
                valid_leads.append(lead_data)
        
        if not valid_leads:
             conn.close()
             return json.dumps({'error': 'Nenhum lead válido encontrado nesta lista (verifique filtro de status=1)'}), 400

        # Replace logic: Delete pending leads, insert new ones
        with conn.cursor() as cur:
            # Delete only pending to preserve history of sent leads
            cur.execute("DELETE FROM campaign_leads WHERE campaign_id = %s AND status = 'pending'", (campaign_id,))
            
            # Insert new (csv_row_order = ordem das linhas no CSV)
            args_str = ','.join(
                cur.mogrify("(%s, %s, %s, %s, %s, %s)", 
                           (campaign_id, l.get('phone'), l.get('name'), l.get('whatsapp_link'), 'pending', idx + 1)).decode('utf-8') 
                for idx, l in enumerate(valid_leads)
            )
            cur.execute(
                "INSERT INTO campaign_leads (campaign_id, phone, name, whatsapp_link, status, csv_row_order) VALUES "
                + args_str
            )
            
        conn.commit()
        conn.close()
        
        return json.dumps({'success': True, 'count': len(valid_leads)})
        
    except Exception as e:
        if 'conn' in locals(): conn.close()
        return json.dumps({'error': str(e)}), 500

# ==========================================
# MIGRATION ROUTE (TEMPORARY - REMOVE AFTER USE)
# ==========================================
@app.route('/migrate_cadence')
@login_required
def migrate_cadence_route():
    # Security check: only super admin
    if current_user.email not in SUPER_ADMIN_EMAILS:
        return "Unauthorized", 403
    
    conn = get_db_connection()
    log = ["<h1>Relatório de Migração de Cadência</h1>"]
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            user_id = current_user.id
            log.append(f"<p><strong>Super Admin ID:</strong> {user_id} ({current_user.email})</p>")
            
            # 2. Get all campaigns
            cur.execute("SELECT id, name FROM campaigns WHERE user_id = %s", (user_id,))
            campaigns = cur.fetchall()
            
            log.append(f"<p>Encontradas {len(campaigns)} campanhas.</p><ul>")
            
            for camp in campaigns:
                cid = camp['id']
                cname = camp['name']
                log.append(f"<li><strong>Campanha {cid} ({cname}):</strong>")
                
                # Enable cadence
                cur.execute("UPDATE campaigns SET enable_cadence = TRUE, terms_accepted = TRUE WHERE id = %s", (cid,))
                
                # Check steps
                cur.execute("SELECT count(*) as count FROM campaign_steps WHERE campaign_id = %s", (cid,))
                count = cur.fetchone()['count']
                
                if count == 0:
                    steps = [
                        (1, "Mensagem Inicial", "[]", 0),
                        (2, "Follow-up 1", "Olá, conseguiu ver minha mensagem anterior?", 1),
                        (3, "Follow-up 2", "Oi novamente! Imagino que esteja corrido. Se tiver interesse, estou por aqui.", 2),
                        (4, "Break-up", "Última tentativa. Vou encerrar meu contato por enquanto.", 3)
                    ]
                    for s_num, s_label, s_msg, s_delay in steps:
                        cur.execute("""
                            INSERT INTO campaign_steps (campaign_id, step_number, step_label, message_template, delay_days)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (cid, s_num, s_label, s_msg, s_delay))
                    log.append(" <span style='color:green'>Steps criados.</span>")
                else:
                    log.append(" <span style='color:gray'>Steps já existem.</span>")
                
                # Update leads
                cur.execute("""
                    UPDATE campaign_leads
                    SET current_step = 1,
                        cadence_status = 'snoozed',
                        snooze_until = NOW() + INTERVAL '1 day',
                        last_message_sent_at = COALESCE(last_message_sent_at, NOW())
                    WHERE campaign_id = %s
                      AND status = 'sent'
                      AND (cadence_status IS NULL OR cadence_status = 'pending')
                """, (cid,))
                leads_count = cur.rowcount
                if leads_count > 0:
                    log.append(f" <span style='color:blue'>{leads_count} leads movidos para cadência (snoozed 1 day).</span>")
                
                log.append("</li>")
            
            log.append("</ul><p><strong>Migração concluída com sucesso!</strong></p>")
                
        conn.commit()
        return "".join(log)
    except Exception as e:
        conn.rollback()
        return f"Error: {str(e)}", 500
    finally:
        conn.close()


@app.route('/migrate_notes')
@login_required
def migrate_notes_route():
    # Security check: only super admin
    if current_user.email not in SUPER_ADMIN_EMAILS:
        return "Unauthorized", 403
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE campaign_leads ADD COLUMN IF NOT EXISTS notes TEXT;")
        conn.commit()
        return "Migration notes executed successfully."
    except Exception as e:
        conn.rollback()
        return f"Error: {str(e)}", 500
    finally:
        conn.close()


@app.route('/migrate_enrichment')
@login_required
def migrate_enrichment_route():
    # Security check: only super admin
    if current_user.email not in SUPER_ADMIN_EMAILS:
        return "Unauthorized", 403
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE campaign_leads ADD COLUMN IF NOT EXISTS address TEXT;")
            cur.execute("ALTER TABLE campaign_leads ADD COLUMN IF NOT EXISTS website TEXT;")
            cur.execute("ALTER TABLE campaign_leads ADD COLUMN IF NOT EXISTS category TEXT;")
            cur.execute("ALTER TABLE campaign_leads ADD COLUMN IF NOT EXISTS location TEXT;")
            cur.execute("ALTER TABLE campaign_leads ADD COLUMN IF NOT EXISTS reviews_count FLOAT;")
            cur.execute("ALTER TABLE campaign_leads ADD COLUMN IF NOT EXISTS reviews_rating FLOAT;")
            cur.execute("ALTER TABLE campaign_leads ADD COLUMN IF NOT EXISTS latitude FLOAT;")
            cur.execute("ALTER TABLE campaign_leads ADD COLUMN IF NOT EXISTS longitude FLOAT;")
        conn.commit()
        return "Enrichment Migration executed successfully."
    except Exception as e:
        conn.rollback()
        return f"Error: {str(e)}", 500
    finally:
        conn.close()

# ==========================================
# SYNC SNOOZED CONVERSATIONS TO CHATWOOT
# ==========================================
@app.route('/sync_chatwoot_snooze')
@login_required
def sync_chatwoot_snooze_route():
    # Security check: only super admin
    if current_user.email not in SUPER_ADMIN_EMAILS:
        return "Unauthorized: Only Super Admin can run this sync", 403
        
    chatwoot_url = os.environ.get('CHATWOOT_API_URL', 'https://chatwoot.wbtech.dev')
    chatwoot_token = os.environ.get('CHATWOOT_ACCESS_TOKEN')
    chatwoot_account_id = os.environ.get('CHATWOOT_ACCOUNT_ID', '2')
    
    if not chatwoot_token:
        # Try to load from .env if not in os.environ (for local dev)
        from dotenv import load_dotenv
        load_dotenv()
        chatwoot_token = os.environ.get('CHATWOOT_ACCESS_TOKEN')
        if not chatwoot_token:
            return "Error: CHATWOOT_ACCESS_TOKEN not properly configured in env variables", 500

    log = ["<h1>Log de Sincronização Chatwoot (Snooze + Discovery v2)</h1>"]
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            user_id = current_user.id
            
            # 2. Find Snoozed Leads (Check ALL snoozed leads, even without conversation_id)
            query = """
                SELECT cl.id, cl.chatwoot_conversation_id, cl.snooze_until, cl.phone, cl.name, c.name as campaign_name
                FROM campaign_leads cl
                JOIN campaigns c ON cl.campaign_id = c.id
                WHERE c.user_id = %s
                AND cl.cadence_status = 'snoozed'
                AND cl.snooze_until > NOW()
            """
            # ==========================================
            # MANUAL CORRECTIONS (User provided)
            # ==========================================
            manual_fixes = {
                6386: 890,
                6388: 895,
                6389: 900,
                6393: 907,
                6617: 877,
                6618: 880,
                6622: 887,
                6624: 891, # Rule: Check reply/human
                6626: 901,
                6627: 903  # Rule: Label check
            }
            
            for l_id, c_id in manual_fixes.items():
                cur.execute("UPDATE campaign_leads SET chatwoot_conversation_id = %s WHERE id = %s", (c_id, l_id))
            conn.commit()
            
            # Re-fetch leads after update
            cur.execute(query, (user_id,))
            leads = cur.fetchall()
            
            log.append(f"<p>Encontrados <strong>{len(leads)} leads</strong> em estado 'snoozed' localmente.</p><ul>")
            
            # Calculate tomorrow 8 AM timestamp for Chatwoot
            tomorrow_8am = (datetime.now() + timedelta(days=1)).replace(hour=8, minute=0, second=0, microsecond=0)
            snooze_timestamp = int(tomorrow_8am.timestamp())
            log.append(f"<p>Horário de retorno definido para: <strong>{tomorrow_8am.strftime('%d/%m/%Y %H:%M')}</strong></p>")
            
            headers = {
                "api_access_token": chatwoot_token,
                "Content-Type": "application/json"
            }
            
            success_count = 0
            linked_count = 0
            
            for lead in leads:
                conv_id = lead['chatwoot_conversation_id']
                phone = lead['phone']
                name = lead['name']
                lead_id = lead['id']
                
                # If no conversation ID, try to find it
                if not conv_id:
                    try:
                        clean_phone = re.sub(r'\D', '', str(phone or ''))
                        contact_id = None
                        
                        strategies = []
                        if clean_phone:
                            strategies.append(('Raw', clean_phone))
                            strategies.append(('+Phone', f"+{clean_phone}"))
                            # Try last 9 digits (no country/area code sometimes)
                            if len(clean_phone) >= 9:
                                strategies.append(('Last9', clean_phone[-9:]))
                            # Try last 8 digits (very broad match)
                            if len(clean_phone) >= 8:
                                strategies.append(('Last8', clean_phone[-8:]))
                        
                        if name:
                            strategies.append(('Name', name))

                        for label, query_val in strategies:
                            if contact_id: break # Found!
                            
                            try:
                                search_url = f"{chatwoot_url}/api/v1/accounts/{chatwoot_account_id}/contacts/search"
                                search_resp = requests.get(search_url, params={'q': query_val}, headers=headers, timeout=5)
                                if search_resp.status_code == 200:
                                    data = search_resp.json()
                                    if data.get('payload'):
                                        # Use the first one
                                        match = data['payload'][0]
                                        contact_id = match['id']
                            except:
                                pass

                        if contact_id:
                            # Get Conversations for Contact
                            conv_url = f"{chatwoot_url}/api/v1/accounts/{chatwoot_account_id}/contacts/{contact_id}/conversations"
                            conv_resp = requests.get(conv_url, headers=headers, timeout=5)
                            
                            if conv_resp.status_code == 200:
                                conv_data = conv_resp.json()
                                if conv_data.get('payload'):
                                    # Pick the most recent one
                                    conv_id = conv_data['payload'][0]['id']
                                    
                                    # Update Database
                                    conn2 = get_db_connection()
                                    with conn2.cursor() as cur2:
                                        cur2.execute("UPDATE campaign_leads SET chatwoot_conversation_id = %s WHERE id = %s", (conv_id, lead_id))
                                    conn2.commit()
                                    conn2.close()
                                    linked_count += 1
                                    log.append(f"<li>🔗 Lead #{lead_id} ({name}): Vinculado à conversa {conv_id} (via {label})</li>")
                                else:
                                     log.append(f"<li>❓ Lead #{lead_id}: Contato encontrado (ID {contact_id}) mas sem conversas.</li>")
                        else:
                            log.append(f"<li>❌ Lead #{lead_id} ({name}): Não encontrado no Chatwoot (Tentado: {clean_phone})</li>")
                            
                    except Exception as e_discovery:
                         log.append(f"<li>⚠️ Discovery Error Lead #{lead_id}: {str(e_discovery)}</li>")

                # If we have a conversation ID (either existing or just found), snooze it
                if conv_id:
                    # SPECIAL RULES (Manual Overrides)
                    if lead_id == 6624:
                        log.append(f"<li>🛑 Lead #{lead_id} (Conv {conv_id}): <strong>Skipped Snooze</strong> (Rule: Reply/Human check required)</li>")
                        continue
                        
                    if lead_id == 6627:
                         log.append(f"<li>🛑 Lead #{lead_id} (Conv {conv_id}): <strong>Skipped Snooze</strong> (Rule: Label check required)</li>")
                         continue
                        
                    try:
                        url = f"{chatwoot_url}/api/v1/accounts/{chatwoot_account_id}/conversations/{conv_id}/toggle_status"
                        payload = {
                            "status": "snoozed",
                            "snoozed_until": snooze_timestamp
                        }
                        
                        resp = requests.post(url, json=payload, headers=headers, timeout=5)
                        
                        if resp.status_code == 200:
                            success_count += 1
                            log.append(f"<li>✅ Lead #{lead['id']} (Conv {conv_id}): <span style='color:green'>Snoozed until 8am</span></li>")
                        elif resp.status_code == 404:
                             log.append(f"<li>⚠️ Lead #{lead['id']} (Conv {conv_id}): Chatwoot 404 (Not Found)</li>")
                        else:
                            log.append(f"<li>❌ Lead #{lead['id']} (Conv {conv_id}): API Error {resp.status_code} - {resp.text}</li>")
                            
                    except Exception as ex:
                        log.append(f"<li>❌ Lead #{lead['id']} (Conv {conv_id}): Exception - {str(ex)}</li>")
                
                # Rate limit safety
                time.sleep(0.1)
                
            log.append("</ul>")
            log.append(f"<h3>Resumo: {success_count}/{len(leads)} processados. {linked_count} novos vínculos.</h3>")
            
        return "".join(log)
    except Exception as e:
        return f"Error: {str(e)}", 500
    finally:
        conn.close()

