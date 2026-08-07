"""
Worker Cadence — Processes multi-step campaign cadence follow-ups with Intelligent Checks.

Runs as a separate process alongside worker_sender.py.
For each cadence-enabled campaign:
  1. Finds leads ready for the next step.
  2. DECISION MATRIX: Checks Chatwoot Labels & Status before sending.
  3. Sends the step's message via Uazapi.
  4. POST-SEND MONITORING: Puts lead in 'monitoring' state for 5 mins to check for immediate replies.
  5. Finally snoozes or stops based on the outcome.
"""

import os
import logging
import time
import json
import random
import requests
import base64
import re
from datetime import datetime, date, timedelta
import psycopg2
from psycopg2 import errors as psycopg2_errors
from psycopg2.extras import Json, RealDictCursor
from dotenv import load_dotenv
import pytz

load_dotenv()

# Config
BUSINESS_HOUR_START = 8
BUSINESS_HOUR_END = 20
BRAZIL_TZ = pytz.timezone('America/Sao_Paulo')

# Uazapi
try:
    from services.uazapi import UazapiService
    uazapi_service = UazapiService()
except ImportError:
    uazapi_service = None

# Limites compartilhados
from utils.limits import (
    can_create_campaign_today,
    INITIAL_CHUNK_ACTIVE_SEND_STATUSES,
    check_initial_chunk_daily_quota_for_campaign,
)
from utils.sync_uazapi import (
    sync_campaign_leads_from_uazapi,
    sync_campaign_stage_sends_before_new_chunk,
    get_uazapi_campaign_counts,
    is_initial_campaign_finished,
    fetch_all_phones_by_status,
    normalize_phone_for_match,
    should_block_initial_rollover_for_pending_find,
)
from utils.uazapi_pacing import (
    build_pacing_segments_for_leads,
    default_inter_message_delay_range_minutes,
    estimate_segment_span_minutes,
    stagger_scheduled_utc_naive,
)
from utils.cadence_uazapi import merge_fu1_into_campaign_db

from utils.config import SUPER_ADMIN_EMAILS, USE_MESSAGE_OUTBOX
from utils.next_valid_uazapi_send import is_campaign_send_window, next_valid_send_utc_naive
from utils.campaign_send_policy import uazapi_initial_chunk_distribution_limits
from utils.initial_chunk_schedule_target import (
    cadence_next_send_datetime,
    resolve_initial_chunk_schedule_target,
    uazapi_same_day_initial_chunk_after_unlock_enabled,
)
# Slot matinal automático do chunk initial: ``cadence_next_initial_send_slot`` em
# ``utils/initial_chunk_schedule_target.py`` (sucessor de ``_next_initial_send_slot``).
from utils.uazapi_error_taxonomy import (
    classify_create_advanced_error,
    format_last_error_for_db,
)
from utils.uazapi_support_notify import (
    get_instance_status_cached,
    is_instance_disconnected_status,
    maybe_send_disconnect_support_whatsapp,
    enqueue_reconnect_inapp_alert,
    maybe_send_reconnect_support_whatsapp,
)

from worker_message_outbox import (
    maybe_auto_force_daily_initial_batches,
    maybe_schedule_outbox_initial_batches,
    process_message_outbox_tick,
    schedule_next_initial_outbox_batch,
    enqueue_outbox_rows_for_leads,
    supersede_legacy_follow_stage_sends,
)
from utils.outbox_prometheus import maybe_start_outbox_metrics_http_server

_logger_cadence = logging.getLogger(__name__)


def _campaign_has_message_outbox(conn, campaign_id: int) -> bool:
    """Campanha já usa fila Postgres outbox (envio unitário); cadência legado por lead deve ser ignorada."""
    if not USE_MESSAGE_OUTBOX:
        return False
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM campaign_message_outbox WHERE campaign_id = %s LIMIT 1",
            (campaign_id,),
        )
        return cur.fetchone() is not None

# Chatwoot Config
CHATWOOT_API_URL = os.environ.get('CHATWOOT_API_URL', 'https://chatwoot.wbtech.dev')
CHATWOOT_ACCESS_TOKEN = os.environ.get('CHATWOOT_ACCESS_TOKEN')
CHATWOOT_ACCOUNT_ID = os.environ.get('CHATWOOT_ACCOUNT_ID', '2')

CADENCE_POLL_INTERVAL = 30  # seconds (mais frequente para pegar scheduled_start e Continuar)
SAFETY_BUFFER_MINUTES = 5


def _parse_uazapi_disconnect_pause_check_interval_sec() -> int:
    """Intervalo entre ticks de saúde que pausam campanhas por instância desligada (60–120s recomendado na spec)."""
    raw = (os.environ.get("UAZAPI_DISCONNECT_PAUSE_CHECK_INTERVAL_SEC") or "90").strip()
    try:
        v = int(raw)
    except (TypeError, ValueError):
        v = 90
    return max(45, min(v, 600))


# Janela UTC do materialize automático (`_materialize_scheduled_stage_sends`): mesma SSOT na query SQL
# e no filtro `remaining` (segundos até scheduled_for). Ver docstring da função.
MATERIALIZE_LOOKBACK_MIN = 15
MATERIALIZE_LOOKAHEAD_MIN = 5
PRE_DISPARO_WINDOW_MIN = 2
PRE_DISPARO_WINDOW_MAX = MATERIALIZE_LOOKAHEAD_MIN
# UAZAPI — contadores de campanha (decisão travada; evitar regressão):
# SSOT dos agregados é GET /sender/listfolders (log_sucess, log_failed, log_total, status).
# Não usar listmessages como total oficial (teto/paginação do provedor).
# O intervalo abaixo casa com o filtro last_sync_at (~10 min) em _sync_active_stage_folders.
STAGE_SYNC_INTERVAL_MINUTES = 10
VERIFY_FOLDER_AFTER_SECONDS = 180  # list_folders 3 min após create_advanced_campaign

# Fila de verificação pós-create: (send_id, folder_id, token, verify_at)
_verify_folder_queue = []


def _next_retry_utc_for_materialize(camp_win, now_utc_naive, attempt_count):
    """
    Backoff exponencial (cap 30 min) alinhado à janela BRT: se cair fora, usa
    next_valid_send_utc_naive a partir de agora.
    """
    m = 2 ** min(max(0, int(attempt_count or 0)), 4)
    m = max(1, min(int(m), 30))
    cand = now_utc_naive + timedelta(minutes=m)
    if getattr(cand, "tzinfo", None) is None:
        br = pytz.UTC.localize(cand).astimezone(BRAZIL_TZ)
    else:
        br = cand.astimezone(BRAZIL_TZ)
    if is_campaign_send_window(camp_win, now_brazil=br):
        return cand
    return next_valid_send_utc_naive(
        camp_win, now_utc_naive, margin_minutes=0
    )


def _reconcile_find_before_rollover_enabled():
    """
    Task 5 + Task 7: ``sync_campaign_leads_from_uazapi`` (incl. ``message_find`` em
    ``campaign_stage_sends``) antes de rollover que chama ``create_advanced_campaign`` —
    inicial→FU1 (``process_uazapi_initial_stage_rollovers``) e FU1→FU2→Despedida por tempo
    (``process_rollover_fu_next``). Defeito ligado; ``0`` / ``false`` desliga (emergência).
    """
    return os.environ.get("UAZAPI_RECONCILE_FIND_BEFORE_ROLLOVER", "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


def _parse_json_lead_ids(raw):
    if not raw:
        return []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return []
    if not isinstance(raw, list):
        return []
    out = []
    for x in raw:
        try:
            out.append(int(x))
        except (TypeError, ValueError):
            continue
    return out


def get_db_connection():
    conn = psycopg2.connect(
        host=os.environ.get('DB_HOST', 'localhost'),
        database=os.environ.get('DB_NAME', 'leads_infinitos'),
        user=os.environ.get('DB_USER', 'postgres'),
        password=os.environ.get('DB_PASSWORD'),
        port=os.environ.get('DB_PORT', '5432'),
        cursor_factory=RealDictCursor
    )
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
    return conn

def is_business_hours():
    now_brazil = datetime.now(BRAZIL_TZ)
    return BUSINESS_HOUR_START <= now_brazil.hour < BUSINESS_HOUR_END


def format_jid(phone):
    """Formats a phone number into a WhatsApp JID."""
    clean = re.sub(r'\D', '', str(phone))
    if len(clean) <= 11 and not clean.startswith('55'):
        clean = '55' + clean
    return clean + '@s.whatsapp.net'


def _is_media_path_safe(media_path, user_id):
    """
    Valida que media_path está sob storage/{user_id}/ (segurança multi-tenant).
    """
    if not media_path or not user_id:
        return False
    if '..' in media_path:
        return False
    try:
        real_path = os.path.abspath(media_path)
        user_storage = os.path.abspath(os.path.join('storage', str(user_id)))
        return real_path.startswith(user_storage)
    except Exception:
        return False


# --- CHATWOOT HELPERS ---

def get_chatwoot_conversation_details(conversation_id):
    """
    Fetches conversation details including labels, status, and messages.
    Returns dict or None.
    """
    if not conversation_id:
        return None
        
    url = f"{CHATWOOT_API_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}"
    headers = {"api_access_token": CHATWOOT_ACCESS_TOKEN, "Content-Type": "application/json"}
    
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            return resp.json()
        return None
    except Exception as e:
        print(f"  ❌ [Chatwoot] Details Error: {e}")
        return None

def toggle_chatwoot_status(conversation_id, status, snoozed_until=None):
    """
    Toggles conversation status ('snoozed', 'open', 'resolved').
    If status is 'snoozed' and snoozed_until is provided, includes the timestamp.
    """
    if not conversation_id: return False
    
    url = f"{CHATWOOT_API_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/toggle_status"
    headers = {"api_access_token": CHATWOOT_ACCESS_TOKEN, "Content-Type": "application/json"}
    payload = {"status": status}
    
    if status == 'snoozed' and snoozed_until:
        # Chatwoot expects Unix timestamp for snoozed_until
        if hasattr(snoozed_until, 'timestamp'):
            payload["snoozed_until"] = int(snoozed_until.timestamp())
        else:
            payload["snoozed_until"] = int(snoozed_until)
    
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=10)
        if resp.status_code == 200:
            print(f"  ✅ Chatwoot status set to '{status}' for conv {conversation_id}")
        return resp.status_code == 200
    except Exception as e:
        print(f"  ❌ Chatwoot toggle error: {e}")
        return False

def add_chatwoot_labels(conversation_id, labels):
    """
    Adds labels to a conversation.
    """
    if not conversation_id: return False
    
    url = f"{CHATWOOT_API_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/labels"
    headers = {"api_access_token": CHATWOOT_ACCESS_TOKEN, "Content-Type": "application/json"}
    payload = {"labels": labels}
    
    try:
        requests.post(url, json=payload, headers=headers, timeout=10)
        return True
    except:
        return False

# --- CHATWOOT DISCOVERY ---

def get_chatwoot_conversation_messages(conversation_id):
    """Fetches messages for a conversation."""
    if not conversation_id: return []
    url = f"{CHATWOOT_API_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/conversations/{conversation_id}/messages"
    headers = {"api_access_token": CHATWOOT_ACCESS_TOKEN}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            payload = resp.json()
            return payload.get('payload', [])
    except Exception:
        pass
    return []

def discover_chatwoot_conversation(phone, name=None):
    """
    Discovers the Chatwoot conversation ID for a lead.
    Searches by phone number (multiple formats) and name as fallbacks.
    Returns conversation_id or None.
    """
    if not CHATWOOT_ACCESS_TOKEN:
        return None
    
    headers = {
        "api_access_token": CHATWOOT_ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    
    clean_phone = re.sub(r'\D', '', str(phone or ''))
    if not clean_phone and not name:
        return None
    
    # Build search strategies (ordered by specificity)
    strategies = []
    if clean_phone:
        strategies.append(('Phone+', f'+{clean_phone}'))
        strategies.append(('PhoneRaw', clean_phone))
        strategies.append(('JID', f'{clean_phone}@s.whatsapp.net'))
        if len(clean_phone) >= 9:
            strategies.append(('Last9', clean_phone[-9:]))
        if len(clean_phone) >= 8:
            strategies.append(('Last8', clean_phone[-8:]))
    if name and name.strip() and name.strip() != '.':
        strategies.append(('Name', name.strip()))
    
    contact_id = None
    matched_via = None
    
    for label, query_val in strategies:
        if contact_id:
            break
        try:
            search_url = f"{CHATWOOT_API_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts/search"
            resp = requests.get(search_url, params={'q': query_val}, headers=headers, timeout=8)
            if resp.status_code == 200:
                data = resp.json()
                if data.get('payload') and len(data['payload']) > 0:
                    contact_id = data['payload'][0]['id']
                    matched_via = label
        except Exception as e:
            pass  # Silent, will try next strategy
    
    if not contact_id:
        return None
    
    try:
        conv_url = f"{CHATWOOT_API_URL}/api/v1/accounts/{CHATWOOT_ACCOUNT_ID}/contacts/{contact_id}/conversations"
        resp = requests.get(conv_url, headers=headers, timeout=8)
        if resp.status_code == 200:
            conv_data = resp.json()
            if conv_data.get('payload') and len(conv_data['payload']) > 0:
                conv_id = conv_data['payload'][0]['id']
                print(f"  🔗 Chatwoot: Found conv {conv_id} (via {matched_via}) for contact {contact_id}")
                return conv_id
    except Exception as e:
        print(f"  ⚠️ Chatwoot conv fetch error: {e}")
    
    return None


def get_campaign_instance(campaign_id, conn):
    """Retorna instância conectada para a campanha (prioriza Uazapi)."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT i.name, i.apikey, COALESCE(i.api_provider, 'megaapi') as api_provider
            FROM campaign_instances ci
            JOIN instances i ON ci.instance_id = i.id
            WHERE ci.campaign_id = %s AND i.status = 'connected'
            ORDER BY CASE WHEN COALESCE(i.api_provider, 'megaapi') = 'uazapi' THEN 0 ELSE 1 END
            LIMIT 1
        """, (campaign_id,))
        row = cur.fetchone()
        return dict(row) if row else None


def _resolve_uazapi_remote_jid(token):
    """Resolve remote_jid ativo da instância; best-effort."""
    if not uazapi_service or not token:
        return None
    try:
        result = uazapi_service.get_status(token) or {}
        remote_jid = result.get("id") or result.get("me")
        if not remote_jid and isinstance(result.get("instance_data"), dict):
            remote_jid = (
                result["instance_data"].get("phone")
                or result["instance_data"].get("user")
                or result["instance_data"].get("jid")
            )
        return remote_jid
    except Exception:
        return None


def _load_step_messages(conn, campaign_id, step):
    """
    Carrega mensagens do step. Fonte: campaign_steps; fallback: campaigns.message_template
    (mensagens da criação da campanha, visíveis na edição).
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT message_template FROM campaign_steps WHERE campaign_id = %s AND step_number = %s LIMIT 1",
            (campaign_id, step),
        )
        row = cur.fetchone() or {}
    raw = row.get("message_template") or ""
    msgs = _parse_message_template(raw)
    if msgs:
        return msgs
    # Fallback: campaigns.message_template (criação/edição antiga)
    if step == 1:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT message_template FROM campaigns WHERE id = %s LIMIT 1",
                (campaign_id,),
            )
            row = cur.fetchone() or {}
        raw = row.get("message_template") or ""
        msgs = _parse_message_template(raw)
    return msgs or []


def _parse_message_template(raw):
    """Parse message_template (JSON list ou string) em lista de mensagens."""
    if not raw or not str(raw).strip():
        return []
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            msgs = [str(x).strip() for x in parsed if str(x).strip()]
            return msgs if msgs else []
        if isinstance(parsed, str) and parsed.strip():
            return [parsed.strip()]
    except Exception:
        if isinstance(raw, str) and raw.strip():
            return [raw.strip()]
    return []


def _stale_recovery_enabled():
    """T7: recovery automático de ``scheduled`` initial sem pasta, fora do TTL (default ligado)."""
    return os.environ.get("UAZAPI_STALE_RECOVERY_ENABLED", "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


def _stale_recovery_verbose_logging() -> bool:
    """Emite JSON de recovery de bump com sucesso (padrão: off; liga com UAZAPI_STALE_RECOVERY_VERBOSE=1)."""
    return os.environ.get("UAZAPI_STALE_RECOVERY_VERBOSE", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


_cadence_outside_window_log_last_m: float = 0.0


def _should_log_cadence_outside_send_window() -> bool:
    """
    Evita repetir a cada 30s quando a primeira campanha de cadence está fora da janela.
    Config: ``CADENCE_OUTSIDE_WINDOW_LOG_MINUTES`` (default 20, máx. 240).
    """
    global _cadence_outside_window_log_last_m
    try:
        minutes = float((os.environ.get("CADENCE_OUTSIDE_WINDOW_LOG_MINUTES") or "20").strip())
    except (TypeError, ValueError):
        minutes = 20.0
    minutes = max(1.0, min(float(minutes), 240.0))
    now = time.monotonic()
    interval = minutes * 60.0
    if now - _cadence_outside_window_log_last_m < interval:
        return False
    _cadence_outside_window_log_last_m = now
    return True


def _bump_stale_send_scheduled_for_unique(
    conn,
    *,
    send_id: int,
    campaign_id,
    instance_id,
    next_sf,
    max_slip_seconds: int = 600,
):
    """
    Aplica ``scheduled_for`` evitando violar ``uq_campaign_stage_sends_window``.
    Se ``next_sf`` colidir com outro send ``scheduled`` da mesma (campanha, initial, instância),
    desfaz o segundo a segundo até caber. Retorna (data aplicada | None, erro | None).
    """
    if next_sf is None:
        return None, "next_sf_none"
    for slip in range(max_slip_seconds + 1):
        candidate = next_sf if slip == 0 else (next_sf + timedelta(seconds=slip))
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM campaign_stage_sends
                WHERE campaign_id = %s AND stage = 'initial' AND (instance_id IS NOT DISTINCT FROM %s)
                  AND scheduled_for = %s AND status = 'scheduled' AND id <> %s
                LIMIT 1
                """,
                (campaign_id, instance_id, candidate, send_id),
            )
            if cur.fetchone() is not None:
                continue
            cur.execute(
                """
                UPDATE campaign_stage_sends
                SET scheduled_for = %s, updated_at = NOW()
                WHERE id = %s AND status = 'scheduled'
                """,
                (candidate, send_id),
            )
            return candidate, None
    return None, "stale_bump_no_unique_slot"


def _recover_stale_scheduled_initial_uazapi_sends(
    conn,
    *,
    only_campaign_id=None,
    respect_recovery_env=True,
    dry_run=False,
    return_stats=False,
    force_any_campaign_status=False,
    recovery_mode="recovery",
    max_rows_override=None,
):
    """
    T7 / TD-1 / TD-2 / F1 / F6 / F10: antes do materialize automático, trata linhas
    ``initial`` + Uazapi ``scheduled`` sem pasta com ``scheduled_for`` estritamente
    anterior a ``utcnow() - TTL``.

    Política default ADR **B + C**: tenta **C** (``scheduled_for = next_valid`` com margem
    de materialize); se ``next_valid`` inválido ou instância sem ``apikey``, **B**
    (``status = failed``). Não chama ``create_advanced_campaign`` aqui — exclusão mútua
    com ``_materialize_scheduled_stage_sends`` no mesmo tick.

    **T10 (admin):** ``only_campaign_id`` restringe a uma campanha; ``respect_recovery_env=False``
    ignora ``UAZAPI_STALE_RECOVERY_ENABLED``; ``dry_run`` só inspeciona; ``return_stats``
    devolve contadores/listas; ``force_any_campaign_status`` inclui campanhas fora de
    ``running|pending|completed``; ``recovery_mode`` = ``mark_failed`` força ``failed`` em
    todas as linhas stale (desbloqueio agressivo).

    Env: ``UAZAPI_STALE_RECOVERY_ENABLED`` (default ``1``), ``UAZAPI_STALE_RECOVERY_MAX_PER_TICK``
    (default ``50``), ``UAZAPI_STALE_RECOVERY_TTL_MINUTES`` (default ``90``).
    """
    stats = {
        "bumped_send_ids": [],
        "failed_send_ids": [],
        "dry_run_stale_send_ids": [],
        "skipped_disabled": False,
    }
    if respect_recovery_env and not _stale_recovery_enabled():
        stats["skipped_disabled"] = True
        return stats if return_stats else None
    try:
        ttl_min = int(os.environ.get("UAZAPI_STALE_RECOVERY_TTL_MINUTES", "90"))
    except (TypeError, ValueError):
        ttl_min = 90
    try:
        max_per_tick = (
            int(max_rows_override)
            if max_rows_override is not None
            else int(os.environ.get("UAZAPI_STALE_RECOVERY_MAX_PER_TICK", "50"))
        )
    except (TypeError, ValueError):
        max_per_tick = 50
    if max_per_tick <= 0:
        return stats if return_stats else None

    now_utc_naive = datetime.utcnow()
    cutoff = now_utc_naive - timedelta(minutes=max(1, ttl_min))
    mode_norm = (recovery_mode or "recovery").strip().lower()
    if mode_norm not in ("recovery", "mark_failed"):
        mode_norm = "recovery"

    status_clause = (
        "TRUE"
        if force_any_campaign_status
        else "c.status IN ('running', 'pending', 'completed')"
    )
    campaign_clause = (
        "AND css.campaign_id = %s" if only_campaign_id is not None else ""
    )
    # PostgreSQL: com JOIN, FOR UPDATE OF deve usar o alias da relação no FROM, não o nome da tabela.
    lock_sql = "" if dry_run else "FOR UPDATE OF css SKIP LOCKED"
    params = [cutoff]
    if only_campaign_id is not None:
        params.append(int(only_campaign_id))
    params.append(max_per_tick)

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT css.id, css.campaign_id, css.instance_id, css.scheduled_for,
                       c.send_hour_start, c.send_hour_end, c.send_saturday, c.send_sunday,
                       c.user_id, c.daily_limit, i.apikey
                FROM campaign_stage_sends css
                INNER JOIN campaigns c ON c.id = css.campaign_id
                INNER JOIN instances i ON i.id = css.instance_id
                WHERE css.status = 'scheduled'
                  AND css.uazapi_folder_id IS NULL
                  AND css.stage = 'initial'
                  AND COALESCE(c.use_uazapi_sender, FALSE) = TRUE
                  AND css.scheduled_for IS NOT NULL
                  AND css.scheduled_for < %s
                  AND ({status_clause})
                  {campaign_clause}
                ORDER BY css.scheduled_for ASC, css.id ASC
                {lock_sql}
                LIMIT %s
                """,
                tuple(params),
            )
            rows = cur.fetchall() or []

        def _stale_recovery_log(payload, *, always: bool = False) -> None:
            if not always and not _stale_recovery_verbose_logging():
                return
            print(json.dumps(payload, ensure_ascii=False, default=str), flush=True)

        for row in rows:
            send_id = row["id"]
            cid = row["campaign_id"]
            instance_id = row.get("instance_id")
            sched_old = row.get("scheduled_for")
            camp_win = {
                "send_hour_start": row.get("send_hour_start"),
                "send_hour_end": row.get("send_hour_end"),
                "send_saturday": row.get("send_saturday"),
                "send_sunday": row.get("send_sunday"),
            }
            now_brt = datetime.now(BRAZIL_TZ)
            within_send_window = is_campaign_send_window(camp_win, now_brazil=now_brt)
            if sched_old:
                rem_sec = (sched_old - now_utc_naive).total_seconds()
                within_materialize = (
                    -MATERIALIZE_LOOKBACK_MIN * 60 <= rem_sec <= MATERIALIZE_LOOKAHEAD_MIN * 60
                )
            else:
                within_materialize = False

            if mode_norm == "mark_failed":
                if dry_run:
                    stats["dry_run_stale_send_ids"].append(send_id)
                    _stale_recovery_log(
                        {
                            "event": "uazapi_stale_scheduled_recovery",
                            "campaign_id": cid,
                            "send_ids": [send_id],
                            "instance_id": instance_id,
                            "reason": "admin_stale_flush_mark_failed_dry_run",
                            "policy": "dry_run",
                            "within_send_window": within_send_window,
                            "within_materialize_window": within_materialize,
                        },
                        always=True,
                    )
                    continue
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE campaign_stage_sends
                        SET status = 'failed', updated_at = NOW()
                        WHERE id = %s AND status = 'scheduled'
                        """,
                        (send_id,),
                    )
                stats["failed_send_ids"].append(send_id)
                _stale_recovery_log(
                    {
                        "event": "uazapi_stale_scheduled_recovery",
                        "campaign_id": cid,
                        "send_ids": [send_id],
                        "instance_id": instance_id,
                        "reason": "admin_stale_flush_mark_failed",
                        "policy": "failed",
                        "within_send_window": within_send_window,
                        "within_materialize_window": within_materialize,
                    },
                    always=True,
                )
                continue

            token = row.get("apikey")
            if not token:
                if dry_run:
                    stats["dry_run_stale_send_ids"].append(send_id)
                    _stale_recovery_log(
                        {
                            "event": "uazapi_stale_scheduled_recovery",
                            "campaign_id": cid,
                            "send_ids": [send_id],
                            "instance_id": instance_id,
                            "reason": "stale_recovery_failed_no_apikey_dry_run",
                            "policy": "dry_run",
                            "within_send_window": within_send_window,
                            "within_materialize_window": within_materialize,
                            "quota_policy": None,
                        },
                        always=True,
                    )
                    continue
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE campaign_stage_sends
                        SET status = 'failed', updated_at = NOW()
                        WHERE id = %s AND status = 'scheduled'
                        """,
                        (send_id,),
                    )
                stats["failed_send_ids"].append(send_id)
                _stale_recovery_log(
                    {
                        "event": "uazapi_stale_scheduled_recovery",
                        "campaign_id": cid,
                        "send_ids": [send_id],
                        "instance_id": instance_id,
                        "reason": "stale_recovery_failed_no_apikey",
                        "policy": "failed",
                        "within_send_window": within_send_window,
                        "within_materialize_window": within_materialize,
                        "quota_policy": None,
                    },
                    always=True,
                )
                continue

            try:
                next_sf = next_valid_send_utc_naive(
                    camp_win,
                    from_utc_naive=now_utc_naive,
                    margin_minutes=MATERIALIZE_LOOKAHEAD_MIN,
                )
            except ValueError as exc:
                if dry_run:
                    stats["dry_run_stale_send_ids"].append(send_id)
                    _stale_recovery_log(
                        {
                            "event": "uazapi_stale_scheduled_recovery",
                            "campaign_id": cid,
                            "send_ids": [send_id],
                            "instance_id": instance_id,
                            "reason": "stale_recovery_failed_next_valid_dry_run",
                            "policy": "dry_run",
                            "detail": str(exc),
                            "within_send_window": within_send_window,
                            "within_materialize_window": within_materialize,
                            "next_valid_scheduled_for": None,
                        },
                        always=True,
                    )
                    continue
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE campaign_stage_sends
                        SET status = 'failed', updated_at = NOW()
                        WHERE id = %s AND status = 'scheduled'
                        """,
                        (send_id,),
                    )
                stats["failed_send_ids"].append(send_id)
                _stale_recovery_log(
                    {
                        "event": "uazapi_stale_scheduled_recovery",
                        "campaign_id": cid,
                        "send_ids": [send_id],
                        "instance_id": instance_id,
                        "reason": "stale_recovery_failed_next_valid",
                        "policy": "failed",
                        "detail": str(exc),
                        "within_send_window": within_send_window,
                        "within_materialize_window": within_materialize,
                        "next_valid_scheduled_for": None,
                    },
                    always=True,
                )
                continue

            quota_ok = check_initial_chunk_daily_quota_for_campaign(
                int(cid), instance_id=int(instance_id) if instance_id is not None else None
            )

            if dry_run:
                stats["dry_run_stale_send_ids"].append(send_id)
                _stale_recovery_log(
                    {
                        "event": "uazapi_stale_scheduled_recovery",
                        "campaign_id": cid,
                        "send_ids": [send_id],
                        "instance_id": instance_id,
                        "reason": "stale_recovery_bump_next_valid_dry_run",
                        "policy": "dry_run",
                        "within_send_window": within_send_window,
                        "within_materialize_window": within_materialize,
                        "daily_limit_remaining": None,
                        "next_valid_scheduled_for": next_sf.isoformat()
                        if hasattr(next_sf, "isoformat")
                        else str(next_sf),
                        "quota_allows_initial_today": quota_ok,
                    },
                    always=True,
                )
                continue

            applied, bump_err = _bump_stale_send_scheduled_for_unique(
                conn,
                send_id=send_id,
                campaign_id=cid,
                instance_id=instance_id,
                next_sf=next_sf,
            )
            if bump_err:
                _stale_recovery_log(
                    {
                        "event": "uazapi_stale_scheduled_recovery",
                        "campaign_id": cid,
                        "send_ids": [send_id],
                        "instance_id": instance_id,
                        "reason": "stale_bump_unique_slot_failed",
                        "error": bump_err,
                        "policy": "bump_scheduled_for",
                    },
                    always=True,
                )
                continue
            if applied is not None:
                stats["bumped_send_ids"].append(send_id)
            _stale_recovery_log(
                {
                    "event": "uazapi_stale_scheduled_recovery",
                    "campaign_id": cid,
                    "send_ids": [send_id],
                    "instance_id": instance_id,
                    "reason": "stale_recovery_bump_next_valid",
                    "policy": "bump_scheduled_for",
                    "within_send_window": within_send_window,
                    "within_materialize_window": within_materialize,
                    "daily_limit_remaining": None,
                    "next_valid_scheduled_for": applied.isoformat()
                    if applied is not None and hasattr(applied, "isoformat")
                    else str(applied),
                    "quota_allows_initial_today": quota_ok,
                },
                always=False,
            )

        if dry_run:
            conn.rollback()
        else:
            conn.commit()
        return stats if return_stats else None
    except Exception as e:
        print(
            json.dumps(
                {
                    "event": "uazapi_stale_scheduled_recovery_error",
                    "error": str(e),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        try:
            conn.rollback()
        except Exception:
            pass
        if return_stats:
            stats["error"] = str(e)
            return stats
        return None


def _materialize_scheduled_stage_sends(conn, force_send_ids=None):
    """
    Materialização de sends Uazapi `scheduled` (pasta ainda NULL).

    **Janela UTC (automático, sem force_send_ids):** só entram linhas com
    ``scheduled_for`` entre ``now_utc - MATERIALIZE_LOOKBACK_MIN`` e
    ``now_utc + MATERIALIZE_LOOKAHEAD_MIN`` (minutos), espelhando a query SQL
    e o filtro Python em ``remaining`` (evita drift entre SELECT e loop).

    **force_send_ids:** materializa na hora (ignora a janela UTC acima).

    Comportamento:
    - recalcula elegíveis imediatamente antes do envio
    - exclui converted/lost/removed_from_funnel
    - cria folder na janela definida acima (ou forçada)
    - **T9 / AC-RULE-1:** envio Uazapi fora da janela BRT não usa mais ``continue`` sem efeito:
      telemetria ``skipped_outside_window`` + ``scheduled_for = next_valid`` (margem
      ``MATERIALIZE_LOOKAHEAD_MIN``), ou ``failed`` se a janela da campanha for inválida.

    Retorno: dict {"folders_created": int} ou None se uazapi indisponível.
    """
    if not uazapi_service:
        return None

    force_set = set(force_send_ids) if force_send_ids else None
    folders_created = 0
    if force_set:
        print(f"  📤 [Materialize] force_send_ids={list(force_set)}")

    # scheduled_for é persistido em UTC naive; usar UTC na query para evitar drift de fuso (servidor em BRT).
    now_utc_naive = datetime.utcnow()
    if force_set:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT css.id, css.campaign_id, css.stage, css.instance_id, css.scheduled_for,
                       css.status, i.apikey,
                       css.delay_min_minutes, css.delay_max_minutes, css.message_variations, css.lead_ids,
                       css.materialize_attempt_count,
                       c.delay_min_minutes AS campaign_delay_min, c.delay_max_minutes AS campaign_delay_max,
                       c.send_hour_start, c.send_hour_end, c.send_saturday, c.send_sunday, c.use_uazapi_sender,
                       c.daily_limit, c.user_id
                FROM campaign_stage_sends css
                JOIN campaigns c ON c.id = css.campaign_id
                JOIN instances i ON i.id = css.instance_id
                WHERE css.id = ANY(%s)
                  AND css.status IN ('scheduled', 'waiting_reconnect')
                  AND css.uazapi_folder_id IS NULL
                  AND css.scheduled_for IS NOT NULL
                ORDER BY css.scheduled_for ASC, css.id ASC
                """,
                (list(force_set),),
            )
            rows = cur.fetchall() or []
    else:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT css.id, css.campaign_id, css.stage, css.instance_id, css.scheduled_for,
                       css.status, i.apikey,
                       css.delay_min_minutes, css.delay_max_minutes, css.message_variations, css.lead_ids,
                       css.materialize_attempt_count,
                       c.delay_min_minutes AS campaign_delay_min, c.delay_max_minutes AS campaign_delay_max,
                       c.send_hour_start, c.send_hour_end, c.send_saturday, c.send_sunday, c.use_uazapi_sender,
                       c.daily_limit, c.user_id
                FROM campaign_stage_sends css
                JOIN campaigns c ON c.id = css.campaign_id
                JOIN instances i ON i.id = css.instance_id
                WHERE css.status IN ('scheduled', 'waiting_reconnect')
                  AND css.uazapi_folder_id IS NULL
                  AND css.scheduled_for IS NOT NULL
                  AND css.scheduled_for <= ((NOW() AT TIME ZONE 'UTC') + (%s * INTERVAL '1 minute'))
                  AND css.scheduled_for >= ((NOW() AT TIME ZONE 'UTC') - (%s * INTERVAL '1 minute'))
                ORDER BY css.scheduled_for ASC, css.id ASC
                """,
                (MATERIALIZE_LOOKAHEAD_MIN, MATERIALIZE_LOOKBACK_MIN),
            )
            rows = cur.fetchall() or []

    stage_to_step = {"initial": 1, "follow1": 2, "follow2": 3, "breakup": 4}
    grouped = {}
    for row in rows:
        sched = row.get("scheduled_for")
        if not sched:
            continue
        remaining = (sched - now_utc_naive).total_seconds()
        if force_set is None:
            if remaining > MATERIALIZE_LOOKAHEAD_MIN * 60:
                continue
            if remaining < -MATERIALIZE_LOOKBACK_MIN * 60:
                continue
        elif remaining < -86400:
            continue
        key = (row["campaign_id"], row["stage"], sched)
        grouped.setdefault(key, []).append(row)

    for (campaign_id, stage, scheduled_for), sends in grouped.items():
        step = stage_to_step.get(stage)
        if not step:
            continue
        if USE_MESSAGE_OUTBOX and stage != "initial":
            n_sup = supersede_legacy_follow_stage_sends(
                conn, campaign_id, stage, scheduled_for
            )
            if n_sup:
                conn.commit()
                print(
                    f"  ⏭️ [Materialize] campaign_id={campaign_id} stage={stage}: "
                    f"{n_sup} send(s) legado superseded → outbox cadence"
                )
            continue
        # Ordenar por instance_id: chunks[0]→inst1, chunks[1]→inst2 (rotação, sem overlap de números)
        sends = sorted(sends, key=lambda x: x.get("instance_id") or 0)
        print(f"  📤 [Materialize] campaign_id={campaign_id} stage={stage} scheduled_for={scheduled_for} sends={len(sends)}")
        row0 = sends[0]
        if stage == "initial" and row0.get("use_uazapi_sender"):
            daily_lim = int(row0.get("daily_limit") or 0)
            per_instance_limit, total_limit = uazapi_initial_chunk_distribution_limits(
                daily_lim, len(sends)
            )
        else:
            per_instance_limit = 30
            total_limit = per_instance_limit * len(sends)
        if total_limit <= 0:
            continue

        # Task 6: find no escopo dos sends antigos (mesma etapa) antes de novo folder / create_advanced_campaign.
        if sends and sends[0].get("use_uazapi_sender"):
            try:
                sync_campaign_stage_sends_before_new_chunk(conn, campaign_id, uazapi_service, stage=stage)
            except Exception as e:
                print(
                    json.dumps(
                        {
                            "event": "uazapi_materialize_pre_sync_failed",
                            "campaign_id": campaign_id,
                            "stage": stage,
                            "error": str(e),
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
                try:
                    conn.rollback()
                except Exception:
                    pass

        # Stage 'initial': pending + excluir apenas de chunks que efetivamente enviaram (done/running/partial)
        # Chunks failed/cancelled: libera leads para retry
        status_clause = "AND status = 'pending'" if stage == "initial" else "AND status = 'sent'"
        exclude_clause = """
                  AND id NOT IN (
                    SELECT (elem)::int FROM campaign_stage_sends css,
                    LATERAL jsonb_array_elements_text(COALESCE(css.lead_ids, '[]'::jsonb)) AS elem
                    WHERE css.campaign_id = %s AND css.stage = %s
                      AND elem ~ '^[0-9]+$'
                      AND (
                        (css.uazapi_folder_id IS NOT NULL
                         AND css.status IN ('done', 'running', 'partial'))
                        OR (css.status = 'scheduled'
                            AND jsonb_typeof(COALESCE(css.lead_ids, '[]'::jsonb)) = 'array'
                            AND jsonb_array_length(COALESCE(css.lead_ids, '[]'::jsonb)) > 0)
                      )
                  )
        """ if stage == "initial" else ""
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT id, phone, whatsapp_link, name
                FROM campaign_leads
                WHERE campaign_id = %s
                  {status_clause}
                  AND current_step = %s
                  AND COALESCE(removed_from_funnel, FALSE) = FALSE
                  AND COALESCE(cadence_status, 'active') NOT IN ('converted', 'lost')
                  {exclude_clause}
            ORDER BY COALESCE(send_batch, 999) ASC, COALESCE(csv_row_order, id) ASC, id ASC
                LIMIT %s
                """,
                (campaign_id, step, campaign_id, stage, total_limit) if exclude_clause else (campaign_id, step, total_limit),
            )
            leads = cur.fetchall() or []

        if not leads:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """SELECT COUNT(*) as c FROM campaign_leads
                       WHERE campaign_id = %s AND status = 'pending' AND current_step = 1
                         AND COALESCE(removed_from_funnel, FALSE) = FALSE
                         AND COALESCE(cadence_status, 'active') NOT IN ('converted', 'lost')""",
                    (campaign_id,),
                )
                total_pending = (cur.fetchone() or {}).get("c") or 0
                cur.execute(
                    """SELECT COUNT(DISTINCT (elem)::int) as c FROM campaign_stage_sends css,
                       LATERAL jsonb_array_elements_text(COALESCE(css.lead_ids, '[]'::jsonb)) AS elem
                       WHERE css.campaign_id = %s AND css.stage = %s AND css.uazapi_folder_id IS NOT NULL
                         AND css.status IN ('done', 'running', 'partial') AND elem ~ '^[0-9]+$'""",
                    (campaign_id, stage),
                )
                excluded_count = (cur.fetchone() or {}).get("c") or 0
            print(f"  ❌ [Materialize] campaign_id={campaign_id} stage={stage}: 0 leads elegíveis. pending_total={total_pending} excluídos_em_chunks={excluded_count}")
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE campaign_stage_sends
                    SET status = 'failed',
                        updated_at = NOW()
                    WHERE campaign_id = %s
                      AND stage = %s
                      AND scheduled_for = %s
                      AND status = 'scheduled'
                    """,
                    (campaign_id, stage, scheduled_for),
                )
            conn.commit()
            continue

        chunks = [leads[i : i + per_instance_limit] for i in range(0, len(leads), per_instance_limit)]

        def _build_messages_for_sub(sub, step_msgs_inner):
            messages_inner = []
            lids_inner = []
            for lead in sub:
                raw = lead.get("phone") or lead.get("whatsapp_link")
                clean = re.sub(r"\D", "", str(raw or ""))
                if len(clean) <= 11 and clean and not clean.startswith("55"):
                    clean = "55" + clean
                if not clean:
                    continue
                text = random.choice(step_msgs_inner)
                name = lead.get("name")
                if name:
                    text = (
                        text.replace("{nome}", name)
                        .replace("{name}", name)
                        .replace("{{nome}}", name)
                        .replace("{{name}}", name)
                    )
                messages_inner.append({"number": clean, "type": "text", "text": text})
                lids_inner.append(lead["id"])
            return messages_inner, lids_inner

        for idx, send in enumerate(sends):
            send_id = send["id"]
            token = send.get("apikey")
            camp_win = {
                "send_hour_start": send.get("send_hour_start"),
                "send_hour_end": send.get("send_hour_end"),
                "send_saturday": send.get("send_saturday"),
                "send_sunday": send.get("send_sunday"),
            }
            if send.get("use_uazapi_sender") and not is_campaign_send_window(camp_win):
                sched_for = send.get("scheduled_for") or scheduled_for
                if sched_for:
                    rem_sec = (sched_for - now_utc_naive).total_seconds()
                    within_materialize = (
                        -MATERIALIZE_LOOKBACK_MIN * 60
                        <= rem_sec
                        <= MATERIALIZE_LOOKAHEAD_MIN * 60
                    )
                else:
                    within_materialize = False
                base_log = {
                    "event": "uazapi_materialize_outside_send_window",
                    "campaign_id": campaign_id,
                    "send_id": send_id,
                    "instance_id": send.get("instance_id"),
                    "skipped_outside_window": True,
                    "within_send_window": False,
                    "within_materialize_window": within_materialize,
                    "next_valid_scheduled_for": None,
                }
                try:
                    next_sf = next_valid_send_utc_naive(
                        camp_win,
                        from_utc_naive=now_utc_naive,
                        margin_minutes=MATERIALIZE_LOOKAHEAD_MIN,
                    )
                except ValueError as exc:
                    base_log["reason"] = "materialize_outside_brt_invalid_window"
                    base_log["policy"] = "failed"
                    base_log["error"] = str(exc)
                    print(json.dumps(base_log, ensure_ascii=False, default=str), flush=True)
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE campaign_stage_sends
                            SET status = 'failed', updated_at = NOW()
                            WHERE id = %s AND status IN ('scheduled', 'waiting_reconnect')
                            """,
                            (send_id,),
                        )
                    conn.commit()
                    continue

                base_log["reason"] = "materialize_outside_brt_bump_next_valid"
                base_log["policy"] = "bump_scheduled_for"
                base_log["next_valid_scheduled_for"] = (
                    next_sf.isoformat() if hasattr(next_sf, "isoformat") else str(next_sf)
                )
                print(json.dumps(base_log, ensure_ascii=False, default=str), flush=True)
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE campaign_stage_sends
                        SET scheduled_for = %s, updated_at = NOW()
                        WHERE id = %s AND status IN ('scheduled', 'waiting_reconnect')
                        """,
                        (next_sf, send_id),
                    )
                conn.commit()
                print(
                    f"  ⏭️ [Materialize] send_id={send_id} fora da janela BRT (campanha {campaign_id}); "
                    f"reagendado para {base_log['next_valid_scheduled_for']}."
                )
                continue

            reserved_lids = _parse_json_lead_ids(send.get("lead_ids"))
            if reserved_lids:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        f"""
                        SELECT id, phone, whatsapp_link, name
                        FROM campaign_leads
                        WHERE campaign_id = %s AND id = ANY(%s)
                          {status_clause}
                          AND current_step = %s
                          AND COALESCE(removed_from_funnel, FALSE) = FALSE
                          AND COALESCE(cadence_status, 'active') NOT IN ('converted', 'lost')
                        """,
                        (campaign_id, reserved_lids, step),
                    )
                    by_id = {r["id"]: r for r in (cur.fetchall() or [])}
                chunk = [by_id[i] for i in reserved_lids if i in by_id]
            else:
                chunk = chunks[idx] if idx < len(chunks) else []

            step_msgs = _load_step_messages(conn, campaign_id, step)
            if not step_msgs:
                print(f"  ❌ [Materialize] campaign_id={campaign_id} inst={send.get('instance_id')}: sem mensagem configurada em campaign_steps step {step}. Configure no Kanban ou edição da campanha.")
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE campaign_stage_sends SET status = 'failed', updated_at = NOW() WHERE id = %s",
                        (send_id,),
                    )
                conn.commit()
                continue
            if not token:
                print(f"  ❌ [Materialize] campaign_id={campaign_id} inst={send.get('instance_id')}: sem token (apikey)")
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE campaign_stage_sends SET status = 'failed', updated_at = NOW() WHERE id = %s",
                        (send_id,),
                    )
                conn.commit()
                continue
            if not chunk:
                print(f"  ❌ [Materialize] campaign_id={campaign_id} inst={send.get('instance_id')}: chunk vazio (leads={len(leads)}, idx={idx})")
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE campaign_stage_sends SET status = 'done', planned_count = 0, success_count = 0, failed_count = 0, updated_at = NOW() WHERE id = %s",
                        (send_id,),
                    )
                conn.commit()
                continue

            if not can_create_campaign_today(send.get("instance_id")):
                print(f"  ⚠️ [Materialize] campaign_id={campaign_id} inst={send.get('instance_id')}: limite diário de chunks Uazapi atingido para esta instância")
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE campaign_stage_sends SET status = 'failed', updated_at = NOW() WHERE id = %s",
                        (send_id,),
                    )
                conn.commit()
                continue

            # Reserva: persistir lead_ids + planned_count antes de POST (retomada e exclusão)
            with conn.cursor() as cur:
                pl = [c["id"] for c in chunk if c and c.get("id") is not None]
                cur.execute(
                    """
                    UPDATE campaign_stage_sends
                    SET lead_ids = %s,
                        planned_count = %s,
                        message_variations = %s,
                        status = 'scheduled',
                        updated_at = NOW()
                    WHERE id = %s AND uazapi_folder_id IS NULL
                    """,
                    (json.dumps(pl), len(pl), Json(step_msgs), send_id),
                )
            conn.commit()
            reloaded = dict(send)
            reloaded['materialize_attempt_count'] = int(
                (send.get("materialize_attempt_count") or 0) or 0
            )
            reloaded["lead_ids"] = pl

            delay_fallback_min = int(
                reloaded.get("delay_min_minutes")
                or reloaded.get("campaign_delay_min")
                or 5
            )
            delay_fallback_max = int(
                reloaded.get("delay_max_minutes")
                or reloaded.get("campaign_delay_max")
                or 15
            )
            if delay_fallback_max < delay_fallback_min:
                delay_fallback_max = delay_fallback_min
            reloaded["delay_min_minutes"] = delay_fallback_min
            reloaded["delay_max_minutes"] = delay_fallback_max
            rechunk = [c for c in chunk if c.get("id") in set(pl)]
            if not rechunk and pl:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE campaign_stage_sends SET status = 'failed', last_materialize_error = %s, updated_at = NOW() WHERE id = %s",
                        (json.dumps({"error": "reserved_leads_mismatch"}), send_id),
                    )
                conn.commit()
                break

            if USE_MESSAGE_OUTBOX:
                lead_ids_outbox = [
                    int(c["id"]) for c in rechunk if c and c.get("id") is not None
                ]
                n_ob = enqueue_outbox_rows_for_leads(
                    conn,
                    campaign_id=campaign_id,
                    stage=stage,
                    lead_ids=lead_ids_outbox,
                    instance_id=int(send.get("instance_id") or 0),
                    enqueue_source="materialize_legacy_to_outbox",
                    supersede_send_id=send_id,
                )
                conn.commit()
                print(
                    f"  📤 [Materialize→Outbox] campaign_id={campaign_id} inst={send.get('instance_id')}: "
                    f"{n_ob} envio(s) enfileirado(s) (sem create_advanced_campaign)"
                )
                break

            use_pacing = (
                bool(reloaded.get("use_uazapi_sender")) and stage == "initial" and len(rechunk) >= 2
            )
            if use_pacing:
                pacing_plan = build_pacing_segments_for_leads(rechunk)
            else:
                pacing_plan = [(tuple(rechunk), delay_fallback_min, delay_fallback_max, 0)]

            t_chain = now_utc_naive
            prev_sub, prev_dmin, prev_dmax, prev_gap = None, None, None, 0
            _n_pacing = len(pacing_plan)
            _verbose_pacing = os.environ.get("MATERIALIZE_VERBOSE_SEGMENTS") == "1"
            _pacing_tail_logged = False

            for seg_i, (sub_chunk_t, dmin, dmax, gap_after) in enumerate(pacing_plan):
                sub_chunk = list(sub_chunk_t)
                if (
                    seg_i == 0
                    and sub_chunk
                    and send.get("use_uazapi_sender")
                ):
                    iid = send.get("instance_id")
                    st_payload = get_instance_status_cached(
                        uazapi_service, int(iid), (token or "").strip()
                    )
                    if is_instance_disconnected_status(st_payload):
                        uid = send.get("user_id")
                        print(
                            json.dumps(
                                {
                                    "event": "uazapi_materialize_blocked_disconnected",
                                    "campaign_id": campaign_id,
                                    "send_id": send_id,
                                    "instance_id": iid,
                                },
                                ensure_ascii=False,
                            ),
                            flush=True,
                        )
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                UPDATE campaign_stage_sends
                                SET status = 'waiting_reconnect',
                                    scheduled_for = %s,
                                    last_materialize_error = %s,
                                    materialize_attempt_count = %s,
                                    updated_at = NOW()
                                WHERE id = %s
                                """,
                                (
                                    now_utc_naive + timedelta(minutes=2),
                                    format_last_error_for_db(
                                        {
                                            "uazapi_request_failed": True,
                                            "error_body": "instance_status_disconnected",
                                        },
                                        "no_session",
                                    ),
                                    int((reloaded.get("materialize_attempt_count") or 0) or 0) + 1,
                                    send_id,
                                ),
                            )
                        conn.commit()
                        if uid:
                            try:
                                maybe_send_disconnect_support_whatsapp(
                                    conn,
                                    uazapi_service,
                                    campaign_id=campaign_id,
                                    user_id=int(uid),
                                    instance_id=int(iid),
                                )
                            except Exception as _ex:
                                print(
                                    json.dumps(
                                        {
                                            "event": "uazapi_support_notify_ex",
                                            "error": str(_ex)[:400],
                                        },
                                        ensure_ascii=False,
                                    ),
                                    flush=True,
                                )
                        break

                messages, lead_ids = _build_messages_for_sub(sub_chunk, step_msgs)
                if not messages:
                    print(
                        f"  ⚠️ [Materialize] campaign_id={campaign_id} inst={send.get('instance_id')}: segmento {seg_i} sem msgs válidas"
                    )
                    break

                if seg_i > 0:
                    if prev_sub is None:
                        break
                    span_prev = estimate_segment_span_minutes(len(prev_sub), prev_dmin, prev_dmax)
                    t_chain = stagger_scheduled_utc_naive(
                        t_chain, span_prev + prev_gap + seg_i
                    )  # +seg_i s evita colisão no índice único (mesmo minuto)
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO campaign_stage_sends
                            (campaign_id, stage, instance_id, scheduled_for, status, planned_count, lead_ids,
                             delay_min_minutes, delay_max_minutes, message_variations)
                            VALUES (%s, %s, %s, %s, 'scheduled', %s, %s, %s, %s, %s)
                            """,
                            (
                                campaign_id,
                                stage,
                                send.get("instance_id"),
                                t_chain,
                                len(lead_ids),
                                json.dumps(lead_ids),
                                int(dmin),
                                int(dmax),
                                Json(step_msgs),
                            ),
                        )
                    conn.commit()
                    if _verbose_pacing or seg_i == 1:
                        print(
                            f"  📎 [Materialize] campaign_id={campaign_id} inst={send.get('instance_id')}: "
                            f"sub-campanha agendada seg={seg_i} em {t_chain} UTC ({len(lead_ids)} leads) delay {dmin}-{dmax} min"
                        )
                    elif seg_i == 2 and _n_pacing > 2 and not _pacing_tail_logged:
                        print(
                            f"  📎 [Materialize] campaign_id={campaign_id} inst={send.get('instance_id')}: "
                            f"pacing em {_n_pacing} segmentos (+{_n_pacing - 2} agendados; MATERIALIZE_VERBOSE_SEGMENTS=1 para listar todos)"
                        )
                        _pacing_tail_logged = True
                    prev_sub, prev_dmin, prev_dmax, prev_gap = sub_chunk, dmin, dmax, gap_after
                    continue

                delay_min_sec = int(dmin * 60)
                delay_max_sec = int(dmax * 60)
                if delay_max_sec < delay_min_sec:
                    delay_max_sec = delay_min_sec
                result = uazapi_service.create_advanced_campaign(
                    token=token,
                    delay_min_sec=delay_min_sec,
                    delay_max_sec=delay_max_sec,
                    messages=messages,
                    info=f"Campaign {campaign_id} {stage} inst {send.get('instance_id')} seg0",
                )
                folder_id = (result or {}).get("folder_id") or (result or {}).get("folderId")
                cat, _msg, _http = classify_create_advanced_error(result)
                if not result or not folder_id:
                    err_msg = (result or {}).get("error") or (result or {}).get("message")
                    if err_msg is None and isinstance(result, dict) and result.get(
                        "uazapi_request_failed"
                    ):
                        err_msg = str(
                            (result.get("error_body") or result.get("exception") or "")
                        )[:500]
                    print(
                        f"  ❌ [Materialize] campaign_id={campaign_id} inst={send.get('instance_id')}: "
                        f"Uazapi create_advanced_campaign falhou. folder_id={folder_id} err={err_msg!s} class={cat}"
                    )
                    le = format_last_error_for_db(result, cat)
                    ac = int((reloaded.get("materialize_attempt_count") or 0) or 0)
                    uid = send.get("user_id")
                    if cat == "no_session" and uid:
                        try:
                            maybe_send_disconnect_support_whatsapp(
                                conn,
                                uazapi_service,
                                campaign_id=campaign_id,
                                user_id=int(uid),
                                instance_id=int(send.get("instance_id") or 0),
                            )
                        except Exception as _ex:
                            print(
                                json.dumps(
                                    {
                                        "event": "uazapi_support_notify_ex",
                                        "error": str(_ex)[:400],
                                    },
                                    ensure_ascii=False,
                                ),
                                flush=True,
                            )
                    if cat == "no_session":
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                UPDATE campaign_stage_sends
                                SET status = 'waiting_reconnect',
                                    scheduled_for = %s,
                                    last_materialize_error = %s,
                                    materialize_attempt_count = %s,
                                    updated_at = NOW()
                                WHERE id = %s
                                """,
                                (
                                    now_utc_naive + timedelta(minutes=2),
                                    le,
                                    ac + 1,
                                    send_id,
                                ),
                            )
                        conn.commit()
                        break
                    if cat in (
                        "transient_http",
                        "empty_response",
                        "server_error",
                    ) and ac < 5:
                        try:
                            next_sf = _next_retry_utc_for_materialize(
                                camp_win, now_utc_naive, ac
                            )
                        except ValueError as vex:
                            with conn.cursor() as cur:
                                cur.execute(
                                    """
                                    UPDATE campaign_stage_sends
                                    SET status = 'failed',
                                        last_materialize_error = %s,
                                        materialize_attempt_count = %s,
                                        updated_at = NOW()
                                    WHERE id = %s
                                    """,
                                    (
                                        (le or "")[:1500]
                                        + json.dumps(
                                            {"next_valid_error": str(vex)},
                                            ensure_ascii=False,
                                        )[:200],
                                        ac + 1,
                                        send_id,
                                    ),
                                )
                            conn.commit()
                            break
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                UPDATE campaign_stage_sends
                                SET status = 'scheduled',
                                    scheduled_for = %s,
                                    last_materialize_error = %s,
                                    materialize_attempt_count = %s,
                                    updated_at = NOW()
                                WHERE id = %s
                                """,
                                (next_sf, le, ac + 1, send_id),
                            )
                        conn.commit()
                        break
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE campaign_stage_sends
                            SET status = 'failed',
                                last_materialize_error = %s,
                                materialize_attempt_count = %s,
                                updated_at = NOW()
                            WHERE id = %s
                            """,
                            (le, ac + 1, send_id),
                        )
                    conn.commit()
                    break

                api_status = (result or {}).get("status", "?")
                api_count = (result or {}).get("count", len(messages))
                print(f"  ✅ [Materialize] campaign_id={campaign_id} inst={send.get('instance_id')}: folder_id={folder_id} ({len(messages)} msgs) API status={api_status} count={api_count}")
                if api_status in ("queued", "scheduled"):
                    ctrl = uazapi_service.edit_campaign(token, folder_id, "continue")
                    if ctrl:
                        print(f"  ▶️ [Materialize] campaign_id={campaign_id} inst={send.get('instance_id')}: edit_campaign(continue) ok")
                    else:
                        print(f"  ⚠️ [Materialize] campaign_id={campaign_id} inst={send.get('instance_id')}: edit_campaign(continue) falhou (campanha pode iniciar sozinha)")
                remote_jid = _resolve_uazapi_remote_jid(token)
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE campaign_stage_sends
                        SET uazapi_folder_id = %s,
                            instance_remote_jid = %s,
                            lead_ids = %s,
                            planned_count = %s,
                            delay_min_minutes = %s,
                            delay_max_minutes = %s,
                            message_variations = %s,
                            status = 'running',
                            last_materialize_error = NULL,
                            materialize_attempt_count = 0,
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (
                            folder_id,
                            remote_jid,
                            json.dumps(lead_ids),
                            len(lead_ids),
                            int(dmin),
                            int(dmax),
                            Json(step_msgs),
                            send_id,
                        ),
                    )
                    cur.execute(
                        "INSERT INTO uazapi_instance_sends (instance_id, campaign_id) VALUES (%s, %s)",
                        (send.get("instance_id"), campaign_id),
                    )
                conn.commit()
                folders_created += 1
                _verify_folder_queue.append(
                    {
                        "send_id": send_id,
                        "folder_id": folder_id,
                        "token": token,
                        "verify_at": time.time() + VERIFY_FOLDER_AFTER_SECONDS,
                    }
                )
                prev_sub, prev_dmin, prev_dmax, prev_gap = sub_chunk, dmin, dmax, gap_after

    return {"folders_created": folders_created}


# --- Dual-run desconexão Uazapi (legado advanced vs fila outbox) ---
# ``waiting_reconnect`` em ``campaign_stage_sends`` (fluxo advanced): o worker promove de
# volta para ``scheduled`` quando ``get_instance_status_cached`` / ``get_status`` indica
# instância ligada (``not is_instance_disconnected_status``).
# A fila ``campaign_message_outbox`` (USE_MESSAGE_OUTBOX) não usa esse estado: falhas
# transitórias/desconexão mantêm a linha em ``pending`` ou ``waiting_instance``, com
# retry/backoff em ``_persist_outcome``; o elegível volta a ser claimado quando a
# campanha está ``running`` e a instância responde de novo. Ambos os caminhos dependem
# do mesmo sinal de saúde da instância (``get_status`` via cache) para destravar após
# reconexão — ver também ``_uazapi_instance_health_tick`` (pausa sistema / alertas).


def _resume_waiting_reconnect_stage_sends(conn):
    """
    Tenta reativar ``waiting_reconnect`` quando a instância Uazapi volta a ``connected``,
    puxa ``get_status`` e promove a linha a ``scheduled`` (mantém reserva em lead_ids).
    """
    if not uazapi_service:
        return
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT css.id, css.instance_id, i.apikey
            FROM campaign_stage_sends css
            JOIN instances i ON i.id = css.instance_id
            WHERE css.status = 'waiting_reconnect'
              AND css.uazapi_folder_id IS NULL
              AND css.scheduled_for <= (NOW() AT TIME ZONE 'UTC')
            LIMIT 30
            """
        )
        rows = cur.fetchall() or []
    for r in rows:
        sid = r.get("id")
        iid = r.get("instance_id")
        tok = (r.get("apikey") or "").strip()
        st = get_instance_status_cached(
            uazapi_service, int(iid or 0), tok
        )
        if st and not is_instance_disconnected_status(st):
            nxt = datetime.utcnow() + timedelta(seconds=30)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE campaign_stage_sends
                    SET status = 'scheduled', scheduled_for = %s, updated_at = NOW()
                    WHERE id = %s AND status = 'waiting_reconnect'
                    """,
                    (nxt, sid),
                )
            conn.commit()
            print(
                json.dumps(
                    {
                        "event": "uazapi_waiting_reconnect_resumed",
                        "send_id": sid,
                        "instance_id": iid,
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )


def _process_verify_folder_queue():
    """
    Processa fila de verificação: 3 min após create_advanced_campaign, chama list_folders
    e confirma se folder existe e status em (queued, scheduled, sending). Log único por send.
    """
    if not uazapi_service:
        return
    now = time.time()
    global _verify_folder_queue
    to_remove = []
    for i, item in enumerate(_verify_folder_queue):
        if item["verify_at"] > now:
            continue
        to_remove.append(i)
        send_id = item["send_id"]
        fid = str(item["folder_id"] or "").strip()
        token = (item["token"] or "").strip()
        if not fid or not token:
            continue
        try:
            folders = uazapi_service.list_folders(token) or []
            found = None
            for f in folders:
                cf = str(f.get("id") or f.get("folder_id") or f.get("folderId") or "").strip()
                if cf == fid:
                    found = f
                    break
            status = (found.get("status") or "").lower() if found else None
            if found:
                ok_status = status in ("queued", "scheduled", "sending", "running", "ativo")
                if ok_status:
                    print(f"  ✅ [Verify] send_id={send_id} folder={fid} status={status} (list_folders ok)")
                else:
                    print(f"  ⚠️ [Verify] send_id={send_id} folder={fid} status={status} (verificar se envio iniciou)")
            else:
                print(f"  ⚠️ [Verify] send_id={send_id} folder={fid} NÃO encontrado em list_folders (API delay ou erro)")
        except Exception as e:
            print(f"  ⚠️ [Verify] send_id={send_id}: list_folders falhou: {e}")
    for i in reversed(to_remove):
        _verify_folder_queue.pop(i)


def _uazapi_instance_health_tick(conn) -> dict:
    """
    Task 4–5: health Uazapi por instância (campanhas running/pending ou pausadas por desconexão).
    - Se desligado e há campanha ativa: pausa em cascata (mantém pause manual).
    - Persiste ``worker_last_uazapi_disconnected``; na transição desligado→ligado notifica
      (fila in-app + WhatsApp opcional, com cooldown).
    """
    if not uazapi_service:
        return {"paused": 0, "reconnect_notified": 0}

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT DISTINCT i.id, i.apikey, i.name AS instance_name
            FROM instances i
            WHERE COALESCE(i.api_provider, 'megaapi') = 'uazapi'
              AND i.apikey IS NOT NULL AND BTRIM(i.apikey) <> ''
              AND EXISTS (
                SELECT 1 FROM campaign_instances ci
                JOIN campaigns c ON c.id = ci.campaign_id
                WHERE ci.instance_id = i.id
                  AND COALESCE(c.use_uazapi_sender, false) = true
                  AND (
                    c.status IN ('running', 'pending')
                    OR (
                      c.status = 'paused'
                      AND c.pause_origin = 'system'
                      AND c.pause_reason_code = 'instance_disconnected'
                    )
                  )
              )
            ORDER BY i.id
            """
        )
        rows = cur.fetchall() or []

    disconnected_for_pause: list[int] = []
    reconnect_notified = 0

    for r in rows:
        iid = r.get("id")
        tok = (r.get("apikey") or "").strip()
        inst_name = (r.get("instance_name") or "").strip()
        try:
            iid_int = int(iid)
        except (TypeError, ValueError):
            continue
        if not tok:
            continue

        st = get_instance_status_cached(uazapi_service, iid_int, tok)
        now_disc = is_instance_disconnected_status(st)

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT worker_last_uazapi_disconnected FROM instances WHERE id = %s",
                (iid_int,),
            )
            prow = cur.fetchone()
        prev = prow.get("worker_last_uazapi_disconnected") if prow else None

        if prev is True and not now_disc:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT c.user_id, COUNT(*) AS n
                    FROM campaigns c
                    JOIN campaign_instances ci ON ci.campaign_id = c.id
                    WHERE ci.instance_id = %s
                      AND c.status = 'paused'
                      AND c.pause_origin = 'system'
                      AND c.pause_reason_code = 'instance_disconnected'
                    GROUP BY c.user_id
                    """,
                    (iid_int,),
                )
                groups = cur.fetchall() or []
            for g in groups:
                uid = int(g["user_id"])
                n = int(g["n"] or 0)
                if n <= 0:
                    continue
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT MIN(c.id) AS cid FROM campaigns c
                        JOIN campaign_instances ci ON ci.campaign_id = c.id
                        WHERE ci.instance_id = %s AND c.user_id = %s
                          AND c.status = 'paused'
                          AND c.pause_origin = 'system'
                          AND c.pause_reason_code = 'instance_disconnected'
                        """,
                        (iid_int, uid),
                    )
                    cr = cur.fetchone()
                cid = int(cr["cid"]) if cr and cr.get("cid") is not None else 0

                enq = enqueue_reconnect_inapp_alert(
                    conn,
                    user_id=uid,
                    instance_id=iid_int,
                    instance_name=inst_name,
                    campaign_count=n,
                )
                wa = maybe_send_reconnect_support_whatsapp(
                    conn,
                    uazapi_service,
                    campaign_id=cid or iid_int,
                    user_id=uid,
                    instance_id=iid_int,
                    instance_name=inst_name,
                    n_campaigns=n,
                    context={"enqueue": enq},
                )
                reconnect_notified += 1
                print(
                    json.dumps(
                        {
                            "event": "uazapi_instance_reconnect_transition",
                            "instance_id": iid_int,
                            "user_id": uid,
                            "system_paused_campaigns": n,
                            "inapp": enq,
                            "whatsapp": wa,
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT 1 FROM campaign_instances ci
                JOIN campaigns c ON c.id = ci.campaign_id
                WHERE ci.instance_id = %s
                  AND c.status IN ('running', 'pending')
                  AND COALESCE(c.use_uazapi_sender, false) = true
                LIMIT 1
                """,
                (iid_int,),
            )
            has_active = cur.fetchone() is not None

        if now_disc and has_active:
            disconnected_for_pause.append(iid_int)

        with conn.cursor() as cur:
            cur.execute(
                "UPDATE instances SET worker_last_uazapi_disconnected = %s WHERE id = %s",
                (now_disc, iid_int),
            )

    n_paused = 0
    if disconnected_for_pause:
        disconnected_ids = list(dict.fromkeys(disconnected_for_pause))
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE campaigns c
                SET status = 'paused',
                    pause_origin = 'system',
                    pause_reason_code = 'instance_disconnected',
                    system_paused_at = NOW()
                WHERE c.id IN (
                    SELECT DISTINCT ci.campaign_id
                    FROM campaign_instances ci
                    WHERE ci.instance_id = ANY(%s)
                )
                AND c.status IN ('running', 'pending')
                AND COALESCE(c.use_uazapi_sender, false) = true
                AND (c.pause_origin IS DISTINCT FROM 'user')
                """,
                (disconnected_ids,),
            )
            n_paused = cur.rowcount or 0
        if n_paused:
            print(
                json.dumps(
                    {
                        "event": "uazapi_system_pause_disconnected_instance",
                        "instance_ids": disconnected_ids,
                        "campaigns_paused": n_paused,
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )

    conn.commit()
    return {"paused": int(n_paused), "reconnect_notified": int(reconnect_notified)}


def _sync_active_stage_folders(conn):
    """
    Sincroniza pastas UAZAPI ativas por campanha, respeitando STAGE_SYNC_INTERVAL_MINUTES.

    Contagem/progresso alinhados à API vêm de listfolders apenas; listmessages não substitui
    esse SSOT para totais globais. O SQL limita re-sync ao intervalo de ~10 min (last_sync_at).

    Returns:
        ``set`` de ``campaign_id`` para os quais ``sync_campaign_leads_from_uazapi`` foi chamado
        nesta execução (evita segundo sync HTTP no mesmo tick em rollover — Task 5 / process_rollover_fu_next).
    """
    if not uazapi_service:
        return set()

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT DISTINCT css.campaign_id, c.uazapi_folder_id, i.apikey
            FROM campaign_stage_sends css
            JOIN campaigns c ON c.id = css.campaign_id
            JOIN campaign_instances ci ON ci.campaign_id = css.campaign_id
            JOIN instances i ON i.id = ci.instance_id
            WHERE css.status IN ('scheduled', 'running', 'partial')
              AND css.uazapi_folder_id IS NOT NULL
              AND COALESCE(i.api_provider, 'megaapi') = 'uazapi'
              AND i.apikey IS NOT NULL
              AND (
                    css.last_sync_at IS NULL
                    OR css.last_sync_at <= (NOW() - INTERVAL '10 minutes')
                  )
            ORDER BY css.campaign_id ASC
            """
        )
        rows = cur.fetchall() or []

    if not rows:
        return set()

    by_campaign = {}
    for row in rows:
        by_campaign.setdefault(row["campaign_id"], row)

    for row in by_campaign.values():
        try:
            sync_campaign_leads_from_uazapi(
                conn,
                row["campaign_id"],
                row["apikey"],
                row.get("uazapi_folder_id"),
                uazapi_service,
            )
        except Exception as e:
            print(f"  ⚠️ [Stage Sync] Campaign {row['campaign_id']}: falha no sync: {e}")
            try:
                conn.rollback()
            except Exception:
                pass

    return set(by_campaign.keys())

# --- MAIN LOGIC ---

def process_cadence():
    _logger_cadence.debug("Intelligent Cadence Worker iniciado.")
    maybe_start_outbox_metrics_http_server()
    last_stage_sync_at = None
    last_disconnect_pause_check_mono: float | None = None
    disconnect_pause_interval_sec = _parse_uazapi_disconnect_pause_check_interval_sec()

    while True:
        try:
            conn = get_db_connection()

            # Legado advanced: ``campaign_stage_sends.status = waiting_reconnect`` → scheduled
            # quando a instância volta (get_status). Outbox: linhas pending/waiting_instance no
            # mesmo tick após campanha running — ver bloco "Dual-run desconexão" acima de
            # ``_resume_waiting_reconnect_stage_sends``.
            _resume_waiting_reconnect_stage_sends(conn)

            now_mono = time.monotonic()
            if (
                last_disconnect_pause_check_mono is None
                or (now_mono - last_disconnect_pause_check_mono) >= disconnect_pause_interval_sec
            ):
                _uazapi_instance_health_tick(conn)
                last_disconnect_pause_check_mono = now_mono

            # T7: recovery de ``scheduled`` initial sem pasta (TTL) antes do materialize — F1
            _recover_stale_scheduled_initial_uazapi_sends(conn)

            # Outbox Uazapi (ADR-5): claim → HTTP → persistência; só com flag (utils.config)
            if USE_MESSAGE_OUTBOX:
                process_message_outbox_tick(conn)
                maybe_schedule_outbox_initial_batches(conn)
                # Fallback: catch-up BRT (send_hour_start .. start+CATCHUP_HOURS) force chunk
                maybe_auto_force_daily_initial_batches(conn)

            # Pré-disparo determinístico para agendamentos de etapa (2-5 min antes)
            _materialize_scheduled_stage_sends(conn)

            # Verificação pós-create: 3 min após create_advanced_campaign, list_folders confirma folder
            _process_verify_folder_queue()

            now_sync = datetime.now(BRAZIL_TZ)
            should_sync = (
                last_stage_sync_at is None
                or (now_sync - last_stage_sync_at).total_seconds() >= STAGE_SYNC_INTERVAL_MINUTES * 60
            )
            # Campanhas já sincronizadas neste tick: rollover (initial→FU1, FU chain) reutiliza BD
            # sem segundo sync HTTP para o mesmo campaign_id.
            synced_campaign_ids = set()
            if should_sync:
                synced_campaign_ids = _sync_active_stage_folders(conn) or set()
                last_stage_sync_at = now_sync

            # --- PART A: SAFETY BUFFER CHECK (Monitoring Phase) ---
            check_monitoring_leads(conn)

            # Rollover Inicial → FU1: consulta própria em process_uazapi_initial_stage_rollovers (não usa a lista abaixo).
            # Deve rodar mesmo quando não há campanhas em "running/pending/completed" (ex.: campanha pausada) ou lista vazia por outro motivo;
            # caso contrário os cards nunca saem da Inicial após o envio.
            process_uazapi_initial_stage_rollovers(
                conn, campaigns_synced_this_tick=synced_campaign_ids
            )

            # 1. Campanhas ativas: cadência completa OU Uazapi "só inicial" (enable_cadence=false).
            # Neste último caso só rodamos schedule_next_initial_chunk (chunks etapa initial); sem FU/rollover/send legado.
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT c.id, c.name, c.user_id, c.cadence_config, c.enable_cadence, c.send_hour_start, c.send_hour_end, c.send_saturday, c.send_sunday,
                           c.use_uazapi_sender, c.uazapi_folder_id, c.delay_min_minutes, c.delay_max_minutes,
                           c.scheduled_start, c.rotation_mode, c.daily_limit
                    FROM campaigns c
                    WHERE c.status IN ('running', 'pending', 'completed')
                      AND (c.scheduled_start IS NULL OR c.scheduled_start <= NOW())
                      AND (
                          c.enable_cadence = TRUE
                          OR (
                              COALESCE(c.use_uazapi_sender, FALSE) = TRUE
                              AND COALESCE(c.enable_cadence, FALSE) = FALSE
                          )
                      )
                """)
                campaigns = cur.fetchall()
            conn.commit()  # Libera locks antes do loop longo (evita deadlock com worker_sender/sync)

            if not campaigns:
                conn.close()
                time.sleep(CADENCE_POLL_INTERVAL)
                continue

            first_with_cadence = next((c for c in campaigns if c.get("enable_cadence")), None)

            for campaign in campaigns:
                if not campaign.get('use_uazapi_sender'):
                    process_rollover(campaign, conn)
                else:
                    schedule_next_initial_chunk(campaign, conn)

                if not campaign.get('enable_cadence'):
                    continue

                process_rollover_fu_next(
                    campaign,
                    conn,
                    from_step=2,
                    to_step=3,
                    step_label="Follow-up 2",
                    campaigns_synced_this_tick=synced_campaign_ids,
                )
                process_rollover_fu_next(
                    campaign,
                    conn,
                    from_step=3,
                    to_step=4,
                    step_label="Despedida",
                    campaigns_synced_this_tick=synced_campaign_ids,
                )

                if is_campaign_send_window(campaign):
                    if not (USE_MESSAGE_OUTBOX and _campaign_has_message_outbox(conn, campaign["id"])):
                        process_campaign_sends(campaign, conn)
                    bootstrap_pending_leads(campaign, conn)
                else:
                    now_brazil = datetime.now(BRAZIL_TZ)
                    if first_with_cadence and campaign.get("id") == first_with_cadence.get("id"):
                        if _should_log_cadence_outside_send_window():
                            print(
                                f"⏰ [Cadence] Fora da janela da campanha ({now_brazil.strftime('%H:%M')} BRT). Envio Mega/cadência pausado."
                            )

            conn.close()
            time.sleep(CADENCE_POLL_INTERVAL)

        except psycopg2_errors.DeadlockDetected as e:
            print(f"⚠️ [Cadence] Deadlock detected, retrying in ~5s: {e}")
            try:
                conn.rollback()
                conn.close()
            except Exception:
                pass
            time.sleep(5 + random.uniform(0, 3))  # jitter para evitar colisão repetida
        except Exception as e:
            print(f"❌ [Cadence] Error in main loop: {e}")
            import traceback
            traceback.print_exc()
            try:
                conn.rollback()
                conn.close()
            except Exception:
                pass
            time.sleep(30)

def check_monitoring_leads(conn):
    """
    SAFETY BUFFER Logic:
    Checks leads in 'monitoring' status.
    If 5 mins passed since send:
      - Check Chatwoot for replies/unread.
      - If reply: ABORT SNOOZE (Set 'stopped').
      - If safe: SNOOZE in Chatwoot + Schedule Next Step.
    """
    buffer_time = datetime.now(BRAZIL_TZ) - timedelta(minutes=SAFETY_BUFFER_MINUTES)
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT cl.id, cl.chatwoot_conversation_id, cl.campaign_id, cl.current_step, 
                   cl.last_message_sent_at, cl.phone, cl.name
            FROM campaign_leads cl
            WHERE cl.cadence_status = 'monitoring'
              AND cl.last_message_sent_at <= %s
        """, (buffer_time,))
        monitoring_leads = cur.fetchall()

    if not monitoring_leads:
        return

    print(f"🛡️ [Safety Buffer] Checking {len(monitoring_leads)} monitored leads...")

    for lead in monitoring_leads:
        lead_id = lead['id']
        conv_id = lead['chatwoot_conversation_id']
        
        # If no Chatwoot conversation, try to discover it
        if not conv_id:
            conv_id = discover_chatwoot_conversation(lead['phone'], lead.get('name'))
            if conv_id:
                with conn.cursor() as cur:
                    cur.execute("UPDATE campaign_leads SET chatwoot_conversation_id = %s WHERE id = %s", (conv_id, lead_id))
                conn.commit()
        
        # 1. Check Chatwoot Context
        cw_data = get_chatwoot_conversation_details(conv_id)
        
        abort_snooze = False
        abort_reason = ""

        if cw_data:
            unread = cw_data.get('unread_count', 0)
            status = cw_data.get('status')
            if unread > 0:
                abort_snooze = True
                abort_reason = f"Unread count is {unread}"
            else:
                messages = get_chatwoot_conversation_messages(conv_id)
                if messages:
                    # Check last actual message (0=incoming, 1=outgoing)
                    for msg in reversed(messages):
                        mtype = msg.get('message_type')
                        if mtype in [0, 1]:
                            if mtype == 0:
                                abort_snooze = True
                                abort_reason = "Last message is from Contact"
                            break
        else:
            if conv_id:
                print(f"  ⚠️ Lead #{lead_id}: Could not fetch Chatwoot details. Proceeding with snooze.")

        with conn.cursor() as cur:
            if abort_snooze:
                cur.execute("""
                    UPDATE campaign_leads SET cadence_status = 'stopped', log = %s WHERE id = %s
                """, (f"Safety Buffer Abort: {abort_reason}", lead_id))
                conn.commit()
                print(f"  🛑 Lead #{lead_id}: Snooze ABORTED. {abort_reason}")
            else:
                # SAFE: Execute Snooze + Schedule Next Step
                cur.execute("""
                    SELECT delay_days FROM campaign_steps 
                    WHERE campaign_id = %s AND step_number = %s
                """, (lead['campaign_id'], lead['current_step'] + 1))
                next_step_row = cur.fetchone()
                
                if next_step_row:
                    delay = next_step_row['delay_days']
                    delay = 1 if delay is None else int(delay)
                    now_br = datetime.now(BRAZIL_TZ)
                    snooze_until = now_br + timedelta(minutes=2) if delay <= 0 else now_br + timedelta(days=delay)
                    new_status = 'snoozed'
                    
                    cur.execute("""
                        UPDATE campaign_leads 
                        SET cadence_status = %s, snooze_until = %s 
                        WHERE id = %s
                    """, (new_status, snooze_until, lead_id))
                    
                    # Execute Chatwoot Snooze with timestamp
                    toggle_chatwoot_status(conv_id, 'snoozed', snoozed_until=snooze_until)
                    
                    print(f"  💤 Lead #{lead_id}: Safety Check passed. Snoozed until {snooze_until.strftime('%d/%m %H:%M')}.")
                else:
                    cur.execute("UPDATE campaign_leads SET cadence_status = 'completed' WHERE id = %s", (lead_id,))
                    toggle_chatwoot_status(conv_id, 'resolved')
                    print(f"  🏁 Lead #{lead_id}: Cadence completed.")
            conn.commit()


def _parse_rollover_time(rollover_str):
    """Parse 'HH:MM' ou 'H:MM' para (hour, minute). Default (23, 0)."""
    if not rollover_str or not isinstance(rollover_str, str):
        return 23, 0
    parts = str(rollover_str).strip().split(':')
    if len(parts) >= 2:
        try:
            return int(parts[0]) % 24, int(parts[1]) % 60
        except ValueError:
            pass
    return 23, 0


def schedule_next_initial_chunk(campaign, conn):
    """
    Para campanhas Uazapi: agenda o próximo lote initial via outbox (``schedule_next_initial_outbox_batch``).
    Com ``USE_MESSAGE_OUTBOX=false`` (rollback), usa ``campaign_stage_sends`` legado.
    """
    cid = campaign['id']
    if not campaign.get('use_uazapi_sender'):
        return

    if USE_MESSAGE_OUTBOX:
        if not uazapi_service:
            return
        n_outbox, _reason = schedule_next_initial_outbox_batch(conn, campaign)
        if n_outbox > 0:
            print(
                f"  📤 [Initial Outbox] Campaign '{campaign.get('name')}': "
                f"enfileirados {n_outbox} envio(s) initial (outbox /send/text)"
            )
        return

    _schedule_next_initial_chunk_legacy(campaign, conn)


def _schedule_next_initial_chunk_legacy(campaign, conn):
    """Legado: INSERT em ``campaign_stage_sends`` (só com ``USE_MESSAGE_OUTBOX=false``)."""
    cid = campaign['id']
    if not uazapi_service:
        return
    # Leads pendentes no stage initial (chunk 2, 3, ...)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS cnt FROM campaign_leads
            WHERE campaign_id = %s
              AND status = 'pending'
              AND current_step = 1
              AND COALESCE(removed_from_funnel, FALSE) = FALSE
            """,
            (cid,),
        )
        row = cur.fetchone()
    if not row or int(row.get('cnt') or 0) <= 0:
        return

    # Instâncias Uazapi vinculadas
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
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
            (cid,),
        )
        instances = cur.fetchall() or []
    if not instances:
        return

    send_hour = int(campaign.get('send_hour_start') or 8)
    send_sat = bool(campaign.get('send_saturday'))
    send_sun = bool(campaign.get('send_sunday'))
    now_brazil = datetime.now(BRAZIL_TZ)
    camp_win = {
        "send_hour_start": campaign.get("send_hour_start"),
        "send_hour_end": campaign.get("send_hour_end"),
        "send_saturday": campaign.get("send_saturday"),
        "send_sunday": campaign.get("send_sunday"),
    }
    quota_allows = check_initial_chunk_daily_quota_for_campaign(cid)
    target_dt, use_immediate, same_day_reason = resolve_initial_chunk_schedule_target(
        now_brazil=now_brazil,
        send_hour_start=send_hour,
        send_sat=send_sat,
        send_sun=send_sun,
        scheduled_start_raw=campaign.get("scheduled_start"),
        campaign_send_window=camp_win,
        same_day_env_enabled=uazapi_same_day_initial_chunk_after_unlock_enabled(),
        quota_allows_today=quota_allows,
    )
    scheduled_for = target_dt.astimezone(pytz.UTC).replace(tzinfo=None)
    try:
        # TD-11 / TD-9: instante gravado deve cair em janela BRT + dias permitidos (sem skip silencioso).
        scheduled_for = next_valid_send_utc_naive(
            camp_win, scheduled_for, margin_minutes=0
        )
    except ValueError as e:
        print(
            json.dumps(
                {
                    "event": "uazapi_schedule_next_chunk_invalid_window",
                    "campaign_id": cid,
                    "error": str(e),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        return

    delay_min, delay_max = default_inter_message_delay_range_minutes()
    if campaign.get("delay_min_minutes") is not None:
        try:
            delay_min = int(campaign.get("delay_min_minutes"))
        except (TypeError, ValueError):
            pass
    if campaign.get("delay_max_minutes") is not None:
        try:
            delay_max = int(campaign.get("delay_max_minutes"))
        except (TypeError, ValueError):
            pass
    if delay_max < delay_min:
        delay_max = delay_min

    # Mensagens do step 1: campaign_steps ou campaigns.message_template (criação)
    variations = _load_step_messages(conn, cid, 1)
    if not variations:
        print(f"  ❌ [Initial Chunk] Campaign '{campaign.get('name')}': sem mensagem configurada (campaign_steps step 1 e campaigns.message_template vazios)")
        return

    # Task 6: reconciliar sends iniciais já materializados antes de agendar novo chunk (evita duplicidade pós-reconexão).
    try:
        sync_campaign_stage_sends_before_new_chunk(conn, cid, uazapi_service, stage="initial")
    except Exception as e:
        print(
            json.dumps(
                {
                    "event": "uazapi_schedule_next_chunk_pre_sync_failed",
                    "campaign_id": cid,
                    "error": str(e),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        try:
            conn.rollback()
        except Exception:
            pass

    # Um ciclo = chunks para todas as instâncias. Materialize atribui leads distintos por instância.
    created = 0
    for inst in sorted(instances, key=lambda x: x.get('instance_id') or 0):
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id FROM campaign_stage_sends
                WHERE campaign_id = %s AND stage = 'initial' AND instance_id = %s
                  AND status = ANY(%s)
                LIMIT 1
                """,
                (cid, inst['instance_id'], list(INITIAL_CHUNK_ACTIVE_SEND_STATUSES)),
            )
            if cur.fetchone():
                continue  # Instância já tem chunk ativo — evita duplicação
            cur.execute(
                """
                INSERT INTO campaign_stage_sends
                (campaign_id, stage, instance_id, scheduled_for, status, planned_count, lead_ids,
                 delay_min_minutes, delay_max_minutes, message_variations)
                VALUES (%s, 'initial', %s, %s, 'scheduled', 0, '[]'::jsonb, %s, %s, %s)
                """,
                (cid, inst['instance_id'], scheduled_for, delay_min, delay_max, json.dumps(variations)),
            )
            created += 1

    if created > 0:
        if use_immediate:
            with conn.cursor() as cur:
                cur.execute("UPDATE campaigns SET scheduled_start = NULL WHERE id = %s", (cid,))
        conn.commit()
        if same_day_reason:
            print(
                json.dumps(
                    {
                        "event": "uazapi_initial_chunk_scheduled",
                        "campaign_id": cid,
                        "reason": same_day_reason,
                        "within_send_window": True,
                        "instances": created,
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
        sched_brt = pytz.UTC.localize(scheduled_for).astimezone(BRAZIL_TZ).strftime("%d/%m %H:%M BRT")
        print(f"  📅 [Initial Chunk] Campaign '{campaign['name']}': agendado próximo chunk para {sched_brt} ({created} instâncias)")


def process_uazapi_initial_stage_rollovers(conn, campaigns_synced_this_tick=None):
    """
    Rollover automático (cadência + Uazapi): quando a pasta inicial contabilizou todos os slots
    (success_count + failed_count >= planned_count), move para FU1 os leads ainda em step 1 com
    status sent (falhas parciais na API não bloqueiam mais o avanço dos enviados).
    Se houver texto/mídia em campaign_steps step 2 e credenciais Uazapi, cria também a
    campanha agendada na API; caso contrário apenas atualiza o banco (Gerar no Kanban depois).
    Um registro em campaign_stage_sends = um folder Uazapi; cada um faz rollover independente.

    Task 5: com ``UAZAPI_RECONCILE_FIND_BEFORE_ROLLOVER`` (defeito 1), corre ``sync_campaign_leads_from_uazapi``
    antes de escolher ``rollover_leads`` — nunca promover FU só com ``log_sucess`` + ordem em ``lead_ids``.
    Com ``UAZAPI_LEAD_RECONCILE_V2=1``, aplica gate D9: não marca ``fu_rollover_done`` nem move leads
    enquanto existirem candidatos a ``message_find`` no escopo do send. Mutaciones no send usam
    ``SELECT … FOR UPDATE`` (D10) para concorrência entre workers.

    Args:
        campaigns_synced_this_tick: ``campaign_id`` já sincronizados neste tick do worker
        (ex.: por ``_sync_active_stage_folders``); evita segundo ``sync_campaign_leads_from_uazapi``
        HTTP para a mesma campanha — mantém-se o ``SELECT`` de reload do send na BD.
    """
    if not uazapi_service:
        return

    already_synced = campaigns_synced_this_tick or set()

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT css.id AS send_id, css.campaign_id, css.instance_id, css.instance_remote_jid,
                   css.uazapi_folder_id, css.lead_ids,
                   css.planned_count, css.success_count, css.failed_count,
                   c.name AS campaign_name, c.user_id AS user_id, c.send_hour_start, c.send_saturday, c.send_sunday,
                   i.apikey
            FROM campaign_stage_sends css
            JOIN campaigns c ON c.id = css.campaign_id
            JOIN instances i ON i.id = css.instance_id
            WHERE css.stage = 'initial'
              AND css.status IN ('done', 'partial')
              AND COALESCE(css.fu_rollover_done, FALSE) = FALSE
              AND c.enable_cadence = TRUE
              AND c.use_uazapi_sender = TRUE
              AND COALESCE(i.api_provider, 'megaapi') = 'uazapi'
            """
        )
        pending = cur.fetchall() or []

    for row in pending:
        cid = row["campaign_id"]
        send_id = row["send_id"]
        lids_raw = row.get("lead_ids") or []
        if isinstance(lids_raw, str):
            try:
                lids_raw = json.loads(lids_raw)
            except Exception:
                lids_raw = []
        planned = int(row.get("planned_count") or 0)
        if planned <= 0 and lids_raw:
            planned = len(lids_raw)
        succ = int(row.get("success_count") or 0)
        fail = int(row.get("failed_count") or 0)
        if planned <= 0:
            continue
        # Pasta processou todos os slots (API); falhas parciais não bloqueiam mais o rollover:
        # leads com status=sent seguem para FU1; falhos permanecem na Inicial.
        if succ + fail < planned:
            continue

        token = (row.get("apikey") or "").strip()
        folder_for_sync = row.get("uazapi_folder_id")
        instance_remote_jid = row.get("instance_remote_jid")

        if (
            _reconcile_find_before_rollover_enabled()
            and token
            and folder_for_sync
        ):
            if cid not in already_synced:
                try:
                    sync_campaign_leads_from_uazapi(conn, cid, token, folder_for_sync, uazapi_service)
                except Exception as e:
                    print(
                        f"  ⚠️ [Uazapi Rollover] '{row['campaign_name']}' send_id={send_id}: "
                        f"sync antes do rollover falhou ({e}); segue com contagens já carregadas."
                    )
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT planned_count, success_count, failed_count, lead_ids, instance_remote_jid
                    FROM campaign_stage_sends WHERE id = %s
                    """,
                    (send_id,),
                )
                ref = cur.fetchone()
            if ref:
                planned = int(ref.get("planned_count") or 0)
                succ = int(ref.get("success_count") or 0)
                fail = int(ref.get("failed_count") or 0)
                lr = ref.get("lead_ids") or []
                if isinstance(lr, str):
                    try:
                        lr = json.loads(lr)
                    except Exception:
                        lr = []
                lids_raw = lr
                if planned <= 0 and lids_raw:
                    planned = len(lids_raw)
                instance_remote_jid = ref.get("instance_remote_jid")

        if planned <= 0:
            continue
        if succ + fail < planned:
            continue

        send_row_scope = {
            "id": send_id,
            "lead_ids": lids_raw,
            "stage": "initial",
            "instance_id": row.get("instance_id"),
            "instance_remote_jid": instance_remote_jid,
        }
        if folder_for_sync and should_block_initial_rollover_for_pending_find(
            conn, cid, send_row_scope, folder_for_sync
        ):
            print(
                f"  ⏸️ [Uazapi Rollover] '{row['campaign_name']}' send_id={send_id}: "
                "ainda há candidatos a message_find neste send — rollover adiado até reconciliar (D9)."
            )
            continue

        if not lids_raw:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM campaign_stage_sends WHERE id = %s FOR UPDATE",
                    (send_id,),
                )
                if cur.fetchone() is None:
                    continue
                cur.execute(
                    "UPDATE campaign_stage_sends SET fu_rollover_done = TRUE, updated_at = NOW() WHERE id = %s",
                    (send_id,),
                )
            conn.commit()
            continue

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id FROM campaign_stage_sends WHERE id = %s FOR UPDATE",
                (send_id,),
            )
            if cur.fetchone() is None:
                continue
            cur.execute(
                """
                SELECT cl.id, cl.phone, cl.name, cl.whatsapp_link
                FROM campaign_leads cl
                WHERE cl.campaign_id = %s
                  AND cl.id = ANY(%s)
                  AND cl.current_step = 1
                  AND cl.status = 'sent'
                  AND COALESCE(cl.cadence_status, '') NOT IN ('converted', 'lost')
                """,
                (cid, lids_raw),
            )
            rollover_leads = cur.fetchall() or []
        if not rollover_leads:
            # Evita loop infinito: pasta já contabilizada (succ+fail>=planned) mas ninguém em step1/sent
            # (ex.: todos falharam, ids desalinhados ou já movidos).
            if succ + fail >= planned:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT id FROM campaign_stage_sends WHERE id = %s FOR UPDATE",
                        (send_id,),
                    )
                    if cur.fetchone() is not None:
                        cur.execute(
                            "UPDATE campaign_stage_sends SET fu_rollover_done = TRUE, updated_at = NOW() WHERE id = %s",
                            (send_id,),
                        )
                conn.commit()
                print(
                    f"  ⏭️ [Uazapi Rollover] '{row['campaign_name']}' send_id={send_id}: "
                    f"sem leads em Inicial+Enviado para mover; rollover marcado concluído."
                )
            continue

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT message_template, delay_days, media_path, media_type FROM campaign_steps WHERE campaign_id = %s AND step_number = 2 LIMIT 1",
                (cid,),
            )
            step2 = cur.fetchone()

        send_hour = int(row.get("send_hour_start") or 8)
        send_sat = bool(row.get("send_saturday"))
        send_sun = bool(row.get("send_sunday"))
        if step2 is not None:
            delay_days = step2.get("delay_days")
            delay_days = 1 if delay_days is None else int(delay_days)
        else:
            delay_days = 3
        now_brazil = datetime.now(BRAZIL_TZ)
        target_dt = cadence_next_send_datetime(now_brazil, delay_days, send_hour, send_sat, send_sun)
        scheduled_ts = int(target_dt.timestamp() * 1000)

        user_id = row.get("user_id")
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT email FROM users WHERE id = %s", (user_id,))
            urow = cur.fetchone()
        is_sa = urow and urow.get("email") in SUPER_ADMIN_EMAILS

        media_file_data = None
        media_type = "image"
        if step2 and is_sa and step2.get("media_path"):
            mp = step2["media_path"]
            if mp and _is_media_path_safe(mp, user_id) and os.path.exists(mp):
                try:
                    with open(mp, "rb") as f:
                        b64 = base64.b64encode(f.read()).decode("utf-8")
                    ext = os.path.splitext(mp)[1].lower()
                    mime_map = {
                        ".jpg": "image/jpeg",
                        ".jpeg": "image/jpeg",
                        ".png": "image/png",
                        ".gif": "image/gif",
                        ".mp4": "video/mp4",
                        ".webm": "video/webm",
                    }
                    mime = mime_map.get(ext, "application/octet-stream")
                    media_file_data = f"data:{mime};base64,{b64}"
                    media_type = step2.get("media_type") or "image"
                except Exception as e:
                    print(f"  ⚠️ [Uazapi Rollover] Erro mídia step2: {e}")

        msg_text = ""
        if step2:
            raw_tpl = step2.get("message_template") or "[]"
            try:
                parsed = json.loads(raw_tpl)
                if isinstance(parsed, list):
                    msg_text = random.choice(parsed) if parsed else ""
                else:
                    msg_text = str(parsed) if parsed else ""
            except Exception:
                msg_text = str(raw_tpl)

        can_api = bool(token and row.get("uazapi_folder_id"))
        has_body = bool((msg_text or "").strip()) or bool(media_file_data)
        want_api = can_api and has_body
        if USE_MESSAGE_OUTBOX and _campaign_has_message_outbox(conn, cid):
            want_api = False

        messages = []
        moved_ids = []
        for lead in rollover_leads:
            phone = lead.get("phone") or ""
            if not phone and lead.get("whatsapp_link"):
                match = re.search(r"(\d{10,})", str(lead["whatsapp_link"]))
                if match:
                    phone = match.group(1)
            if not phone:
                continue
            clean = re.sub(r"\D", "", str(phone))
            if len(clean) <= 11 and not clean.startswith("55"):
                clean = "55" + clean
            name = lead.get("name") or "Visitante"
            moved_ids.append(lead["id"])
            if not want_api:
                continue
            text = (
                msg_text.replace("{{nome}}", name)
                .replace("{{name}}", name)
                .replace("{nome}", name)
                .replace("{name}", name)
            )
            if media_file_data:
                messages.append({"number": clean, "type": media_type, "file": media_file_data, "text": text})
            else:
                messages.append({"number": clean, "type": "text", "text": text})

        if not moved_ids:
            continue

        api_ok = False
        if want_api and messages:
            result = uazapi_service.create_advanced_campaign(
                token=token,
                delay_min_sec=60,
                delay_max_sec=120,
                messages=messages,
                info=f"Auto FU1 c{cid} send{send_id}",
                scheduled_for=scheduled_ts,
            )
            if result:
                folder_id = result.get("folder_id") or result.get("folderId")
                if folder_id:
                    merge_fu1_into_campaign_db(conn, cid, str(folder_id), str(send_id))
                    api_ok = True
                else:
                    print(f"  ⚠️ [Uazapi Rollover] Campaign '{row['campaign_name']}': API sem folder_id; leads ainda movidos para FU1 no banco.")
            else:
                print(f"  ❌ [Uazapi Rollover] Campaign '{row['campaign_name']}': create_advanced_campaign FU1 falhou; leads movidos só no banco.")
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM campaign_stage_sends WHERE id = %s FOR UPDATE",
                (send_id,),
            )
            if cur.fetchone() is None:
                continue
            cur.execute(
                """
                UPDATE campaign_leads
                SET current_step = 2, cadence_status = 'snoozed', snooze_until = %s, status = 'sent'
                WHERE id = ANY(%s)
                """,
                (target_dt, moved_ids),
            )
            cur.execute(
                "UPDATE campaign_stage_sends SET fu_rollover_done = TRUE, updated_at = NOW() WHERE id = %s",
                (send_id,),
            )
        conn.commit()
        if api_ok:
            print(
                f"  🔄 [Uazapi Rollover] '{row['campaign_name']}' send_id={send_id}: {len(moved_ids)} leads → FU1, agendado {target_dt.strftime('%d/%m %H:%M')} BRT"
            )
        else:
            print(
                f"  🔄 [Uazapi Rollover] '{row['campaign_name']}' send_id={send_id}: {len(moved_ids)} leads → coluna FU1 (snooze {target_dt.strftime('%d/%m %H:%M')} BRT); use Gerar no Kanban para criar envio Uazapi"
            )


def process_rollover(campaign, conn):
    """
    Rollover diário: às rollover_time, leads em Inicial (current_step=1) que constam
    em list_messages(Sent) da API → mover para Follow-up 1 e criar campanha Uazapi agendada.
    API é fonte de verdade (não depende de campaign_leads.status).
    Só processa instâncias Uazapi.
    """
    cid = campaign['id']
    if USE_MESSAGE_OUTBOX and _campaign_has_message_outbox(conn, cid):
        return
    cadence_config = campaign.get('cadence_config') or {}
    if isinstance(cadence_config, str):
        try:
            cadence_config = json.loads(cadence_config) if cadence_config else {}
        except json.JSONDecodeError:
            cadence_config = {}
    rollover_str = cadence_config.get('rollover_time', '23:00')
    rollover_test_mode = bool(cadence_config.get('rollover_test_mode', False))
    rollover_h, rollover_m = _parse_rollover_time(rollover_str)

    now_brazil = datetime.now(BRAZIL_TZ)
    # Modo teste OU 00:00: roda em todo ciclo. Caso contrário: só quando hora >= rollover_time
    if not rollover_test_mode and rollover_str != '00:00':
        now_minutes = now_brazil.hour * 60 + now_brazil.minute
        rollover_minutes = rollover_h * 60 + rollover_m
        if now_minutes < rollover_minutes:
            return

    instance = get_campaign_instance(cid, conn)
    if not instance:
        print(f"  ⏭️ [Rollover] Campaign '{campaign['name']}': sem instância vinculada, pulando.")
        return
    if instance.get('api_provider') != 'uazapi':
        print(f"  ⏭️ [Rollover] Campaign '{campaign['name']}': instância {instance.get('api_provider', '?')}, requer Uazapi.")
        return
    if not uazapi_service:
        print(f"  ⏭️ [Rollover] Campaign '{campaign['name']}': UazapiService indisponível.")
        return

    # Sync Uazapi → DB antes do rollover (marca envios para Kanban/stats; rollover não depende mais dele)
    if campaign.get('use_uazapi_sender') and campaign.get('uazapi_folder_id') and instance.get('apikey'):
        try:
            sync_result = sync_campaign_leads_from_uazapi(
                conn, cid, instance['apikey'], campaign['uazapi_folder_id'], uazapi_service
            )
            if sync_result.get('updated_sent') or sync_result.get('updated_failed'):
                print(f"  🔄 [Rollover] Campaign '{campaign['name']}': sync Uazapi → {sync_result}")
            elif sync_result.get('sent', 0) > 0 and sync_result.get('updated_sent', 0) == 0:
                print(f"  ⚠️ [Rollover] Campaign '{campaign['name']}': API retornou {sync_result.get('sent')} Sent mas 0 atualizados no DB (verificar match de telefone)")
        except Exception as e:
            print(f"  ⚠️ [Rollover] Campaign '{campaign['name']}': sync Uazapi falhou: {e}")

    # Verificar se campanha inicial terminou (Scheduled=0) e obter sent_phones da API
    if not campaign.get('uazapi_folder_id') or not instance.get('apikey'):
        print(f"  ⏭️ [Rollover] Campaign '{campaign['name']}': sem uazapi_folder_id ou apikey, pulando.")
        return
    counts = get_uazapi_campaign_counts(uazapi_service, instance['apikey'], campaign['uazapi_folder_id'])
    if not is_initial_campaign_finished(counts):
        print(f"  ⏭️ [Rollover] Campaign '{campaign['name']}': campanha inicial ainda enviando (scheduled={counts.get('scheduled', 0)}). Aguardando.")
        return

    # API como fonte de verdade: obter sent_phones de list_messages(Sent)
    sent_phones = fetch_all_phones_by_status(
        uazapi_service, instance['apikey'], campaign['uazapi_folder_id'], "Sent"
    )
    sent_normalized = set()
    for ph in sent_phones:
        sent_normalized |= normalize_phone_for_match(ph)

    # Buscar leads em Inicial (current_step=1), sem filtro de status. Exclui converted/lost.
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT cl.id, cl.phone, cl.name, cl.whatsapp_link, cl.sent_at
            FROM campaign_leads cl
            WHERE cl.campaign_id = %s
              AND cl.current_step = 1
              AND (cl.cadence_status IS NULL OR cl.cadence_status IN ('snoozed', 'pending'))
              AND COALESCE(cl.cadence_status, '') NOT IN ('converted', 'lost')
            LIMIT 100
        """, (cid,))
        initial_leads = cur.fetchall()

    # Match por normalização: lead elegível se phone/whatsapp_link intersecta sent_normalized
    rollover_leads = []
    for lead in initial_leads:
        lead_variants = normalize_phone_for_match(lead.get('phone')) | normalize_phone_for_match(
            lead.get('whatsapp_link')
        )
        if lead_variants and (lead_variants & sent_normalized):
            rollover_leads.append(lead)

    # Modo teste + delay: só rollover se MIN(sent_at) >= N minutos entre elegíveis
    rollover_test_delay_minutes = int(cadence_config.get('rollover_test_delay_minutes', 5))
    if rollover_test_mode and rollover_test_delay_minutes > 0 and rollover_leads:
        sent_ats = [l.get('sent_at') for l in rollover_leads if l.get('sent_at')]
        if sent_ats:
            min_sent = min(sent_ats)
            if getattr(min_sent, 'tzinfo', None) is None:
                min_sent = BRAZIL_TZ.localize(min_sent)
            elapsed_min = (now_brazil - min_sent).total_seconds() / 60
            if elapsed_min < rollover_test_delay_minutes:
                return  # Aguardar delay

    if not rollover_leads:
        if sent_phones:
            print(f"  ⏭️ [Rollover] Campaign '{campaign['name']}': API retornou {len(sent_phones)} Sent mas 0 leads em Inicial deram match.")
        else:
            print(f"  ⏭️ [Rollover] Campaign '{campaign['name']}': 0 Sent na API, nenhum lead elegível.")
        return

    print(f"  🔄 [Rollover] Campaign '{campaign['name']}': {len(rollover_leads)} leads elegíveis, criando campanha FU1...")

    # Step 2 config (incl. media_path para Uazapi type image/video)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT message_template, delay_days, media_path, media_type FROM campaign_steps WHERE campaign_id = %s AND step_number = 2 LIMIT 1",
            (cid,),
        )
        step2 = cur.fetchone()
    if not step2:
        print(f"  ⏭️ [Rollover] Campaign '{campaign['name']}': step 2 (Follow-up 1) não configurado.")
        return

    send_hour = int(campaign.get('send_hour_start') or 8)
    send_sat = bool(campaign.get('send_saturday'))
    send_sun = bool(campaign.get('send_sunday'))
    delay_days = step2.get('delay_days')
    delay_days = 1 if delay_days is None else int(delay_days)

    target_dt = cadence_next_send_datetime(now_brazil, delay_days, send_hour, send_sat, send_sun)
    scheduled_ts = int(target_dt.timestamp() * 1000)  # Uazapi espera ms

    # Gate superadmin para mídia
    user_id = campaign.get('user_id')
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT email FROM users WHERE id = %s", (user_id,))
        row = cur.fetchone()
    is_sa = row and row.get('email') in SUPER_ADMIN_EMAILS

    # Mídia step 2 (superadmin only)
    media_file_data = None
    media_type = 'image'
    if is_sa and step2.get('media_path'):
        mp = step2['media_path']
        if mp and _is_media_path_safe(mp, user_id) and os.path.exists(mp):
            try:
                with open(mp, 'rb') as f:
                    b64 = base64.b64encode(f.read()).decode('utf-8')
                ext = os.path.splitext(mp)[1].lower()
                mime_map = {'.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png', '.gif': 'image/gif', '.mp4': 'video/mp4', '.webm': 'video/webm'}
                mime = mime_map.get(ext, 'application/octet-stream')
                media_file_data = f"data:{mime};base64,{b64}"
                media_type = step2.get('media_type') or 'image'
            except Exception as e:
                print(f"  ⚠️ [Rollover] Erro ao ler mídia step 2: {e}")

    # Re-query imediatamente antes de criar campanha (excluir leads movidos para Convertido/Perdido)
    lead_ids = [l['id'] for l in rollover_leads]
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT cl.id, cl.phone, cl.name, cl.whatsapp_link
            FROM campaign_leads cl
            WHERE cl.id = ANY(%s)
              AND cl.campaign_id = %s
              AND cl.current_step = 1
              AND COALESCE(cl.cadence_status, '') NOT IN ('converted', 'lost')
        """, (lead_ids, cid))
        rollover_leads = cur.fetchall()

    if not rollover_leads:
        return

    raw_tpl = step2.get('message_template') or '[]'
    try:
        parsed = json.loads(raw_tpl)
        msg_text = random.choice(parsed) if isinstance(parsed, list) else str(parsed)
    except Exception:
        msg_text = str(raw_tpl)

    messages = []
    for lead in rollover_leads:
        phone = lead.get('phone') or ''
        if not phone and lead.get('whatsapp_link'):
            match = re.search(r'(\d{10,})', str(lead['whatsapp_link']))
            if match:
                phone = match.group(1)
        if not phone:
            continue
        clean = re.sub(r'\D', '', str(phone))
        if len(clean) <= 11 and not clean.startswith('55'):
            clean = '55' + clean
        name = lead.get('name') or 'Visitante'
        text = msg_text.replace('{{nome}}', name).replace('{{name}}', name).replace('{nome}', name).replace('{name}', name)
        if media_file_data:
            messages.append({'number': clean, 'type': media_type, 'file': media_file_data, 'text': text})
        else:
            messages.append({'number': clean, 'type': 'text', 'text': text})

    if not messages:
        print(f"  ⏭️ [Rollover] Campaign '{campaign['name']}': nenhum telefone válido nos leads.")
        return

    token = instance.get('apikey')
    if not token:
        print(f"  ⏭️ [Rollover] Campaign '{campaign['name']}': instância sem apikey.")
        return

    result = uazapi_service.create_advanced_campaign(
        token=token,
        delay_min_sec=60,
        delay_max_sec=120,
        messages=messages,
        info=f"Rollover FU1 c{cid}",
        scheduled_for=scheduled_ts,
    )

    if not result:
        print(f"  ❌ [Rollover] Campaign '{campaign['name']}': Uazapi create_advanced_campaign falhou. Leads NÃO movidos.")
        return

    folder_id = result.get('folder_id') or result.get('folderId')
    # Sucesso: mover leads para Follow-up 1 (current_step=2, cadence_status=snoozed, status=sent para consistência)
    lead_ids = [l['id'] for l in rollover_leads]
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE campaign_leads
            SET current_step = 2, cadence_status = 'snoozed', snooze_until = %s, status = 'sent'
            WHERE id = ANY(%s)
            """,
            (target_dt, lead_ids),
        )
    if folder_id:
        merge_fu1_into_campaign_db(conn, cid, str(folder_id), "legacy_time_rollover")
    conn.commit()
    print(f"  🔄 [Rollover] Campaign '{campaign['name']}': {len(lead_ids)} leads Inicial → Follow-up 1, agendado {target_dt.strftime('%d/%m %H:%M')} BRT")


def process_rollover_fu_next(
    campaign, conn, from_step, to_step, step_label, campaigns_synced_this_tick=None
):
    """
    Rollover por tempo (snooze): FU1→FU2 ou FU2→Despedida.

    Mapeamento ``campaign_stage_sends.stage`` (UAZAPI) ↔ coluna de cadência (``current_step``):
    - ``initial`` — envio da etapa Inicial (step 1); rollover automático em ``process_uazapi_initial_stage_rollovers``.
    - ``follow1`` — pasta do Follow-up 1 (leads em ``current_step`` 2 após rollover inicial).
    - ``follow2`` — pasta do Follow-up 2 (step 3).
    - ``breakup`` — pasta da Despedida (step 4).

    **Task 7 / product-rules §5.3:** não promover a etapa seguinte só com ``snooze_until`` expirado se o
    estado ``status='sent'`` + ``last_sent_stage`` ainda puder estar desalinhado do ``message_find``.
    Com ``_reconcile_find_before_rollover_enabled()`` (env ``UAZAPI_RECONCILE_FIND_BEFORE_ROLLOVER``,
    defeito ligado), corre-se ``sync_campaign_leads_from_uazapi`` antes de selecionar leads — o mesmo
    pipeline que reconcilia ``follow1``/``follow2``/``breakup`` em ``campaign_stage_sends``. O token
    da instância retornada por ``get_campaign_instance`` é opcional: o sync usa ``apikey`` por send na BD.

    ``required_last_stage`` garante que só entram leads cuja última etapa confirmada na BD corresponde
    ao send anterior (ex.: FU2→Despedida exige ``last_sent_stage='follow2'``).
    """
    cid = campaign['id']
    instance = get_campaign_instance(cid, conn)
    if not instance or instance.get('api_provider') != 'uazapi' or not uazapi_service:
        return

    # Sync completo da campanha (todas as pastas em campaign_stage_sends + find no escopo) antes
    # de confiar em status=sent para o próximo create_advanced_campaign.
    # ``token`` pode ser vazio: ``sync_campaign_leads_from_uazapi`` usa apikey por send na BD.
    if _reconcile_find_before_rollover_enabled() and campaign.get('use_uazapi_sender'):
        try:
            sync_token = (instance.get('apikey') or '').strip() if instance else ''
            sync_campaign_leads_from_uazapi(
                conn, cid, sync_token, campaign.get('uazapi_folder_id'), uazapi_service
            )
        except Exception as e:
            print(f"  ⚠️ [Rollover {step_label}] Sync pré-decisão falhou: {e}")

    required_last_stage = {2: 'follow1', 3: 'follow2'}.get(from_step)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        extra_stage_clause = ""
        params = [cid, from_step]
        if required_last_stage:
            extra_stage_clause = "AND COALESCE(cl.last_sent_stage, '') = %s"
            params.append(required_last_stage)

        cur.execute(f"""
            SELECT cl.id, cl.phone, cl.name, cl.whatsapp_link
            FROM campaign_leads cl
            WHERE cl.campaign_id = %s
              AND cl.current_step = %s
              AND cl.status = 'sent'
              AND cl.cadence_status = 'snoozed'
              AND cl.snooze_until <= NOW()
              AND COALESCE(cl.cadence_status, '') NOT IN ('converted', 'lost')
              {extra_stage_clause}
            LIMIT 100
        """, tuple(params))
        rollover_leads = cur.fetchall()

    if not rollover_leads:
        return

    # delay_days vem da etapa de destino (mesmo "Aguardar após etapa anterior" do formulário / campaign_steps).
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT message_template, delay_days, media_path, media_type FROM campaign_steps WHERE campaign_id = %s AND step_number = %s LIMIT 1",
            (cid, to_step),
        )
        step_cfg = cur.fetchone()
    if not step_cfg:
        print(f"  ⏭️ [Rollover {step_label}] Campaign '{campaign['name']}': step {to_step} não configurado.")
        return

    send_hour = int(campaign.get('send_hour_start') or 8)
    send_sat = bool(campaign.get('send_saturday'))
    send_sun = bool(campaign.get('send_sunday'))
    delay_days = step_cfg.get('delay_days')
    delay_days = 1 if delay_days is None else int(delay_days)
    now_brazil = datetime.now(BRAZIL_TZ)
    target_dt = cadence_next_send_datetime(now_brazil, delay_days, send_hour, send_sat, send_sun)
    scheduled_ts = int(target_dt.timestamp() * 1000)

    # Gate superadmin para mídia
    user_id = campaign.get('user_id')
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT email FROM users WHERE id = %s", (user_id,))
        row = cur.fetchone()
    is_sa = row and row.get('email') in SUPER_ADMIN_EMAILS

    # Mídia do step (superadmin only)
    media_file_data = None
    media_type = 'image'
    if is_sa and step_cfg.get('media_path'):
        mp = step_cfg['media_path']
        if mp and _is_media_path_safe(mp, user_id) and os.path.exists(mp):
            try:
                with open(mp, 'rb') as f:
                    b64 = base64.b64encode(f.read()).decode('utf-8')
                ext = os.path.splitext(mp)[1].lower()
                mime_map = {'.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png', '.gif': 'image/gif', '.mp4': 'video/mp4', '.webm': 'video/webm'}
                mime = mime_map.get(ext, 'application/octet-stream')
                media_file_data = f"data:{mime};base64,{b64}"
                media_type = step_cfg.get('media_type') or 'image'
            except Exception as e:
                print(f"  ⚠️ [Rollover {step_label}] Erro ao ler mídia: {e}")

    # Re-query imediatamente antes de criar campanha (excluir leads movidos para Convertido/Perdido)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        extra_stage_clause = ""
        params = [cid, from_step]
        if required_last_stage:
            extra_stage_clause = "AND COALESCE(cl.last_sent_stage, '') = %s"
            params.append(required_last_stage)

        cur.execute(f"""
            SELECT cl.id, cl.phone, cl.name, cl.whatsapp_link
            FROM campaign_leads cl
            WHERE cl.campaign_id = %s
              AND cl.current_step = %s
              AND cl.status = 'sent'
              AND cl.cadence_status = 'snoozed'
              AND cl.snooze_until <= NOW()
              AND COALESCE(cl.cadence_status, '') NOT IN ('converted', 'lost')
              {extra_stage_clause}
            LIMIT 100
        """, tuple(params))
        rollover_leads = cur.fetchall()

    if not rollover_leads:
        return

    raw_tpl = step_cfg.get('message_template') or '[]'
    try:
        parsed = json.loads(raw_tpl)
        msg_text = random.choice(parsed) if isinstance(parsed, list) else str(parsed)
    except Exception:
        msg_text = str(raw_tpl)

    messages = []
    for lead in rollover_leads:
        phone = lead.get('phone') or ''
        if not phone and lead.get('whatsapp_link'):
            match = re.search(r'(\d{10,})', str(lead['whatsapp_link']))
            if match:
                phone = match.group(1)
        if not phone:
            continue
        clean = re.sub(r'\D', '', str(phone))
        if len(clean) <= 11 and not clean.startswith('55'):
            clean = '55' + clean
        name = lead.get('name') or 'Visitante'
        text = msg_text.replace('{{nome}}', name).replace('{{name}}', name).replace('{nome}', name).replace('{name}', name)
        if media_file_data:
            messages.append({'number': clean, 'type': media_type, 'file': media_file_data, 'text': text})
        else:
            messages.append({'number': clean, 'type': 'text', 'text': text})

    if not messages:
        return

    token = instance.get('apikey')
    if not token:
        return

    lead_ids = [l['id'] for l in rollover_leads]
    if USE_MESSAGE_OUTBOX and _campaign_has_message_outbox(conn, cid):
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE campaign_leads
                SET current_step = %s, cadence_status = 'snoozed', snooze_until = %s
                WHERE id = ANY(%s)
                """,
                (to_step, target_dt, lead_ids),
            )
        conn.commit()
        print(
            f"  🔄 [Rollover→Outbox] Campaign '{campaign['name']}': {len(lead_ids)} leads → "
            f"{step_label} (snooze {target_dt.strftime('%d/%m %H:%M')} BRT); envio via outbox"
        )
        return

    result = uazapi_service.create_advanced_campaign(
        token=token,
        delay_min_sec=60,
        delay_max_sec=120,
        messages=messages,
        info=f"Rollover {step_label} c{cid}",
        scheduled_for=scheduled_ts,
    )

    if not result:
        print(f"  ❌ [Rollover {step_label}] Campaign '{campaign['name']}': Uazapi create_advanced_campaign falhou.")
        return

    folder_id = result.get('folder_id') or result.get('folderId')
    config_key = {3: 'rollover_fu2_folder_id', 4: 'rollover_fu3_folder_id'}.get(to_step)

    lead_ids = [l['id'] for l in rollover_leads]
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE campaign_leads
            SET current_step = %s, cadence_status = 'snoozed', snooze_until = %s
            WHERE id = ANY(%s)
            """,
            (to_step, target_dt, lead_ids),
        )
        if folder_id and config_key:
            cur.execute(
                """
                UPDATE campaigns SET cadence_config = COALESCE(cadence_config, '{}')::jsonb || %s::jsonb
                WHERE id = %s
                """,
                (json.dumps({config_key: str(folder_id)}), cid),
            )
    conn.commit()
    print(f"  🔄 [Rollover] Campaign '{campaign['name']}': {len(lead_ids)} leads → {step_label}, agendado {target_dt.strftime('%d/%m %H:%M')} BRT")


def bootstrap_pending_leads(campaign, conn):
    """
    Handles leads that were sent by worker_sender but never entered the cadence cycle.
    These leads have status='sent' and cadence_status='pending' (or NULL).
    Sets them to 'snoozed' with snooze_until = now, so they are immediately
    picked up by process_campaign_sends on the next poll.
    Also tries to discover their Chatwoot conversation ID if missing.
    """
    cid = campaign['id']

    # Uazapi + modo manual no Kanban: não fazer bootstrap automático
    # para evitar conflito com o fluxo "Gerar Campanha" por etapa.
    cfg = campaign.get('cadence_config') or {}
    if isinstance(cfg, str):
        try:
            cfg = json.loads(cfg) if cfg else {}
        except Exception:
            cfg = {}
    setup_mode = str(cfg.get('cadence_setup_mode') or '').strip().lower()
    if campaign.get('use_uazapi_sender') and setup_mode == 'kanban_later':
        return
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT id, phone, name, chatwoot_conversation_id, current_step
            FROM campaign_leads
            WHERE campaign_id = %s
              AND status = 'sent'
              AND (cadence_status IS NULL OR cadence_status = 'pending')
            LIMIT 50
        """, (cid,))
        pending_leads = cur.fetchall()
    
    if not pending_leads:
        return
    
    print(f"  🔄 Campaign '{campaign['name']}': Bootstrapping {len(pending_leads)} pending sent leads into cadence...")
    
    # Build next-step delay map to preserve stage progression for all steps.
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT step_number, delay_days FROM campaign_steps WHERE campaign_id = %s",
            (cid,)
        )
        steps = cur.fetchall() or []
    delay_by_step = {}
    for s in steps:
        try:
            delay_by_step[int(s.get('step_number'))] = int(s.get('delay_days')) if s.get('delay_days') is not None else 1
        except Exception:
            continue
    max_step = max(delay_by_step.keys()) if delay_by_step else 4

    for lead in pending_leads:
        lead_id = lead['id']
        conv_id = lead['chatwoot_conversation_id']
        try:
            current_step = int(lead.get('current_step') or 1)
        except Exception:
            current_step = 1
        if current_step < 1:
            current_step = 1

        next_step = current_step + 1 if current_step < max_step else current_step
        delay_days = delay_by_step.get(next_step, 1)
        now_br = datetime.now(BRAZIL_TZ)
        snooze_until = now_br + timedelta(minutes=2) if delay_days <= 0 else now_br + timedelta(days=delay_days)
        
        # Try to discover Chatwoot conversation if missing
        if not conv_id:
            conv_id = discover_chatwoot_conversation(lead['phone'], lead.get('name'))
            if conv_id:
                with conn.cursor() as cur:
                    cur.execute("UPDATE campaign_leads SET chatwoot_conversation_id = %s WHERE id = %s", (conv_id, lead_id))
                conn.commit()
                print(f"    🔗 Lead #{lead_id}: Linked to Chatwoot conv {conv_id}")
                time.sleep(0.3)  # Rate limit
        
        # Set to snoozed so cadence worker picks them up.
        # IMPORTANT: never overwrite current_step (prevents regression).
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE campaign_leads 
                SET cadence_status = 'snoozed', 
                    snooze_until = %s,
                    last_message_sent_at = COALESCE(last_message_sent_at, sent_at, NOW())
                WHERE id = %s
            """, (snooze_until, lead_id))
        conn.commit()

    print(f"  ✅ {len(pending_leads)} leads bootstrapped into cadence.")


def process_campaign_sends(campaign, conn):
    cid = campaign['id']
    user_id = campaign.get('user_id')
    instance = get_campaign_instance(cid, conn)
    if not instance:
        return
    if instance.get('api_provider') != 'uazapi':
        print(
            f"  ⏭️ Campaign '{campaign['name']}': instância legada ({instance.get('api_provider')!r}); "
            "apenas Uazapi. Pulando follow-ups."
        )
        return

    # Gate superadmin (mídia Uazapi apenas para superadmin)
    is_sa = False
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT email FROM users WHERE id = %s", (user_id,))
        row = cur.fetchone()
    if row and row.get('email') in SUPER_ADMIN_EMAILS:
        is_sa = True

    instance_name = instance['name']
    api_provider = instance.get('api_provider') or 'megaapi'

    # Get steps
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM campaign_steps WHERE campaign_id = %s ORDER BY step_number ASC", (cid,))
        steps = cur.fetchall()
    
    if not steps: return
    steps_by_number = {s['step_number']: s for s in steps}
    max_step = max(s['step_number'] for s in steps)

    # Find leads ready for follow-up (snooze expired)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT cl.id, cl.phone, cl.name, cl.current_step, cl.cadence_status, cl.whatsapp_link, cl.chatwoot_conversation_id
            FROM campaign_leads cl
            WHERE cl.campaign_id = %s
              AND cl.cadence_status = 'snoozed'
              AND cl.snooze_until <= NOW()
            ORDER BY COALESCE(cl.csv_row_order, cl.id) ASC, cl.id ASC, cl.snooze_until ASC
            LIMIT 20
        """, (cid,))
        ready_leads = cur.fetchall()

    if not ready_leads: return

    print(f"  📨 Campaign '{campaign['name']}': {len(ready_leads)} leads ready for follow-up")

    for lead in ready_leads:
        lead_id = lead['id']
        conv_id = lead['chatwoot_conversation_id']
        current_step = lead['current_step'] or 1
        next_step = current_step + 1

        # If no Chatwoot conversation ID, try to discover it
        if not conv_id:
            conv_id = discover_chatwoot_conversation(lead['phone'], lead.get('name'))
            if conv_id:
                with conn.cursor() as cur:
                    cur.execute("UPDATE campaign_leads SET chatwoot_conversation_id = %s WHERE id = %s", (conv_id, lead_id))
                conn.commit()

        state_stop = False
        state_reason = ""

        # --- DECISION MATRIX (Pre-Send) ---
        cw_data = get_chatwoot_conversation_details(conv_id)
        unread = cw_data.get('unread_count', 0) if cw_data else 0

        if cw_data:
            cw_labels = cw_data.get('labels', [])
            cw_status = cw_data.get('status')  # open, snoozed, resolved

            # A. Check Labels (Hard Stop)
            stop_labels = ['01-interessado', '02-demo', '03-negociacao', '04-ganho']
            lost_labels = ['05-perdido']

            if any(l in cw_labels for l in stop_labels):
                state_stop = True
                state_reason = f"Label Stop: {list(set(cw_labels) & set(stop_labels))}"

            elif any(l in cw_labels for l in lost_labels):
                state_stop = True
                state_reason = "Label Lost"

            # B. Check Context (Smart Pause)
            if not state_stop:
                if unread > 0:
                    print(f"    ⏸️ Lead #{lead_id}: Has {unread} unread messages. Pausing.")
                    continue

                # Check last message sender
                messages = get_chatwoot_conversation_messages(conv_id)
                last_sender_is_contact = False
                if messages:
                    for msg in reversed(messages):
                        mtype = msg.get('message_type')
                        if mtype in [0, 1]:
                            if mtype == 0:
                                last_sender_is_contact = True
                            break
                
                if last_sender_is_contact:
                    print(f"    ⏸️ Lead #{lead_id}: Last message is from contact. Pausing.")
                    continue
        else:
            if not conv_id:
                pass  # No Chatwoot ID yet, proceed with WhatsApp-only send
            else:
                print(f"    ⚠️ Lead #{lead_id}: Chatwoot fetch failed. Proceeding anyway.")

        # Handle Stop State
        if state_stop:
            with conn.cursor() as cur:
                cur.execute("UPDATE campaign_leads SET cadence_status = 'stopped', log = %s WHERE id = %s", (state_reason, lead_id))
            conn.commit()
            print(f"    🛑 Lead #{lead_id}: {state_reason}")
            continue

        # --- SENDING LOGIC ---
        step_config = steps_by_number.get(next_step)
        if not step_config:
            # End of cadence
            with conn.cursor() as cur:
                cur.execute("UPDATE campaign_leads SET cadence_status = 'completed' WHERE id = %s", (lead_id,))
            conn.commit()
            if conv_id:
                toggle_chatwoot_status(conv_id, 'resolved')
            print(f"    🏁 Lead #{lead_id}: Cadence completed (no more steps).")
            continue

        # Prepare Message
        phone = lead['phone']
        if not phone and lead.get('whatsapp_link'):
             match = re.search(r'(\d{10,})', str(lead['whatsapp_link']))
             if match: phone = match.group(1)
        
        if not phone:
             print(f"    ⚠️ Lead #{lead_id}: No phone.")
             continue
             
        phone_jid = format_jid(phone)
        
        raw_template = step_config['message_template']
        if not raw_template: continue

        message = ""
        try:
            parsed = json.loads(raw_template)
            if isinstance(parsed, list):
                message = random.choice(parsed)
            elif isinstance(parsed, str):
                message = parsed
            else:
                message = str(parsed)
        except:
            message = raw_template
        lead_name = lead.get('name', 'Visitante')
        message = message.replace('{{nome}}', lead_name).replace('{{name}}', lead_name)

        phone_num = re.sub(r'\D', '', str(phone))
        if len(phone_num) <= 11 and not phone_num.startswith('55'):
            phone_num = '55' + phone_num

        # Uazapi + superadmin + media: enviar APENAS mídia com caption (não enviar texto separado)
        sent_ok = False
        sent_via_uazapi_media = False
        if step_config.get('media_path') and is_sa and uazapi_service and instance.get('apikey'):
            if _is_media_path_safe(step_config['media_path'], user_id) and os.path.exists(step_config['media_path']):
                result = uazapi_service.send_media(
                    instance['apikey'], phone_num,
                    step_config.get('media_type', 'image'),
                    step_config['media_path'],
                    caption=message
                )
                sent_ok = bool(result)
                sent_via_uazapi_media = True

        # Send Text (quando não enviou via mídia Uazapi)
        if not sent_via_uazapi_media:
            result = uazapi_service.send_text(instance['apikey'], phone_num, message)
            sent_ok = bool(result)

        if sent_ok:
            # SUCCESS: Enter MONITORING state (Safety Buffer)
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE campaign_leads
                    SET current_step = %s,
                        cadence_status = 'monitoring',
                        last_message_sent_at = NOW(),
                        snooze_until = NULL
                    WHERE id = %s
                """, (next_step, lead_id))
            conn.commit()
            print(f"    ✅ Lead #{lead_id}: Step {next_step} sent ({api_provider}). Entering 5m Safety Buffer.")
        else:
            print(f"    ❌ Lead #{lead_id}: Send failed.")

        # Cooldown
        time.sleep(random.randint(20, 40))

if __name__ == "__main__":
    process_cadence()
