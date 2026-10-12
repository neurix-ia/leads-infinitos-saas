"""
Configurações carregadas de variáveis de ambiente.
Fonte única para valores sensíveis ou que variam por ambiente.
"""

import os


def _parse_super_admin_emails():
    raw = os.environ.get(
        "SUPER_ADMIN_EMAILS",
        "augustogumi@gmail.com,ricardo.ost@gmail.com",
    )
    return tuple(e.strip() for e in raw.split(",") if e.strip())


SUPER_ADMIN_EMAILS = _parse_super_admin_emails()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


# Fila Postgres ``campaign_message_outbox`` (envio unitário ``/send/text`` via worker) em vez de
# pastas ``create_advanced_campaign`` na Uazapi. Default: ligado (``USE_MESSAGE_OUTBOX=false``
# só para rollback de emergência).
USE_MESSAGE_OUTBOX = _env_bool("USE_MESSAGE_OUTBOX", True)

# Criação de campanha: com ``USE_MESSAGE_OUTBOX`` activo, todos os utilizadores entram na fila
# ``campaign_message_outbox`` (envio unitário). APIs admin de polling/pausa outbox (fase 1)
# continuam restritas a ``SUPER_ADMIN_EMAILS``. ``campaigns.created_by_admin_id`` é só auditoria
# (painel admin); não define modo de envio.

# --- Disparo via n8n (passo 3 de docs/INTEGRACAO_CRM_N8N.md) ---
# ``USE_N8N_DISPATCH``: regista as rotas /api/n8n/tools/* e POST /api/campaigns/<id>/n8n-dispatch/<step>.
# O envio em si é o workflow "Disparador CRM Simple" do n8n, o mesmo do CRM Neurix.
USE_N8N_DISPATCH = _env_bool("USE_N8N_DISPATCH", False)
N8N_DISPATCH_WEBHOOK_URL = (
    os.environ.get("N8N_DISPATCH_WEBHOOK_URL")
    or "https://n8n-neurix-1.wbtech.dev/webhook/disparador-crm-start"
).strip()
# Secret do header X-CRM-API-Key que o n8n manda de volta (credencial "CRM API Key" no n8n).
N8N_DISPATCH_API_KEY = (os.environ.get("N8N_DISPATCH_API_KEY") or "").strip()
# URL pública deste app; vai em ``crm_base_url`` para o n8n saber onde fazer o callback.
PUBLIC_BASE_URL = (os.environ.get("PUBLIC_BASE_URL") or "").strip()

# --- Task 14 / observabilidade ops (Prometheus) ---
# ``EXPOSE_PROMETHEUS_METRICS``: expõe ``GET /metrics`` no processo Flask (default off).
EXPOSE_PROMETHEUS_METRICS = _env_bool("EXPOSE_PROMETHEUS_METRICS", False)
# Worker ``worker_cadence``: servidor HTTP Prometheus nesta porta (0 = desligado).
# O scrape deve apontar para o **mesmo** processo que incrementa
# ``campaign_outbox_send_attempts_total`` (normalmente o worker, não o Gunicorn web).
UAZAPI_OUTBOX_METRICS_PORT = int(
    (os.environ.get("UAZAPI_OUTBOX_METRICS_PORT") or "0").strip() or "0"
)
# Limiares sugestivos para acordo com ops (documentação + métrica Info em ``utils/outbox_prometheus``).
# Exemplo de alerta (taxa de não-sucesso em 5m, com volume mínimo):
#   sum(rate(campaign_outbox_send_attempts_total{outcome!="sent"}[5m]))
#     / clamp_min(sum(rate(campaign_outbox_send_attempts_total[5m])), 0.001)
#   > OUTBOX_ALERT_FAILURE_RATE_THRESHOLD
# e ``sum(rate(...[5m])) >= OUTBOX_ALERT_MIN_ATTEMPTS_PER_SEC`` para evitar ruído.
OUTBOX_ALERT_FAILURE_RATE_THRESHOLD = float(
    os.environ.get("OUTBOX_ALERT_FAILURE_RATE_THRESHOLD", "0.15")
)
OUTBOX_ALERT_MIN_ATTEMPTS_PER_SEC = float(
    os.environ.get("OUTBOX_ALERT_MIN_ATTEMPTS_PER_SEC", "0.01")
)
