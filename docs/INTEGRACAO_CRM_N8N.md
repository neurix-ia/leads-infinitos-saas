# Leads Infinitos × CRM Neurix: reaproveitar o disparador n8n

> 2026-10-12. Lido de `11 CRM Neurix` (backend FastAPI + Supabase) e do workflow n8n `Disparador CRM Simple (multi-tenant)` (id `pL8e4ydG1TvjkatU`, instância n8n-neurix-1).

## 1. O que o CRM tem hoje ("Comunicados")

| Peça | Onde | O que faz |
|---|---|---|
| Tabelas | `dispatch_lists`, `dispatch_members`, `dispatch_campaigns`, `dispatch_targets` (Supabase, migrations 019/023/025/026/029) | lista por organização com inbox, mensagem, delay 180–540 s, janela (weekdays + window_start/end), daily_limit; membros por telefone E.164; campanha = 1 mensagem × N targets |
| Backend | `routers/dispatch.py` (JWT do tenant), `routers/n8n_tools.py` (API key), `services/dispatch_service.py` | importar CSV, checar WhatsApp, criar campanha, cancelar; calcular `wait_seconds` por janela/cota |
| Frontend | `app/(dashboard)/disparador/[listId]/page.tsx` (1.707 l.) | abas de lista, membros, mensagem, confirmar disparo, progresso, campanhas recentes |
| n8n | workflow de 12 nós | Webhook Start → Register Execution → loop: Get Targets (limit 1) → Gate → Send UAZAPI `/send/text` → Update Status → Anti-ban (wait aleatório) → volta |

Contrato CRM ↔ n8n (4 endpoints, header `X-CRM-API-Key`):

1. `POST /webhook/disparador-crm-start` (CRM → n8n): `{campaign_id, instance_token, message, min_delay, max_delay, campaign_status, schedule_enabled, weekdays, window_start, window_end, wait_seconds, wait_reason, resume_at, daily_limit, daily_remaining, quota_exhausted, crm_base_url}`
2. `POST /api/n8n/tools/dispatch-execution` (n8n → CRM): grava `execution_id` para permitir cancelar
3. `GET /api/n8n/tools/dispatch-targets?campaign_id&status=pending&limit=1` (n8n → CRM): devolve próximo target **ou** `wait_seconds` (janela/cota) **ou** `cancelled`
4. `POST /api/n8n/tools/dispatch-target-status` `{target_id, status: sent|failed, error}` e `POST /api/n8n/tools/dispatch-complete`

Toda a inteligência de janela, cota diária e cancelamento fica no backend; o n8n só pergunta "posso mandar o próximo?" e obedece. É exatamente o que o motor do Leads Infinitos tenta fazer com 8.000 linhas.

Limites do disparador do CRM: 1 mensagem por campanha (sem etapas), só texto (sem mídia), só `{{nome}}` (sem variações/spintax), 1 instância por lista (sem round-robin), URLs do CRM e a API key **hardcoded nos nós** (o campo `crm_base_url` do payload existe mas o workflow não usa).

## 2. Comparação com o Leads Infinitos

| | Leads Infinitos | CRM Neurix |
|---|---|---|
| Público | clientes externos (Hubla, trial, planos) | clientes Neurix com conta no CRM (orgs, Vendi, catálogo) |
| Auth / banco | Flask-Login + Postgres próprio | Supabase Auth + Supabase Postgres, RBAC por org |
| Extração Google Maps | sim (Apify) | não |
| Campanha | 4 etapas, variações, mídia, round-robin, pacotes 5–30/dia | 1 mensagem, texto, 1 inbox, daily_limit livre |
| Follow-up / cadência | sim (delay_days, rollover, Chatwoot) | não |
| Kanban de leads | 6 colunas por campanha | kanban de funil do CRM (outro conceito: leads de atendimento, não de disparo) |
| Quem envia | worker Python próprio (3 gerações) | n8n |
| Janela / cota | 5 regras sobrepostas | 1 função (`dispatch_resume_state`) |

Mesclar os dois produtos não compensa: auth, banco, modelo de tenant e público são diferentes. O que compensa é o **contrato** do disparador.

## 3. Recomendação: LI vira cliente do mesmo workflow n8n

Manter os dois apps separados. No Leads Infinitos, substituir o M6 (motor de envio) por quatro endpoints que imitam o contrato do CRM, e disparar o mesmo workflow n8n. O que muda em cada lado:

**Leads Infinitos (novo, ~400 linhas no lugar de ~8.000):**
- Tabela `campaign_sends` (lead_id, step, instance_id, status pending/sent/failed, sent_at, error). Substitui `campaign_stage_sends`, `campaign_message_outbox`, `campaign_send_attempts`.
- `POST /api/n8n/tools/dispatch-targets`, `dispatch-execution`, `dispatch-target-status`, `dispatch-complete`, protegidos por `X-CRM-API-Key` (um secret próprio do LI).
- `dispatch-targets` aplica as regras do LI: janela da campanha (`send_hour_start/end`, sábado/domingo), `daily_limit` da campanha, round-robin de instância (devolve `instance_token` por target), variação de mensagem (sorteia e devolve o texto já renderizado em `message`).
- "Gerar etapa" no kanban = criar os `campaign_sends` da etapa e chamar o webhook. Cadência = um scheduler pequeno que, N dias depois, cria os sends da próxima etapa para quem não respondeu (resposta chega pelo webhook Uazapi, que o CRM já sabe tratar em `routers/webhooks.py`).

**n8n (uma mudança de 3 nós):**
- Trocar as 4 URLs fixas `https://crm.wbtech.dev/...` por `{{ $('Webhook Start').first().json.body.crm_base_url }}/api/n8n/tools/...`. O campo já vem no payload do CRM; o LI manda o seu.
- Tirar a API key dos headers hardcoded e deixar só na credencial `CRM API Key`; para o LI, ou segunda credencial, ou um `api_key` no payload. Hoje o secret aparece em texto nos nós do workflow e vale rotacionar.
- `Expand Targets` já aceita `instance_token` e `message` vindos por target (`data.instance_token || body.instance_token`), então round-robin e variação funcionam sem mexer no nó de envio. Mídia exige um ramo novo (`/send/media`) se quiser manter a feature do LI.

Resultado: um único disparador em produção (o que já roda para o CRM), dois front-ends. Bugs de envio passam a ser corrigidos em um lugar.

## 4. Alternativas descartadas

- **Migrar o LI para dentro do CRM** (orgs do CRM como tenants, extração como módulo): tecnicamente limpo, mas exige reescrever auth/licença/Hubla no Supabase e mistura clientes externos do LI com clientes de atendimento. Só faz sentido se o LI deixar de ser produto vendido separado.
- **Copiar o disparador do CRM para o LI em Python**: recria o problema atual (dois motores para manter).

## 5. Ordem sugerida

1. Agora: Apify novo (`main.py:191`), sem tocar em envio.
2. Rotacionar a API key do CRM e parametrizar `crm_base_url` no workflow (ganho imediato para o CRM também).
3. Prototipar os 4 endpoints no LI atrás de flag, testar com uma campanha de 1 etapa.
4. Cadência (etapas 2–4) via scheduler + webhook Uazapi.
5. Remover M6 legado, tabelas e ~40 flags.
