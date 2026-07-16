# Outbox daily batch prime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Campanhas Uazapi outbox enviam `daily_limit` leads todo dia útil (seg–sex na janela) sem force chunk.

**Architecture:** Quando a cota diária esgota e não há outbox in-flight, pré-enfileirar o próximo lote com `next_run_at` na próxima janela BRT. Rede de segurança: se cota livre + elegíveis + zero pending, enfileirar imediatamente (mesmo dia se dentro do horário).

**Tech Stack:** Python, `worker_message_outbox.py`, pytest

## Global Constraints

- Manter `send_saturday`/`send_sunday` da campanha (padrão: sem fim de semana).
- Não bypassar cota diária no mesmo dia.
- Pré-enfileirar no máximo 1 lote (`daily_limit`) por vez quando cota esgotada.

---

## Task 1: Pré-enfileirar próximo lote ao esgotar cota

**Files:**
- Modify: `worker_message_outbox.py`
- Test: `tests/test_outbox_daily_batch_prime.py`

- [ ] Test: quota esgotada + 0 in-flight + N elegíveis → N (cap daily_limit) pending com `next_run_at` futuro
- [ ] Test: quota esgotada + já há pending futuro → não duplica lote
- [ ] Test: quota livre + na janela → `next_run_at` ~ agora (mesmo dia)
- [ ] Implementar em `schedule_next_initial_outbox_batch`
- [ ] Reset `sent_today` quando `sent_campaign_today == 0` e contador stale
