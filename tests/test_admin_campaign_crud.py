#!/usr/bin/env python3
"""
Integration tests for admin campaign CRUD (Sprint 4).
Tests the full flow: admin creates campaign for another user via POST /api/admin/campaigns.
Mocks UazapiService to avoid real API calls.
"""

import os
import sys
import json
import tempfile
import importlib
from contextlib import contextmanager

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _REPO_ROOT)
sys.path.insert(0, _TESTS_DIR)

import campaign_test_data as ctd

from unittest.mock import patch, MagicMock
import pytest


def test_task9_sanitize_dispatch_audit_payload_redacts_secrets():
    """Task 9 (spec): tokens e chaves sensíveis não permanecem legíveis no payload de auditoria."""
    from utils.campaign_dispatch_audit import sanitize_dispatch_audit_payload

    secret = "tok-abcd-1234"
    payload = {
        "request": {
            "kind": "text",
            "apiKey": secret,
            "nested": {"refresh_token": "rt-secret"},
        },
        "Authorization": f"Bearer {secret}",
    }
    out = sanitize_dispatch_audit_payload(payload)
    assert secret not in json.dumps(out)
    assert out["Authorization"] == "[REDACTED]"
    assert out["request"]["apiKey"] == "[REDACTED]"
    assert out["request"]["nested"]["refresh_token"] == "[REDACTED]"


def _reload_outbox_modules(monkeypatch, enabled: bool):
    """Alinha ``USE_MESSAGE_OUTBOX`` em utils.config, app e worker_message_outbox (import-time)."""
    if enabled:
        monkeypatch.setenv("USE_MESSAGE_OUTBOX", "1")
    else:
        monkeypatch.delenv("USE_MESSAGE_OUTBOX", raising=False)
    import utils.config as cfg
    importlib.reload(cfg)
    import app as app_mod
    importlib.reload(app_mod)
    import worker_message_outbox as wmo
    importlib.reload(wmo)
    return app_mod, wmo


def _env_outbox_enabled(val):
    if val is None:
        return False
    return str(val).strip().lower() in ("1", "true", "yes", "on")


@contextmanager
def use_message_outbox_env(monkeypatch):
    """Feature flag on para testes outbox; restaura estado ao sair (Task 10 tech-spec)."""
    _reload_outbox_modules(monkeypatch, True)
    import app as app_mod
    yield app_mod
    _reload_outbox_modules(monkeypatch, False)


@pytest.fixture
def db_conn():
    from app import get_db_connection
    conn = get_db_connection()
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture
def ensure_admin_user(db_conn):
    """Ensure the superadmin user exists and return its id."""
    from psycopg2.extras import RealDictCursor
    email = 'augustogumi@gmail.com'
    with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id FROM users WHERE email = %s", (email,))
        row = cur.fetchone()
        if row:
            return row['id']
        cur.execute(
            "INSERT INTO users (email, password_hash, is_admin) VALUES (%s, %s, true) RETURNING id",
            (email, 'fakehash')
        )
        uid = cur.fetchone()['id']
        db_conn.commit()
        return uid


@pytest.fixture
def ensure_target_user(db_conn):
    """Create a regular target user and return its id."""
    from psycopg2.extras import RealDictCursor
    email = 'target_user_test@example.com'
    with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id FROM users WHERE email = %s", (email,))
        row = cur.fetchone()
        if row:
            return row['id']
        cur.execute(
            "INSERT INTO users (email, password_hash, is_admin) VALUES (%s, %s, false) RETURNING id",
            (email, 'fakehash')
        )
        uid = cur.fetchone()['id']
        db_conn.commit()
        return uid


@pytest.fixture
def ensure_instance(db_conn, ensure_target_user):
    """Primeira instância Uazapi ``connected`` do utilizador-alvo; cria se não existir."""
    return ctd.first_connected_uazapi_instance_id(db_conn, ensure_target_user)


@pytest.fixture
def ensure_scraping_job(db_conn, ensure_target_user):
    """Scraping job com CSV de leads válido (``campaign_test_data``)."""
    from psycopg2.extras import RealDictCursor
    user_id = ensure_target_user

    tmp = tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".csv",
        delete=False,
        encoding="utf-8",
        newline="",
    )
    tmp.write(ctd.SAMPLE_LEADS_CSV)
    tmp.close()

    with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """INSERT INTO scraping_jobs (user_id, keyword, locations, total_results, status, results_path, created_at)
               VALUES (%s, 'test-keyword', 'SP', %s, 'completed', %s, NOW())
               RETURNING id""",
            (user_id, ctd.SAMPLE_LEADS_ROW_COUNT, tmp.name),
        )
        job_id = cur.fetchone()['id']
        db_conn.commit()

    yield job_id
    os.unlink(tmp.name)


@pytest.fixture
def ensure_admin_instance(db_conn, ensure_admin_user):
    """Instância Uazapi ``connected`` do superadmin (dono da campanha em ``POST /api/campaigns``)."""
    return ctd.first_connected_uazapi_instance_id(db_conn, ensure_admin_user)


@pytest.fixture
def ensure_admin_scraping_job(db_conn, ensure_admin_user):
    """Job com CSV de leads pertencente ao superadmin (mesmo utilizador que ``POST /api/campaigns``)."""
    from psycopg2.extras import RealDictCursor
    user_id = ensure_admin_user

    tmp = tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".csv",
        delete=False,
        encoding="utf-8",
        newline="",
    )
    tmp.write(ctd.SAMPLE_LEADS_CSV)
    tmp.close()

    with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """INSERT INTO scraping_jobs (user_id, keyword, locations, total_results, status, results_path, created_at)
               VALUES (%s, 'test-keyword', 'SP', %s, 'completed', %s, NOW())
               RETURNING id""",
            (user_id, ctd.SAMPLE_LEADS_ROW_COUNT, tmp.name),
        )
        job_id = cur.fetchone()['id']
        db_conn.commit()

    yield job_id
    os.unlink(tmp.name)


def test_admin_create_campaign(
    db_conn, ensure_admin_user, ensure_target_user,
    ensure_instance, ensure_scraping_job
):
    """Superadmin cria campanha para outro usuário via POST /api/admin/campaigns."""
    from psycopg2.extras import RealDictCursor

    admin_id = ensure_admin_user
    target_user_id = ensure_target_user
    instance_id = ensure_instance
    job_id = ensure_scraping_job

    with patch('services.uazapi.UazapiService.create_advanced_campaign') as mock_uazapi:
        mock_uazapi.return_value = {
            'folder_id': 'test_folder_123',
            'status': 'queued',
            'count': 5
        }

        from app import app
        app.config['TESTING'] = True
        app.config['LOGIN_DISABLED'] = True

        with app.test_client() as client:
            with client.session_transaction() as sess:
                sess['_user_id'] = str(admin_id)

            payload = {
                'user_id': target_user_id,
                'name': 'Test Admin Campaign',
                'job_id': job_id,
                'message_templates': ['Olá {nome}, teste admin!'],
                'instance_ids': [instance_id],
                'use_uazapi_sender': True,
                'rotation_mode': 'single',
                'send_hour_start': ctd.DEFAULT_TEST_SEND_HOUR_START,
                'send_hour_end': ctd.DEFAULT_TEST_SEND_HOUR_END,
                'send_saturday': True,
                'send_sunday': True,
            }

            res = client.post(
                '/api/admin/campaigns',
                data=json.dumps(payload),
                content_type='application/json'
            )

            assert res.status_code == 200 or res.status_code == 201, \
                f"Expected 2xx, got {res.status_code}: {res.get_data(as_text=True)}"

            data = json.loads(res.get_data(as_text=True))
            campaign_id = data.get('campaign_id') or data.get('id')
            assert campaign_id, f"Response missing campaign_id: {data}"

    with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM campaigns WHERE id = %s", (campaign_id,))
        campaign = cur.fetchone()
        assert campaign is not None, "Campaign not found in DB"
        assert campaign['user_id'] == target_user_id
        assert campaign.get('created_by_admin_id') == admin_id

        cur.execute(
            "SELECT COUNT(*) as cnt FROM campaign_instances WHERE campaign_id = %s",
            (campaign_id,)
        )
        assert cur.fetchone()['cnt'] >= 1, "No campaign_instances rows"

        cur.execute(
            "SELECT COUNT(*) as cnt FROM campaign_leads WHERE campaign_id = %s",
            (campaign_id,)
        )
        leads_count = cur.fetchone()['cnt']
        assert leads_count > 0, "No leads populated"

    print(f"✅ test_admin_create_campaign passed — campaign {campaign_id}, {leads_count} leads")


def test_outbox_state_returns_403_when_use_message_outbox_disabled(
    db_conn, ensure_admin_user, monkeypatch,
):
    """AC / §7: polling outbox exige ``USE_MESSAGE_OUTBOX`` no ambiente."""
    prev_flag = os.environ.get("USE_MESSAGE_OUTBOX")
    _reload_outbox_modules(monkeypatch, False)
    try:
        admin_id = ensure_admin_user
        import app as app_mod

        flask_app = app_mod.app
        flask_app.config["TESTING"] = True
        flask_app.config["LOGIN_DISABLED"] = True
        with flask_app.test_client() as client:
            with client.session_transaction() as sess:
                sess["_user_id"] = str(admin_id)
            res = client.get("/api/admin/campaigns/1/outbox-state")
        assert res.status_code == 403
        body = res.get_json(silent=True) or {}
        assert "USE_MESSAGE_OUTBOX" in (body.get("message") or "")
    finally:
        _reload_outbox_modules(monkeypatch, _env_outbox_enabled(prev_flag))


def test_superadmin_outbox_create_skips_advanced_api_and_enqueues_outbox(
    db_conn, ensure_admin_user, ensure_target_user,
    ensure_instance, ensure_scraping_job, monkeypatch,
):
    """
    Flag on + superadmin: não chama ``create_advanced_campaign``; persiste ``campaign_message_outbox``.
    """
    from psycopg2.extras import RealDictCursor

    admin_id = ensure_admin_user
    target_user_id = ensure_target_user
    instance_id = ensure_instance
    job_id = ensure_scraping_job

    with use_message_outbox_env(monkeypatch):
        with patch('services.uazapi.UazapiService.create_advanced_campaign') as mock_adv:
            with patch('utils.limits.can_create_campaign_today', return_value=True):
                app_mod = __import__('app', fromlist=['app'])
                flask_app = app_mod.app
                flask_app.config['TESTING'] = True
                flask_app.config['LOGIN_DISABLED'] = True

                with flask_app.test_client() as client:
                    with client.session_transaction() as sess:
                        sess['_user_id'] = str(admin_id)

                    payload = {
                        'user_id': target_user_id,
                        'name': 'Outbox Test Campaign',
                        'job_id': job_id,
                        'message_templates': ['Olá {nome}, outbox!'],
                        'instance_ids': [instance_id],
                        'use_uazapi_sender': True,
                        'rotation_mode': 'single',
                        'send_hour_start': ctd.DEFAULT_TEST_SEND_HOUR_START,
                        'send_hour_end': ctd.DEFAULT_TEST_SEND_HOUR_END,
                        'send_saturday': True,
                        'send_sunday': True,
                    }
                    res = client.post(
                        '/api/admin/campaigns',
                        data=json.dumps(payload),
                        content_type='application/json',
                    )

                assert res.status_code in (200, 201), res.get_data(as_text=True)
                mock_adv.assert_not_called()

                data = json.loads(res.get_data(as_text=True))
                campaign_id = data.get('campaign_id') or data.get('id')
                assert campaign_id

                with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        "SELECT COUNT(*) AS n FROM campaign_message_outbox WHERE campaign_id = %s AND status = 'pending'",
                        (campaign_id,),
                    )
                    assert cur.fetchone()['n'] >= 1


def test_superadmin_post_api_campaigns_new_outbox_skips_advanced_and_enqueues(
    db_conn,
    ensure_admin_user,
    ensure_admin_instance,
    ensure_admin_scraping_job,
    monkeypatch,
):
    """
    Superadmin em ``POST /api/campaigns`` com ``USE_MESSAGE_OUTBOX`` não chama
    ``create_advanced_campaign`` e enfileira ``campaign_message_outbox``.
    """
    from psycopg2.extras import RealDictCursor

    admin_id = ensure_admin_user
    instance_id = ensure_admin_instance
    job_id = ensure_admin_scraping_job

    with use_message_outbox_env(monkeypatch):
        with patch('services.uazapi.UazapiService.create_advanced_campaign') as mock_adv:
            with patch('utils.limits.can_create_campaign_today', return_value=True):
                app_mod = __import__('app', fromlist=['app'])
                flask_app = app_mod.app
                flask_app.config['TESTING'] = True
                flask_app.config['LOGIN_DISABLED'] = True

                with flask_app.test_client() as client:
                    with client.session_transaction() as sess:
                        sess['_user_id'] = str(admin_id)

                    payload = {
                        'name': 'Outbox User Route Campaign',
                        'job_id': job_id,
                        'message_templates': ['Olá {nome}, rota /api/campaigns!'],
                        'instance_ids': [instance_id],
                        'use_uazapi_sender': True,
                        'rotation_mode': 'single',
                        'send_hour_start': ctd.DEFAULT_TEST_SEND_HOUR_START,
                        'send_hour_end': ctd.DEFAULT_TEST_SEND_HOUR_END,
                        'send_saturday': True,
                        'send_sunday': True,
                    }
                    res = client.post(
                        '/api/campaigns',
                        data=json.dumps(payload),
                        content_type='application/json',
                    )

                assert res.status_code in (200, 201), res.get_data(as_text=True)
                mock_adv.assert_not_called()

                data = json.loads(res.get_data(as_text=True))
                campaign_id = data.get('campaign_id') or data.get('id')
                assert campaign_id

                with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        "SELECT COUNT(*) AS n FROM campaign_message_outbox WHERE campaign_id = %s AND status = 'pending'",
                        (campaign_id,),
                    )
                    assert cur.fetchone()['n'] >= 1


def test_regular_user_post_api_campaigns_outbox_skips_advanced_and_enqueues(
    db_conn,
    ensure_target_user,
    ensure_instance,
    ensure_scraping_job,
    monkeypatch,
):
    """
    Utilizador normal (fora de SUPER_ADMIN_EMAILS): ``POST /api/campaigns`` com
    ``USE_MESSAGE_OUTBOX`` não chama ``create_advanced_campaign`` e enfileira outbox.
    """
    from psycopg2.extras import RealDictCursor

    user_id = ensure_target_user
    instance_id = ensure_instance
    job_id = ensure_scraping_job

    with use_message_outbox_env(monkeypatch):
        with patch('services.uazapi.UazapiService.create_advanced_campaign') as mock_adv:
            with patch('utils.limits.can_create_campaign_today', return_value=True):
                app_mod = __import__('app', fromlist=['app'])
                flask_app = app_mod.app
                flask_app.config['TESTING'] = True
                flask_app.config['LOGIN_DISABLED'] = True

                with flask_app.test_client() as client:
                    with client.session_transaction() as sess:
                        sess['_user_id'] = str(user_id)

                    payload = {
                        'name': 'Outbox Regular User Campaign',
                        'job_id': job_id,
                        'message_templates': ['Olá {nome}, utilizador normal!'],
                        'instance_ids': [instance_id],
                        'use_uazapi_sender': True,
                        'rotation_mode': 'single',
                        'send_hour_start': ctd.DEFAULT_TEST_SEND_HOUR_START,
                        'send_hour_end': ctd.DEFAULT_TEST_SEND_HOUR_END,
                        'send_saturday': True,
                        'send_sunday': True,
                    }
                    res = client.post(
                        '/api/campaigns',
                        data=json.dumps(payload),
                        content_type='application/json',
                    )

                assert res.status_code in (200, 201), res.get_data(as_text=True)
                mock_adv.assert_not_called()

                data = json.loads(res.get_data(as_text=True))
                campaign_id = data.get('campaign_id') or data.get('id')
                assert campaign_id

                with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        "SELECT COUNT(*) AS n FROM campaign_message_outbox WHERE campaign_id = %s AND status = 'pending'",
                        (campaign_id,),
                    )
                    assert cur.fetchone()['n'] >= 1


def test_outbox_tick_writes_attempt_and_marks_sent(
    db_conn, ensure_admin_user, ensure_target_user,
    ensure_instance, ensure_scraping_job, monkeypatch,
):
    """Worker: com mock 200, após ticks sucessivos (fila global, 1 envio/tick) esta campanha fica com linha ``sent`` e tentativa."""
    from psycopg2.extras import RealDictCursor

    admin_id = ensure_admin_user
    target_user_id = ensure_target_user
    instance_id = ensure_instance
    job_id = ensure_scraping_job

    with use_message_outbox_env(monkeypatch):
        with patch('utils.limits.can_create_campaign_today', return_value=True):
            with patch('utils.limits.check_initial_chunk_daily_quota_for_campaign', return_value=True):
                app_mod = __import__('app', fromlist=['app'])
                flask_app = app_mod.app
                flask_app.config['TESTING'] = True
                flask_app.config['LOGIN_DISABLED'] = True

                with flask_app.test_client() as client:
                    with client.session_transaction() as sess:
                        sess['_user_id'] = str(admin_id)
                    payload = {
                        'user_id': target_user_id,
                        'name': 'Outbox Tick Campaign',
                        'job_id': job_id,
                        'message_templates': ['Ping {nome}'],
                        'instance_ids': [instance_id],
                        'use_uazapi_sender': True,
                        'rotation_mode': 'single',
                        'send_hour_start': ctd.DEFAULT_TEST_SEND_HOUR_START,
                        'send_hour_end': ctd.DEFAULT_TEST_SEND_HOUR_END,
                        'send_saturday': True,
                        'send_sunday': True,
                    }
                    res = client.post(
                        '/api/admin/campaigns',
                        data=json.dumps(payload),
                        content_type='application/json',
                    )
                assert res.status_code in (200, 201), res.get_data(as_text=True)
                data = json.loads(res.get_data(as_text=True))
                campaign_id = data.get('campaign_id') or data.get('id')
                assert campaign_id

                import worker_message_outbox as wmo

                row = None
                for _ in range(200):
                    with patch.object(wmo, 'is_campaign_send_window', return_value=True):
                        with patch.object(
                            wmo.uazapi_service,
                            'send_text_idempotent',
                            return_value={'messageId': 'x'},
                        ):
                            wmo.process_message_outbox_tick(db_conn)
                    with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute(
                            """
                            SELECT o.id, o.status FROM campaign_message_outbox o
                            WHERE o.campaign_id = %s AND o.status = 'sent' LIMIT 1
                            """,
                            (campaign_id,),
                        )
                        row = cur.fetchone()
                    if row:
                        break
                assert row and row['status'] == 'sent', (
                    "Nenhuma linha desta campanha passou a sent após 200 ticks; "
                    "fila global ou janela/throttle podem estar a bloquear de forma persistente."
                )

                with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        "SELECT outcome FROM campaign_send_attempts WHERE outbox_id = %s ORDER BY id DESC LIMIT 1",
                        (row['id'],),
                    )
                    att = cur.fetchone()
                    assert att and att['outcome'] == 'sent'


def test_admin_new_campaign_page_loads(db_conn, ensure_admin_user):
    """GET /admin/campaigns/new retorna 200."""
    admin_id = ensure_admin_user

    from app import app
    app.config['TESTING'] = True
    app.config['LOGIN_DISABLED'] = True

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['_user_id'] = str(admin_id)

        res = client.get('/admin/campaigns/new')
        assert res.status_code == 200, f"Expected 200, got {res.status_code}"
        html = res.get_data(as_text=True)
        assert 'Nova Campanha (Admin)' in html

    print("✅ test_admin_new_campaign_page_loads passed")


def test_admin_edit_campaign_page_loads(
    db_conn, ensure_admin_user, ensure_target_user,
    ensure_instance, ensure_scraping_job
):
    """GET /admin/campaigns/<id>/edit retorna 200 com dados preenchidos."""
    from psycopg2.extras import RealDictCursor

    admin_id = ensure_admin_user
    target_user_id = ensure_target_user
    instance_id = ensure_instance

    with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """INSERT INTO campaigns (user_id, name, message_template, status, created_at)
               VALUES (%s, 'Edit Test Campaign', '["Hello"]', 'pending', NOW())
               RETURNING id""",
            (target_user_id,)
        )
        campaign_id = cur.fetchone()['id']
        cur.execute(
            "INSERT INTO campaign_instances (campaign_id, instance_id) VALUES (%s, %s)",
            (campaign_id, instance_id)
        )
        db_conn.commit()

    from app import app
    app.config['TESTING'] = True
    app.config['LOGIN_DISABLED'] = True

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess['_user_id'] = str(admin_id)

        res = client.get(f'/admin/campaigns/{campaign_id}/edit')
        assert res.status_code == 200, f"Expected 200, got {res.status_code}"
        html = res.get_data(as_text=True)
        assert 'Edit Test Campaign' in html
        assert 'Editar Campanha' in html

    print("✅ test_admin_edit_campaign_page_loads passed")


def test_admin_link_unlink_campaign_instance(
    db_conn, ensure_admin_user, ensure_target_user, ensure_instance
):
    """Admin pode vincular/desvincular instância Uazapi em campaign_instances."""
    from psycopg2.extras import RealDictCursor
    import secrets

    admin_id = ensure_admin_user
    target_user_id = ensure_target_user
    instance_id = ensure_instance

    with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """INSERT INTO campaigns (user_id, name, message_template, status, created_at)
               VALUES (%s, 'Link Instance Test', '["Hi"]', 'running', NOW())
               RETURNING id""",
            (target_user_id,),
        )
        campaign_id = cur.fetchone()["id"]
        db_conn.commit()

    from app import app

    app.config["TESTING"] = True
    app.config["LOGIN_DISABLED"] = True
    csrf = secrets.token_urlsafe(16)

    with app.test_client() as client:
        with client.session_transaction() as sess:
            sess["_user_id"] = str(admin_id)
            sess["csrf_token"] = csrf

        detail = client.get(f"/api/admin/campaigns/{campaign_id}/detail")
        assert detail.status_code == 200, detail.get_data(as_text=True)
        d = detail.get_json()
        assert d["instances"] == []
        assert any(int(i["id"]) == int(instance_id) for i in d["available_instances"])

        link = client.post(
            f"/api/admin/campaigns/{campaign_id}/instances",
            data=json.dumps({"instance_id": instance_id}),
            content_type="application/json",
            headers={"X-CSRF-Token": csrf},
        )
        assert link.status_code == 200, link.get_data(as_text=True)
        linked = link.get_json()
        assert linked["success"] is True
        assert any(int(i["id"]) == int(instance_id) for i in linked["instances"])

        with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT COUNT(*) AS n FROM campaign_instances WHERE campaign_id = %s AND instance_id = %s",
                (campaign_id, instance_id),
            )
            assert cur.fetchone()["n"] == 1

        unlink = client.delete(
            f"/api/admin/campaigns/{campaign_id}/instances/{instance_id}",
            data=json.dumps({}),
            content_type="application/json",
            headers={"X-CSRF-Token": csrf},
        )
        assert unlink.status_code == 200, unlink.get_data(as_text=True)
        assert unlink.get_json()["instances"] == []

        with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT COUNT(*) AS n FROM campaign_instances WHERE campaign_id = %s",
                (campaign_id,),
            )
            assert cur.fetchone()["n"] == 0

    print(f"✅ test_admin_link_unlink_campaign_instance passed — campaign {campaign_id}")


if __name__ == '__main__':
    test_admin_new_campaign_page_loads(None, None)
    print("Run with pytest for full integration tests.")
