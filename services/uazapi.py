"""
UazapiService - Integração com Uazapi para WhatsApp.

Usado pelo superadmin para criar instâncias, conectar, verificar status,
deletar e enviar mensagens. URL base via UAZAPI_URL; admintoken via UAZAPI_ADMIN_TOKEN.
"""

import base64
import json
import os
import time
from typing import Any, Optional, Tuple

import requests


def _parse_timeout_seconds(
    env_name: str, default: int, *, minimum: int = 5, maximum: int = 600
) -> int:
    """Lê timeout em segundos a partir de env; valores inválidos usam default."""
    raw = (os.environ.get(env_name) or "").strip()
    if not raw:
        return default
    try:
        v = int(raw, 10)
    except ValueError:
        return default
    return max(minimum, min(maximum, v))


def _parse_error_body_from_response(response: requests.Response) -> Any:
    """Extrai corpo de erro JSON ou texto truncado de uma resposta HTTP."""
    try:
        return response.json()
    except Exception:
        return {"raw": (getattr(response, "text", None) or "")[:8000]}


def _uazapi_request_failure(
    *,
    http_status: Optional[int] = None,
    error_body: Any = None,
    exception: Optional[BaseException] = None,
    reason: Optional[str] = None,
) -> dict[str, Any]:
    """
    Dict de falha estruturada — paridade com ``create_advanced_campaign`` e
    ``classify_outbox_send_failure`` no worker outbox.
    """
    payload: dict[str, Any] = {"uazapi_request_failed": True}
    if http_status is not None:
        payload["http_status"] = http_status
    if error_body is not None:
        payload["error_body"] = error_body
    if exception is not None:
        payload["exception"] = type(exception).__name__
        payload["exception_message"] = str(exception)[:2000]
    if reason:
        payload["reason"] = reason
    return payload


def _resolve_media_file_value(file: str) -> Optional[str]:
    """
    Converte path local em data URL base64; mantém URL/http/data: como estão.
    Retorna None se path local não existir.
    """
    if file.startswith(("http://", "https://", "data:")):
        return file
    if os.path.isfile(file):
        with open(file, "rb") as f:
            data = f.read()
        b64 = base64.b64encode(data).decode("utf-8")
        ext = os.path.splitext(file)[1].lower()
        mime_map = {
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".png": "image/png",
            ".gif": "image/gif",
            ".mp4": "video/mp4",
            ".webm": "video/webm",
        }
        mime = mime_map.get(ext, "application/octet-stream")
        return f"data:{mime};base64,{b64}"
    print(f"❌ [Uazapi] send_media: arquivo não encontrado: {file}")
    return None

# Rate limit para log 401: por chave; intervalo via UAZAPI_401_LOG_INTERVAL_SEC (default 600s).
_401_log_last: dict[tuple, float] = {}


def _uazapi_401_log_interval_sec() -> float:
    raw = (os.environ.get("UAZAPI_401_LOG_INTERVAL_SEC") or "600").strip()
    try:
        return max(30.0, min(float(raw), 86400.0))
    except (TypeError, ValueError):
        return 600.0


def _uazapi_should_log_401(key: tuple) -> bool:
    now = time.monotonic()
    interval = _uazapi_401_log_interval_sec()
    if now - _401_log_last.get(key, 0) >= interval:
        _401_log_last[key] = now
        return True
    return False


class UazapiService:
    """Cliente para API Uazapi (WhatsApp)."""

    def __init__(self) -> None:
        self.base_url = os.environ.get(
            "UAZAPI_URL", "https://neurix.uazapi.com"
        ).rstrip("/")
        self.admin_token = os.environ.get("UAZAPI_ADMIN_TOKEN", "")

    def create_instance(self, name: str) -> Optional[dict[str, Any]]:
        """
        Cria nova instância via POST /instance/init.
        Usa header admintoken.
        Retorna dict com token (response.get('token') or response.get('instance', {}).get('token')).
        """
        url = f"{self.base_url}/instance/init"
        headers = {
            "admintoken": self.admin_token,
            "Content-Type": "application/json",
        }
        payload = {"name": name}

        try:
            response = requests.post(
                url, json=payload, headers=headers, timeout=15
            )
            if response.status_code != 200:
                print(f"❌ [Uazapi] create_instance Status: {response.status_code}")
                print(f"❌ [Uazapi] create_instance Body: {response.text}")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ [Uazapi] Error creating instance: {e}")
            if hasattr(e, "response") and e.response is not None:
                print(f"❌ [Uazapi] Response: {e.response.text}")
            return None

    def connect(self, token: str) -> Optional[dict[str, Any]]:
        """
        Inicia conexão via POST /instance/connect.
        Usa header token. Payload vazio para gerar QR code.
        Retorna instance com qrcode (base64) se em processo de conexão.
        """
        url = f"{self.base_url}/instance/connect"
        headers = {
            "token": token,
            "Content-Type": "application/json",
        }
        payload = {}

        try:
            response = requests.post(
                url, json=payload, headers=headers, timeout=15
            )
            if response.status_code != 200:
                print(f"❌ [Uazapi] connect Status: {response.status_code}")
                print(f"❌ [Uazapi] connect Body: {response.text}")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ [Uazapi] Error connecting: {e}")
            if hasattr(e, "response") and e.response is not None:
                print(f"❌ [Uazapi] Response: {e.response.text}")
            return None

    def get_status(self, token: str) -> Optional[dict[str, Any]]:
        """
        Obtém status via GET /instance/status.
        Retorna instance.status: connected, connecting ou disconnected.
        """
        url = f"{self.base_url}/instance/status"
        headers = {"token": token}

        def _maybe_log_get_status_401(_resp) -> None:
            # Uma linha no intervalo global: vários tokens/instâncias com apikey inválida geram
            # uma única mensagem (reduz ruído em cadence/health).
            if _uazapi_should_log_401(("uazapi_401", "get_status")):
                print(
                    "⚠️ [Uazapi] 401 Invalid token (get_status). Uma ou mais instâncias; "
                    "atualize apikeys (UAZAPI_401_LOG_INTERVAL_SEC / UAZAPI_DEBUG=1)."
                )

        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 401:
                body_l = (response.text or "").lower()
                if "invalid token" in body_l:
                    _maybe_log_get_status_401(response)
                    return None
            if response.status_code != 200:
                if os.environ.get("UAZAPI_DEBUG", "").strip().lower() in (
                    "1",
                    "true",
                    "yes",
                ):
                    print(
                        f"❌ [Uazapi] get_status Status: {response.status_code} "
                        f"Body: {(response.text or '')[:500]}"
                    )
                return None
            return response.json()
        except requests.exceptions.RequestException as e:
            if os.environ.get("UAZAPI_DEBUG", "").strip().lower() in (
                "1",
                "true",
                "yes",
            ):
                print(f"❌ [Uazapi] Error getting status: {e}")
            return None

    def delete_instance(self, token: str) -> Tuple[bool, Optional[int]]:
        """
        Deleta instância via DELETE /instance.
        Retorna (success, status_code).
        Trata como sucesso: 200, 404, ou erro cujo body indique instância já deletada.
        """
        url = f"{self.base_url}/instance"
        headers = {"token": token}

        try:
            response = requests.delete(url, headers=headers, timeout=15)
            if response.status_code in (200, 404):
                return True, response.status_code
            # Instância já deletada na Uazapi? Tratar como sucesso para remover do nosso DB
            body_lower = (response.text or "").lower()
            if any(
                x in body_lower
                for x in ("not found", "não encontrad", "deleted", "deletada", "404", "instance not found")
            ):
                return True, response.status_code
            print(f"❌ [Uazapi] delete_instance Status: {response.status_code}")
            print(f"❌ [Uazapi] delete_instance Body: {response.text}")
            return False, response.status_code
        except requests.exceptions.RequestException as e:
            print(f"❌ [Uazapi] Error deleting instance: {e}")
            if hasattr(e, "response") and e.response is not None:
                return False, e.response.status_code if e.response else None
            return False, None

    def send_text(
        self, token: str, number: str, text: str
    ) -> Optional[dict[str, Any]]:
        """
        Envia mensagem de texto via POST /send/text.
        number: formato 5511999999999 (sem @s.whatsapp.net).
        """
        url = f"{self.base_url}/send/text"
        headers = {
            "token": token,
            "Content-Type": "application/json",
        }
        payload = {"number": number, "text": text}

        try:
            response = requests.post(
                url, json=payload, headers=headers, timeout=15
            )
            if response.status_code != 200:
                print(f"❌ [Uazapi] send_text Status: {response.status_code}")
                print(f"❌ [Uazapi] send_text Body: {response.text}")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ [Uazapi] Error sending text: {e}")
            if hasattr(e, "response") and e.response is not None:
                print(f"❌ [Uazapi] Response: {e.response.text}")
            return None

    def send_text_idempotent(
        self,
        token: str,
        number: str,
        text: str,
        *,
        track_id: str,
        track_source: str,
        timeout_seconds: int = 15,
    ) -> Optional[dict[str, Any]]:
        """
        POST /send/text com track_id e track_source (idempotência / rastreio Uazapi).
        Campos alinhados ao OpenAPI (snake_case).
        """
        url = f"{self.base_url}/send/text"
        headers = {
            "token": token,
            "Content-Type": "application/json",
        }
        payload: dict[str, Any] = {
            "number": number,
            "text": text,
            "track_id": track_id,
            "track_source": track_source,
        }

        try:
            response = requests.post(
                url, json=payload, headers=headers, timeout=timeout_seconds
            )
            if response.status_code != 200:
                err_body = _parse_error_body_from_response(response)
                print(f"❌ [Uazapi] send_text_idempotent Status: {response.status_code}")
                print(f"❌ [Uazapi] send_text_idempotent Body: {response.text}")
                return _uazapi_request_failure(
                    http_status=response.status_code,
                    error_body=err_body,
                )
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ [Uazapi] Error send_text_idempotent: {e}")
            status = None
            err_body: Any = None
            if hasattr(e, "response") and e.response is not None:
                print(f"❌ [Uazapi] Response: {e.response.text}")
                try:
                    status = e.response.status_code
                except Exception:
                    status = None
                try:
                    err_body = e.response.json()
                except Exception:
                    err_body = (getattr(e.response, "text", None) or str(e))[:4000]
            return _uazapi_request_failure(
                http_status=status,
                error_body=err_body,
                exception=e,
            )

    def send_media(
        self,
        token: str,
        number: str,
        media_type: str,
        file: str,
        caption: str = "",
    ) -> Optional[dict[str, Any]]:
        """
        Envia mídia (imagem ou vídeo) via POST /send/media.
        number: formato 5511999999999 (sem @s.whatsapp.net).
        media_type: 'image' ou 'video'.
        file: path local (será convertido para base64), URL ou string base64.
        caption: legenda opcional.
        """
        url = f"{self.base_url}/send/media"
        headers = {
            "token": token,
            "Content-Type": "application/json",
        }

        file_value = _resolve_media_file_value(file)
        if file_value is None:
            return _uazapi_request_failure(
                reason="media_file_not_found",
                error_body={"file": file, "message": "media file not found or unreadable"},
            )

        payload: dict[str, Any] = {
            "number": number,
            "type": media_type,
            "file": file_value,
            "text": caption or "",
        }

        try:
            response = requests.post(
                url, json=payload, headers=headers, timeout=30
            )
            if response.status_code != 200:
                err_body = _parse_error_body_from_response(response)
                print(f"❌ [Uazapi] send_media Status: {response.status_code}")
                print(f"❌ [Uazapi] send_media Body: {response.text}")
                return _uazapi_request_failure(
                    http_status=response.status_code,
                    error_body=err_body,
                )
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ [Uazapi] Error sending media: {e}")
            status = None
            err_body: Any = None
            if hasattr(e, "response") and e.response is not None:
                print(f"❌ [Uazapi] Response: {e.response.text}")
                try:
                    status = e.response.status_code
                except Exception:
                    status = None
                try:
                    err_body = e.response.json()
                except Exception:
                    err_body = (getattr(e.response, "text", None) or str(e))[:4000]
            return _uazapi_request_failure(
                http_status=status,
                error_body=err_body,
                exception=e,
            )

    def send_media_campaign(
        self,
        token: str,
        number: str,
        media_type: str,
        file: str,
        caption: str = "",
        *,
        track_id: str,
        track_source: str,
        timeout_seconds: Optional[int] = None,
    ) -> Optional[dict[str, Any]]:
        """
        POST /send/media com track_id e track_source (campanhas / outbox).
        Timeout: ``timeout_seconds`` se definido; senão ``UAZAPI_SEND_MEDIA_TIMEOUT_SECONDS`` (default 30).
        """
        url = f"{self.base_url}/send/media"
        headers = {
            "token": token,
            "Content-Type": "application/json",
        }

        file_value = _resolve_media_file_value(file)
        if file_value is None:
            return _uazapi_request_failure(
                reason="media_file_not_found",
                error_body={"file": file, "message": "media file not found or unreadable"},
            )

        timeout = timeout_seconds
        if timeout is None:
            timeout = _parse_timeout_seconds(
                "UAZAPI_SEND_MEDIA_TIMEOUT_SECONDS", default=30, minimum=5, maximum=600
            )

        payload: dict[str, Any] = {
            "number": number,
            "type": media_type,
            "file": file_value,
            "text": caption or "",
            "track_id": track_id,
            "track_source": track_source,
        }

        try:
            response = requests.post(
                url, json=payload, headers=headers, timeout=timeout
            )
            if response.status_code != 200:
                err_body = _parse_error_body_from_response(response)
                print(f"❌ [Uazapi] send_media_campaign Status: {response.status_code}")
                print(f"❌ [Uazapi] send_media_campaign Body: {response.text}")
                return _uazapi_request_failure(
                    http_status=response.status_code,
                    error_body=err_body,
                )
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ [Uazapi] Error send_media_campaign: {e}")
            status = None
            err_body: Any = None
            if hasattr(e, "response") and e.response is not None:
                print(f"❌ [Uazapi] Response: {e.response.text}")
                try:
                    status = e.response.status_code
                except Exception:
                    status = None
                try:
                    err_body = e.response.json()
                except Exception:
                    err_body = (getattr(e.response, "text", None) or str(e))[:4000]
            return _uazapi_request_failure(
                http_status=status,
                error_body=err_body,
                exception=e,
            )

    def check_phone(
        self, token: str, numbers: list[str], timeout: int = 15
    ) -> Optional[list[dict[str, Any]]]:
        """
        Verifica números via POST /chat/check.
        numbers: lista de números sem @s.whatsapp.net (ex: ["5511999999999"]).
        Retorna array com objetos contendo isInWhatsapp (camelCase).
        timeout: segundos (default 15; validate-leads usa 90 para batches grandes).
        """
        url = f"{self.base_url}/chat/check"
        headers = {
            "token": token,
            "Content-Type": "application/json",
        }
        payload = {"numbers": numbers}

        try:
            response = requests.post(
                url, json=payload, headers=headers, timeout=timeout
            )
            if response.status_code != 200:
                print(f"❌ [Uazapi] check_phone Status: {response.status_code}")
                print(f"❌ [Uazapi] check_phone Body: {response.text}")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError:
            raise  # Propagar 503/WhatsApp disconnected para caller trocar instância
        except requests.exceptions.RequestException as e:
            print(f"❌ [Uazapi] Error checking phone: {e}")
            if hasattr(e, "response") and e.response is not None:
                print(f"❌ [Uazapi] Response: {e.response.text}")
            return None

    # --- Métodos de campanha (envio em massa avançado) ---

    def create_advanced_campaign(
        self,
        token: str,
        delay_min_sec: int,
        delay_max_sec: int,
        messages: list[dict[str, Any]],
        info: Optional[str] = None,
        scheduled_for: Optional[int] = None,
    ) -> Optional[dict[str, Any]]:
        """
        Cria campanha de envio em massa via POST /sender/advanced.
        messages: array de {number, type, text} (number sem @s.whatsapp.net).
        Retorna dict com folder_id, count, status.
        """
        url = f"{self.base_url}/sender/advanced"
        headers = {
            "token": token,
            "Content-Type": "application/json",
        }
        payload: dict[str, Any] = {
            "delayMin": delay_min_sec,
            "delayMax": delay_max_sec,
            "messages": messages,
        }
        if info is not None:
            payload["info"] = info
        if scheduled_for is not None:
            payload["scheduled_for"] = scheduled_for

        try:
            response = requests.post(
                url, json=payload, headers=headers, timeout=30
            )
            if response.status_code != 200:
                err_body: Any = None
                try:
                    err_body = response.json()
                except Exception:
                    err_body = {"raw": (getattr(response, "text", None) or "")[:8000]}
                print(
                    f"❌ [Uazapi] create_advanced_campaign Status: {response.status_code}"
                )
                print(f"❌ [Uazapi] create_advanced_campaign Body: {response.text}")
                return {
                    "uazapi_request_failed": True,
                    "http_status": response.status_code,
                    "error_body": err_body,
                }
            data = response.json()
            if os.environ.get("UAZAPI_DEBUG", "").strip().lower() in ("1", "true", "yes"):
                try:
                    blob = json.dumps(data, ensure_ascii=False, default=str)[:4000]
                    print(f"🔎 [Uazapi DEBUG] create_advanced_campaign response: {blob}")
                except Exception:
                    print(f"🔎 [Uazapi DEBUG] create_advanced_campaign response: {data!r}")
            return data
        except requests.exceptions.RequestException as e:
            print(f"❌ [Uazapi] Error creating advanced campaign: {e}")
            if hasattr(e, "response") and e.response is not None:
                print(f"❌ [Uazapi] Response: {e.response.text}")
            status = None
            err_body: Any = None
            if hasattr(e, "response") and e.response is not None:
                try:
                    status = e.response.status_code
                except Exception:
                    status = None
                try:
                    err_body = e.response.json()
                except Exception:
                    err_body = (getattr(e.response, "text", None) or str(e))[:4000]
            return {
                "uazapi_request_failed": True,
                "http_status": status,
                "error_body": err_body,
                "exception": str(e)[:2000],
            }

    def edit_campaign(
        self, token: str, folder_id: str, action: str
    ) -> Optional[dict[str, Any]]:
        """
        Controla campanha via POST /sender/edit.
        action: "stop" | "continue" | "delete".
        Retorna dict com status.
        """
        if action not in ("stop", "continue", "delete"):
            print(f"❌ [Uazapi] edit_campaign invalid action: {action}")
            return None
        url = f"{self.base_url}/sender/edit"
        headers = {
            "token": token,
            "Content-Type": "application/json",
        }
        payload = {"folder_id": folder_id, "action": action}

        try:
            response = requests.post(
                url, json=payload, headers=headers, timeout=15
            )
            if response.status_code != 200:
                print(
                    f"❌ [Uazapi] edit_campaign Status: {response.status_code}"
                )
                print(f"❌ [Uazapi] edit_campaign Body: {response.text}")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ [Uazapi] Error editing campaign: {e}")
            if hasattr(e, "response") and e.response is not None:
                print(f"❌ [Uazapi] Response: {e.response.text}")
            return None

    def list_folders(
        self, token: str, status: Optional[str] = None, context: Optional[dict] = None
    ) -> Optional[list[dict[str, Any]]]:
        """
        Lista campanhas via GET /sender/listfolders.
        status: "Active" | "Archived" (opcional).
        context: dict opcional (campaign_id, instance_id) para logs de erro.
        Retorna lista de pastas em sucesso (HTTP 200, corpo JSON em array).

        Retorna None em falha de transporte/HTTP (timeout, rede, status ≠200, 401 token inválido,
        etc.). O sync não deve inferir pasta órfã quando este método devolve None (Task 7 / AC5).
        """
        url = f"{self.base_url}/sender/listfolders"
        headers = {"token": token}
        params: dict[str, str] = {}
        if status is not None:
            params["status"] = status
        ctx_str = f" {context}" if context else ""

        def _should_silent_return(resp) -> bool:
            if resp is None:
                return False
            if resp.status_code == 401:
                body = (getattr(resp, "text", None) or "").lower()
                if "invalid token" in body:
                    return True
            if resp.status_code == 400:
                body = (getattr(resp, "text", None) or "").lower()
                if "folder not found" in body or "access denied" in body:
                    return True
            return False

        def _maybe_log_401(resp, ctx: Optional[dict], endpoint: str) -> None:
            if resp is None or resp.status_code != 401:
                return
            inst_id = (ctx or {}).get("instance_id")
            log_key = (
                "uazapi_401",
                endpoint,
                inst_id if inst_id is not None else (ctx or {}).get("campaign_id"),
            )
            if not _uazapi_should_log_401(log_key):
                return
            msg = f"instance_id={inst_id}" if inst_id is not None else f"campaign_id={(ctx or {}).get('campaign_id')}" if (ctx or {}).get("campaign_id") is not None else "instância"
            print(f"⚠️ [Uazapi] {msg}: 401 Invalid token ({endpoint}). Atualize o apikey da instância.")

        try:
            response = requests.get(
                url, headers=headers, params=params or None, timeout=15
            )
            if _should_silent_return(response):
                _maybe_log_401(response, context, "list_folders")
                return None
            if response.status_code != 200:
                print(
                    f"❌ [Uazapi] list_folders Status: {response.status_code}{ctx_str}"
                )
                print(f"❌ [Uazapi] list_folders Body: {response.text}")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            resp = getattr(e, "response", None)
            if _should_silent_return(resp):
                _maybe_log_401(resp, context, "list_folders")
                return None
            print(f"❌ [Uazapi] Error listing folders: {e}{ctx_str}")
            if resp is not None:
                print(f"❌ [Uazapi] Response: {resp.text}")
            return None

    def list_messages(
        self,
        token: str,
        folder_id: str,
        message_status: Optional[str] = None,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
        context: Optional[dict] = None,
    ) -> Optional[dict[str, Any]]:
        """
        Lista mensagens de campanha via POST /sender/listmessages.
        message_status: "Scheduled" | "Sent" | "Failed" (opcional).
        context: dict opcional (campaign_id, instance_id) para logs de erro.
        Retorna dict com messages (array) e pagination.
        """
        url = f"{self.base_url}/sender/listmessages"
        headers = {
            "token": token,
            "Content-Type": "application/json",
        }
        payload: dict[str, Any] = {"folder_id": folder_id}
        if message_status is not None:
            payload["messageStatus"] = message_status
        if page is not None:
            payload["page"] = page
        if page_size is not None:
            payload["pageSize"] = page_size
        ctx_str = f" {context}" if context else ""

        def _should_silent_return(resp) -> bool:
            if resp is None:
                return False
            if resp.status_code == 401:
                body = (getattr(resp, "text", None) or "").lower()
                if "invalid token" in body:
                    return True
            if resp.status_code == 400:
                body = (getattr(resp, "text", None) or "").lower()
                if "folder not found" in body or "access denied" in body:
                    return True
            return False

        def _maybe_log_401(resp, ctx: Optional[dict], endpoint: str) -> None:
            if resp is None or resp.status_code != 401:
                return
            inst_id = (ctx or {}).get("instance_id")
            log_key = (
                "uazapi_401",
                endpoint,
                inst_id if inst_id is not None else (ctx or {}).get("campaign_id"),
            )
            if not _uazapi_should_log_401(log_key):
                return
            msg = f"instance_id={inst_id}" if inst_id is not None else f"campaign_id={(ctx or {}).get('campaign_id')}" if (ctx or {}).get("campaign_id") is not None else "instância"
            print(f"⚠️ [Uazapi] {msg}: 401 Invalid token ({endpoint}). Atualize o apikey da instância.")

        try:
            response = requests.post(
                url, json=payload, headers=headers, timeout=15
            )
            if _should_silent_return(response):
                _maybe_log_401(response, context, "list_messages")
                return None
            if response.status_code == 400:
                print(f"❌ [Uazapi] list_messages 400{ctx_str}: {(response.text or '')[:200]}")
                return None
            if response.status_code != 200:
                print(f"❌ [Uazapi] list_messages Status: {response.status_code}{ctx_str}")
                print(f"❌ [Uazapi] list_messages Body: {response.text}")
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            resp = getattr(e, "response", None)
            if _should_silent_return(resp):
                _maybe_log_401(resp, context, "list_messages")
                return None
            print(f"❌ [Uazapi] Error listing messages: {e}{ctx_str}")
            if resp is not None:
                print(f"❌ [Uazapi] Response: {resp.text}")
            return None

    def message_find(
        self,
        token: str,
        chatid: str,
        limit: int = 30,
        offset: int = 0,
        context: Optional[dict] = None,
    ) -> Optional[dict[str, Any]]:
        """
        Busca mensagens de um chat via POST /message/find.
        chatid: ex. 5511999999999@s.whatsapp.net
        """
        url = f"{self.base_url}/message/find"
        headers = {
            "token": token,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        payload: dict[str, Any] = {
            "chatid": chatid,
            "limit": max(1, min(int(limit), 200)),
            "offset": max(0, int(offset)),
        }
        ctx_str = f" {context}" if context else ""

        def _should_silent_return(resp) -> bool:
            if resp is None:
                return False
            if resp.status_code == 401:
                body = (getattr(resp, "text", None) or "").lower()
                if "invalid token" in body:
                    return True
            return False

        def _maybe_log_401(resp, ctx: Optional[dict], endpoint: str) -> None:
            if resp is None or resp.status_code != 401:
                return
            inst_id = (ctx or {}).get("instance_id")
            log_key = (
                "uazapi_401",
                endpoint,
                inst_id if inst_id is not None else (ctx or {}).get("campaign_id"),
            )
            if not _uazapi_should_log_401(log_key):
                return
            msg = f"instance_id={inst_id}" if inst_id is not None else f"campaign_id={(ctx or {}).get('campaign_id')}" if (ctx or {}).get("campaign_id") is not None else "instância"
            print(f"⚠️ [Uazapi] {msg}: 401 Invalid token ({endpoint}). Atualize o apikey da instância.")

        try:
            response = requests.post(
                url, json=payload, headers=headers, timeout=20
            )
            if _should_silent_return(response):
                _maybe_log_401(response, context, "message_find")
                return None
            if response.status_code != 200:
                if response.status_code != 400:
                    print(f"❌ [Uazapi] message_find Status: {response.status_code}{ctx_str}")
                return None
            return response.json()
        except requests.exceptions.RequestException as e:
            resp = getattr(e, "response", None)
            if _should_silent_return(resp):
                _maybe_log_401(resp, context, "message_find")
                return None
            if os.environ.get("UAZAPI_DEBUG", "").strip().lower() in ("1", "true", "yes"):
                print(f"❌ [Uazapi] message_find error: {e}{ctx_str}")
            return None
