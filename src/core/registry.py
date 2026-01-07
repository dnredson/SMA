# src/core/registry.py
from __future__ import annotations
import json
import os
import shlex
import subprocess
import time
import re
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .token import TokenManager, TokenError

logger = logging.getLogger("registry")


def _looks_like_secret(s: str) -> bool:
    """Heurística simples para evitar usar 'Usage:' como segredo."""
    if not s:
        return False
    s = s.strip()
    if "Usage:" in s or "Clients management" in s or "\n" in s:
        return False
    # UUID-like
    if re.fullmatch(r"[0-9a-fA-F-]{20,}", s):
        return True
    # token alfanum longo
    if re.fullmatch(r"[A-Za-z0-9_\-\.]{16,}", s):
        return True
    return False


@dataclass
class EnsureResult:
    ok: bool
    client_id: Optional[str] = None
    client_secret: Optional[str] = None  # plaintext (apenas em memória)
    domain_id: Optional[str] = None
    channel_id: Optional[str] = None
    error: Optional[str] = None


class Registry:
    """
    Implementação baseada no magistrala-cli:

      - criar cliente:
        magistrala-cli clients create '<json>' <DOMAIN_ID> <ACCESS_TOKEN> -r

      - conectar cliente a canal (permissões):
        magistrala-cli clients connect <CLIENT_ID> <CHANNEL_ID> '["publish","subscribe"]' <DOMAIN_ID> <ACCESS_TOKEN>
        (fallback para '["messages"]')

      - rotacionar/obter segredo:
        magistrala-cli clients secret <CLIENT_ID> <DOMAIN_ID> <ACCESS_TOKEN> --raw

      - lookup:
        magistrala-cli clients get all <DOMAIN_ID> <ACCESS_TOKEN> [--name <N>] [--metadata external_id:<ID>] --limit 200
    """

    def __init__(
        self, cfg: Dict[str, Any], key_manager, store, cfg_path: Optional[Path] = None
    ) -> None:
        self.cfg = cfg
        self.km = key_manager
        self.store = store
        self.cfg_path = cfg_path

        # decrypt callback esperado pelo TokenManager
        def _decrypt(enc_or_json) -> str:
            import base64, json as _json

            if isinstance(enc_or_json, str):
                try:
                    enc = _json.loads(enc_or_json)
                except _json.JSONDecodeError:
                    # retrocompat: se alguém salvou só base64 do plaintext
                    return base64.b64decode(enc_or_json).decode("utf-8")
            else:
                enc = enc_or_json
            # TODO: trocar pelo AES-256-GCM real (core.crypto). v1: ct é base64 do plaintext.
            return base64.b64decode(enc["ct"]).decode("utf-8")

        self.tk = TokenManager(cfg, _decrypt, cfg_path=self.cfg_path)

    # ==================== Helpers CLI ====================

    def _cli_path(self) -> str:
        cli = self.cfg.get("magistrala_cli_path")
        if not cli or not os.path.exists(cli):
            raise RuntimeError("magistrala_cli_path não configurado ou não existe")
        return cli

    def _cli_env_cwd(self) -> Tuple[Dict[str, str], Optional[str]]:
        env = os.environ.copy()
        cwd = (
            self.cfg.get("magistrala_cli_cwd")
            or os.path.dirname(self._cli_path())
            or None
        )
        return env, cwd

    def _run_cli(self, args: List[str], timeout: int = 12) -> str:
        env, cwd = self._cli_env_cwd()
        cmd = " ".join(shlex.quote(a) for a in args)
        logger.debug("CLI: %s (cwd=%s)", cmd, cwd or ".")
        out = subprocess.check_output(
            cmd, shell=True, stderr=subprocess.STDOUT, timeout=timeout, env=env, cwd=cwd
        )
        txt = out.decode("utf-8", errors="ignore").strip()
        logger.debug("CLI OUT (%d chars)", len(txt))
        return txt

    # ==================== Operações CLI ====================

    def _cli_issue_access(self) -> str:
        """Normalmente o TokenManager já cuida; expõe aqui caso necessário."""
        return self.tk.ensure()

    def _cli_clients_create(
        self, name: str, meta: Dict[str, Any], domain_id: str, token: str
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Retorna (client_id, secret) ou (None, None).
        """
        cli = self._cli_path()
        payload = {
            "name": name,
            "metadata": {"external_id": name, **(meta or {})},
            "status": "enabled",
        }
        args = [
            cli,
            "clients",
            "create",
            json.dumps(payload, separators=(",", ":")),
            domain_id,
            token,
            "-r",
        ]
        try:
            txt = self._run_cli(args, timeout=15)
            obj = json.loads(txt)
            cid = obj.get("id") or obj.get("client_id")
            secret = None
            cred = obj.get("credentials") or {}
            if isinstance(cred, dict):
                secret = cred.get("secret") or cred.get("key") or cred.get("credential")
            if not secret:
                secret = obj.get("secret") or obj.get("key") or obj.get("credential")
            if cid and _looks_like_secret(secret or ""):
                return cid, secret
            return None, None
        except subprocess.CalledProcessError as e:
            logger.debug(
                "CLI create falhou: rc=%s out=%s",
                getattr(e, "returncode", "?"),
                e.output.decode(errors="ignore") if getattr(e, "output", None) else "",
            )
            return None, None
        except Exception as e:
            logger.debug("CLI create exceção: %s", e)
            return None, None

    def _cli_clients_connect(
        self, client_id: str, channel_id: str, domain_id: str, token: str
    ) -> bool:
        cli = self._cli_path()
        perms_try = ['["publish","subscribe"]', '["messages"]']
        for perms in perms_try:
            args = [
                cli,
                "clients",
                client_id,
                "connect",
                channel_id,
                perms,
                domain_id,
                token,
            ]
            try:
                _ = self._run_cli(args, timeout=12)
                return True
            except subprocess.CalledProcessError:
                continue
            except Exception:
                continue
        return False

    def _cli_clients_rotate_secret(
        self, client_id: str, domain_id: str, token: str
    ) -> Optional[str]:
        """
        Obtém/rotaciona o secret. Usa --raw para tentar imprimir apenas o segredo.
        """
        cli = self._cli_path()
        args = [cli, "clients", "secret", client_id, domain_id, token, "--raw"]
        try:
            txt = self._run_cli(args, timeout=10)
            sec = (txt or "").strip()
            if _looks_like_secret(sec):
                return sec
            return None
        except Exception:
            return None

    def _cli_clients_get_all(
        self,
        domain_id: str,
        token: str,
        name: Optional[str] = None,
        external_id: Optional[str] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        """
        Lista clientes; tenta aplicar filtros pelo nome/metadata se o CLI suportar.
        Retorna uma lista de dicts de clientes.
        """
        cli = self._cli_path()
        base = [cli, "clients", "all", "get", domain_id, token]
        variants: List[List[str]] = []

        if name:
            variants.append(base + ["--name", name, "--limit", str(limit)])

        if external_id:
            variants.append(
                base
                + ["--metadata", f"external_id:{external_id}", "--limit", str(limit)]
            )
            variants.append(
                base
                + ["--metadata", f"external_id={external_id}", "--limit", str(limit)]
            )

        variants.append(base + ["--limit", str(limit)])  # fallback sem filtro

        for args in variants:
            try:
                txt = self._run_cli(args, timeout=15)
                obj = json.loads(txt)
                if isinstance(obj, dict):
                    items = (
                        obj.get("clients")
                        or obj.get("items")
                        or obj.get("things")
                        or []
                    )
                elif isinstance(obj, list):
                    items = obj
                else:
                    items = []
                if isinstance(items, list):
                    return items
            except Exception:
                continue
        return []

    # ==================== Fluxo público ====================

    def ensure_client(
        self, external_id: str, meta: Dict[str, Any] | None = None
    ) -> EnsureResult:
        log = __import__("logging").getLogger("registry")
        domain_id = self.cfg.get("magistrala_default_domain")
        channel_id = self.cfg.get("magistrala_default_channel")
        if not domain_id or not channel_id:
            return EnsureResult(
                ok=False, error="domain/channel default não configurados"
            )

        # 0) token (p/ eventuais chamadas HTTP/CLI que precisem)
        try:
            token = self.tk.ensure()
        except TokenError as te:
            return EnsureResult(ok=False, error=f"auth: {te}")

        # 1) lookup local → se já temos secret guardado, usar
        local = self.store.get_by_external_id(external_id)
        if local:
            cid = local.get("client_id")
            enc = local.get("client_secret") or {}
            # v1: ct tem o valor "em claro" (uuid) — no futuro, decriptar de fato
            secret = self._decrypt_secret_obj(enc)
            if cid and _looks_like_secret(secret or ""):
                # garante conexão ao canal (idempotente)
                self._cli_clients_connect(cid, channel_id, domain_id, token)
                return EnsureResult(
                    ok=True,
                    client_id=cid,
                    client_secret=secret,
                    domain_id=domain_id,
                    channel_id=channel_id,
                )

        # 2) lookup remoto (evita duplicar)
        items = self._cli_clients_get_all(
            domain_id, token, name=external_id, external_id=external_id, limit=1000
        )
        match = None
        for it in items:
            nm = it.get("name")
            md = it.get("metadata") or {}
            if (nm and nm.upper() == external_id.upper()) or (
                isinstance(md, dict)
                and (md.get("external_id") or "").upper() == external_id.upper()
            ):
                match = it
                break

        # 3) se existe remoto mas não temos secret local → tentar emitir/rotacionar via CLI
        if match:
            cid = match.get("id") or match.get("client_id")
            if not cid:
                return EnsureResult(
                    ok=False, error="cliente remoto existe mas sem id (via CLI)"
                )
            secret = self._cli_clients_rotate_secret(cid, domain_id, token)
            if not _looks_like_secret(secret or ""):
                # nada de criar duplicata — devolve erro claro
                return EnsureResult(
                    ok=False,
                    error="cliente já existe mas não foi possível obter secret via CLI (--raw). Ajuste CLI/roles ou forneça o secret uma vez.",
                )
            # garantir conexão
            self._cli_clients_connect(cid, channel_id, domain_id, token)
            now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            self.store.upsert_client(
                {
                    "device_raw_topic": external_id,
                    "external_id": external_id,
                    "client_id": cid,
                    "client_secret": {
                        "v": 1,
                        "alg": "aes-256-gcm",
                        "nonce": "",
                        "ct": secret,
                    },
                    "domain_id": domain_id,
                    "channel_ids": [channel_id],
                    "active": True,
                    "created_at": match.get("created_at") or now_iso,
                    "updated_at": now_iso,
                    "last_seen": None,
                }
            )
            return EnsureResult(
                ok=True,
                client_id=cid,
                client_secret=secret,
                domain_id=domain_id,
                channel_id=channel_id,
            )

        # 4) não existe remoto → criar
        cid, secret = self._cli_clients_create(
            external_id, meta or {}, domain_id, token
        )
        if not cid or not _looks_like_secret(secret or ""):
            return EnsureResult(ok=False, error="falha ao criar cliente (via CLI)")
        if not self._cli_clients_connect(cid, channel_id, domain_id, token):
            return EnsureResult(
                ok=False, error="falha ao conectar cliente ao canal (via CLI)"
            )

        now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        self.store.upsert_client(
            {
                "device_raw_topic": external_id,
                "external_id": external_id,
                "client_id": cid,
                "client_secret": {
                    "v": 1,
                    "alg": "aes-256-gcm",
                    "nonce": "",
                    "ct": secret,
                },
                "domain_id": domain_id,
                "channel_ids": [channel_id],
                "active": True,
                "created_at": now_iso,
                "updated_at": now_iso,
                "last_seen": None,
            }
        )
        return EnsureResult(
            ok=True,
            client_id=cid,
            client_secret=secret,
            domain_id=domain_id,
            channel_id=channel_id,
        )

    # dentro de class Registry:

    def _decrypt_secret_obj(self, enc) -> Optional[str]:
        """
        Retorna o segredo em texto claro a partir do objeto salvo em entities.json.
        V1: ct costuma vir em *texto claro* (UUID). Se vier base64, decodifica.
        É tolerante a dict, string JSON ou string crua.
        """
        if not enc:
            return None
        try:
            import json, base64

            # se veio como string, pode ser JSON (nosso dicionário) ou o próprio segredo
            if isinstance(enc, str):
                enc = enc.strip()
                if not enc:
                    return None
                # tenta JSON
                try:
                    enc = json.loads(enc)
                except json.JSONDecodeError:
                    # não é JSON: pode ser base64 do segredo ou o segredo direto
                    try:
                        dec = base64.b64decode(enc).decode("utf-8")
                        return dec.strip()
                    except Exception:
                        return enc  # já é o segredo em claro
            # se chegou aqui e é dict, pegue o campo ct (ou sinônimos)
            if isinstance(enc, dict):
                ct = enc.get("ct") or enc.get("secret") or enc.get("value")
                if not ct:
                    return None
                ct = str(ct).strip()
                if not ct:
                    return None
                # tenta base64 → se falhar, considera claro
                try:
                    dec = base64.b64decode(ct).decode("utf-8")
                    return dec.strip()
                except Exception:
                    return ct
        except Exception:
            return None
