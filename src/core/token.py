from __future__ import annotations
import base64
import json
import time
import subprocess
import shlex
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib import request, error

from .config import load_config, write_config_atomic

log = logging.getLogger("auth")


class TokenError(Exception):
    pass


class TokenManager:
    """Gerencia access/refresh tokens com cache em config e múltiplas fontes (CLI/HTTP)."""

    def __init__(
        self, cfg: Dict[str, Any], decrypt_cb, cfg_path: Optional[Path] = None
    ):
        self.cfg = cfg
        self._decrypt = decrypt_cb
        self._cfg_path = cfg_path
        self._access: Optional[str] = None
        self._refresh: Optional[str] = None
        self._exp_ts: Optional[int] = None  # epoch seconds
        self.margin = int(cfg.get("token_renew_margin_seconds", 300))
        self._load_cached_tokens()

    # --- helpers ---
    def _jwt_exp(self, tok: str) -> Optional[int]:
        try:
            parts = tok.split(".")
            if len(parts) < 2:
                return None
            pad = "=" * (-len(parts[1]) % 4)
            payload = json.loads(base64.urlsafe_b64decode(parts[1] + pad))
            return int(payload.get("exp")) if "exp" in payload else None
        except Exception as e:
            log.debug("jwt exp parse falhou: %s", e)
            return None

    def _post_json(self, url: str, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        data = json.dumps(body).encode("utf-8")
        req = request.Request(
            url, data=data, headers={"Content-Type": "application/json"}, method="POST"
        )
        try:
            with request.urlopen(req, timeout=5) as resp:
                status = resp.status
                raw = resp.read()
                payload = json.loads(raw.decode("utf-8")) if raw else {}
                return status, payload
        except error.HTTPError as e:
            try:
                payload = json.loads(e.read().decode("utf-8"))
            except Exception:
                payload = {"error": str(e)}
            return e.code, payload

    # --- cache em config.toml ---
    def _persist_tokens(
        self, access: str, refresh: Optional[str], exp_ts: Optional[int]
    ) -> None:
        self._access, self._refresh, self._exp_ts = access, refresh, exp_ts
        if not self._cfg_path:
            log.debug("tokens obtidos (memória): exp_ts=%s", exp_ts)
            return
        try:
            cfg = load_config(self._cfg_path)
            blob = {"access_token": access}
            if refresh:
                blob["refresh_token"] = refresh
            if exp_ts:
                blob["expires_at"] = int(exp_ts)
            cfg["magistrala_user_tokens_json"] = json.dumps(
                blob, separators=(",", ":"), ensure_ascii=True
            )
            write_config_atomic(self._cfg_path, cfg)
            self.cfg["magistrala_user_tokens_json"] = cfg["magistrala_user_tokens_json"]
            log.debug("tokens persistidos no config.toml")
        except Exception as e:
            log.warning("falha ao persistir tokens no config.toml: %s", e)

    def _load_cached_tokens(self) -> None:
        blob = self.cfg.get("magistrala_user_tokens_json")
        if not blob:
            return
        try:
            data = json.loads(blob) if isinstance(blob, str) else blob
            access = data.get("access_token")
            refresh = data.get("refresh_token")
            exp_ts = data.get("expires_at")
            if access:
                if not exp_ts:
                    exp_ts = self._jwt_exp(access)
                self._access, self._refresh, self._exp_ts = access, refresh, exp_ts
                log.debug("carregado token do cache: exp_ts=%s", exp_ts)
        except Exception as e:
            log.debug("falha ao ler cache de tokens: %s", e)

    # --- emissores ---
    def _try_issue_http(
        self, email: str, password: str, base: str
    ) -> Tuple[Optional[str], Optional[str], Optional[int]]:
        candidates = [
            f"{base}/users/tokens",
            f"{base}/users/login",
            f"{base}/tokens",
            f"{base}/tokens/issue",
        ]
        body = {"email": email, "password": password}
        for url in candidates:
            try:
                log.debug("HTTP auth: tentando %s", url)
                status, payload = self._post_json(url, body)
                log.debug("HTTP auth: status=%s keys=%s", status, list(payload.keys()))
                if 200 <= status < 300:
                    access = (
                        payload.get("access_token")
                        or payload.get("token")
                        or payload.get("value")
                        or payload.get("access")
                    )
                    refresh = payload.get("refresh_token") or payload.get("refresh")
                    exp_ts: Optional[int] = payload.get("expires_at")
                    if not exp_ts and access:
                        exp_ts = self._jwt_exp(access)
                    if access:
                        return access, refresh, exp_ts
            except Exception as e:
                log.debug("HTTP auth falhou em %s: %s", url, e)
        return None, None, None

    def _try_issue_cli(
        self, email: str, password: str, cli_path: str
    ) -> Tuple[Optional[str], Optional[str], Optional[int]]:
        import os, subprocess, shlex, json, logging

        log = logging.getLogger("auth")
        users_url = self.cfg.get("users_url")
        cli_cwd = self.cfg.get("magistrala_cli_cwd") or os.path.dirname(cli_path) or "."

        # (1) bootstrap de config do CLI (garante users_url)
        if users_url:
            cmd_cfg = (
                f"{shlex.quote(cli_path)} config users_url {shlex.quote(users_url)}"
            )
            log.debug(
                "CLI auth: configurando users_url via: %s (cwd=%s)", cmd_cfg, cli_cwd
            )
            try:
                subprocess.check_output(
                    cmd_cfg,
                    shell=True,
                    stderr=subprocess.STDOUT,
                    timeout=5,
                    cwd=cli_cwd,
                )
            except subprocess.CalledProcessError as e:
                log.warning(
                    "CLI config falhou: rc=%s out=%s",
                    e.returncode,
                    e.output.decode(errors="ignore"),
                )
            except Exception as e:
                log.warning("CLI config exceção: %s", e)

        # (2) emitir token
        cmd_tok = f"{shlex.quote(cli_path)} users token {shlex.quote(email)} {shlex.quote(password)}"
        log.debug("CLI auth: executando %s (cwd=%s)", cmd_tok, cli_cwd)
        try:
            # opcional: injeta HOME/XDG_CONFIG_HOME se quiser forçar o mesmo HOME do usuário
            env = os.environ.copy()
            # env["HOME"] = "/home/dener"  # se precisar
            # env["XDG_CONFIG_HOME"] = "/home/dener/.config"  # se precisar

            out = subprocess.check_output(
                cmd_tok,
                shell=True,
                stderr=subprocess.STDOUT,
                timeout=10,
                cwd=cli_cwd,
                env=env,
            )
            txt = out.decode("utf-8", errors="ignore").strip()
            log.debug(
                "CLI auth: resposta %s", (txt[:180] + "…") if len(txt) > 180 else txt
            )
            data = json.loads(txt)
            access = data.get("access_token") or data.get("token")
            refresh = data.get("refresh_token") or data.get("refresh")
            exp_ts: Optional[int] = data.get("expires_at")
            if not exp_ts and access:
                exp_ts = self._jwt_exp(access)
            return access, refresh, exp_ts
        except subprocess.CalledProcessError as e:
            log.warning(
                "CLI auth falhou: rc=%s out=%s",
                e.returncode,
                e.output.decode(errors="ignore"),
            )
        except Exception as e:
            log.warning("CLI auth exceção: %s", e)
        return None, None, None

    # --- público ---
    def ensure(self) -> str:
        now = int(time.time())
        if self._access and self._exp_ts and (now + self.margin) < self._exp_ts:
            return self._access
        self._load_cached_tokens()
        if self._access and self._exp_ts and (now + self.margin) < self._exp_ts:
            return self._access

        email = self.cfg.get("magistrala_user")
        enc_holder = self.cfg.get("magistrala_user_password_enc") or self.cfg.get(
            "magistrala_user_password_enc_json"
        )
        if not email or enc_holder is None:
            raise TokenError(
                "credenciais não configuradas (magistrala_user/magistrala_user_password_enc[_json])"
            )
        password = self._decrypt(enc_holder)

        cli_path = self.cfg.get("magistrala_cli_path")
        if cli_path:
            access, refresh, exp_ts = self._try_issue_cli(email, password, cli_path)
            if access:
                self._persist_tokens(access, refresh, exp_ts or (now + 3600))
                return self._access  # type: ignore
            else:
                log.debug("CLI não retornou token, tentando HTTP…")

        users_url = self.cfg.get("users_url")
        if users_url:
            access, refresh, exp_ts = self._try_issue_http(email, password, users_url)
            if access:
                self._persist_tokens(access, refresh, exp_ts or (now + 3600))
                return self._access  # type: ignore

        raise TokenError("falha ao emitir token por CLI e HTTP")
