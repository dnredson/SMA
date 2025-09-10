# src/core/publisher.py
from __future__ import annotations
import json
import logging
from typing import Any, Dict, List, Tuple
import urllib.parse
import http.client
import time

log = logging.getLogger("publisher")


class HttpPublisher:
    def __init__(self, cfg: Dict[str, Any]) -> None:
        self.base = (cfg.get("http_adapter_url", "http://localhost:8008") or "").rstrip(
            "/"
        )
        self.timeout = cfg.get("publish_timeout_ms", 5000) / 1000.0
        # "dash" replica a rota da dashboard (/m/<domain>/c/<channel>/)
        # "api" usa /http/channels/<domain>/<channel>/messages
        self.style = (
            cfg.get("http_ingest_style") or "dash"
        ).lower()  # "dash" | "api" | "auto"
        self.auth_scheme = cfg.get("http_auth_scheme", "Client")  # "Client" | "Thing"
        self.use_subtopic = bool(cfg.get("http_use_subtopic", False))
        self.log_full_body = bool(cfg.get("publisher_log_full_body", False))

    # ---------- paths ----------
    def _path_dash(self, domain_id: str, channel_id: str) -> str:
        return f"/m/{urllib.parse.quote(domain_id)}/c/{urllib.parse.quote(channel_id)}/"

    def _path_api(self, domain_id: str, channel_id: str) -> str:
        return f"/http/channels/{urllib.parse.quote(domain_id)}/{urllib.parse.quote(channel_id)}/messages"

    # ---------- helpers ----------
    def _masked(self, secret: str) -> str:
        if not secret:
            return ""
        if len(secret) <= 8:
            return "*" * len(secret)
        return secret[:4] + "…" + secret[-4:]

    def _ensure_colon(self, bn: str | None) -> str | None:
        if not bn:
            return bn
        return bn if bn.endswith(":") else bn + ":"

    def _normalize_bt(self, bt: Any) -> Any:
        try:
            # se vier em nanos (>> segundos), normaliza para segundos (float)
            if isinstance(bt, (int, float)) and float(bt) > 1e12:
                return float(bt) / 1_000_000_000.0
        except Exception:
            pass
        return bt

    def _massage_senml(self, senml: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Ajusta o payload para o formato que o Magistrala realmente armazena:
        - Se o primeiro item não tiver 'n' (só 'bn'/'bt'), injeta 'bn'/'bt' no primeiro item de medida e remove o cabeçalho.
        - Garante 'bn' com ':' no item que carrega 'bn'.
        - Normaliza 'bt' para segundos quando vier 'grande' (provável nanos).
        """
        if not senml:
            return senml

        out = [x.copy() for x in senml]

        # 1) se head é "base-only", funde com a 1ª medida
        head = out[0]
        head_has_measure = any(k in head for k in ("n", "v", "vd", "vb", "vs"))
        if not head_has_measure and len(out) >= 2:
            bn = self._ensure_colon(head.get("bn"))
            bt = self._normalize_bt(head.get("bt"))
            first_meas = out[1].copy()
            if bn is not None:
                first_meas["bn"] = bn
            if bt is not None:
                first_meas["bt"] = bt
            out = [first_meas] + out[2:]
        else:
            # 2) se o primeiro item já é medida, só garante bn com ':'
            if "bn" in out[0]:
                out[0]["bn"] = self._ensure_colon(out[0].get("bn"))
            if "bt" in out[0]:
                out[0]["bt"] = self._normalize_bt(out[0].get("bt"))

        return out

    def _curl_equiv(
        self, base: str, path: str, headers: Dict[str, str], body: bytes
    ) -> str:
        h2 = headers.copy()
        if "Authorization" in h2:
            try:
                sch, val = h2["Authorization"].split(" ", 1)
            except ValueError:
                sch, val = "Client", h2["Authorization"]
            h2["Authorization"] = f"{sch} {self._masked(val)}"
        body_text = body.decode("utf-8", "ignore")
        return (
            "curl -sS -i -X POST "
            + " ".join(f"-H '{k}: {v}'" for k, v in h2.items())
            + f" '{base}{path}' -d '{body_text}'"
        )

    def _do_post(
        self, base: str, path: str, headers: Dict[str, str], body: bytes
    ) -> Tuple[int, str, bytes]:
        parsed = urllib.parse.urlparse(base)
        conn_cls = (
            http.client.HTTPSConnection
            if parsed.scheme == "https"
            else http.client.HTTPConnection
        )
        conn = conn_cls(
            parsed.hostname,
            parsed.port or (443 if parsed.scheme == "https" else 80),
            timeout=self.timeout,
        )
        try:
            if log.isEnabledFor(logging.DEBUG):
                log.debug("HTTP POST %s%s", base, path)
                log.debug(
                    "  headers: %s",
                    {
                        **headers,
                        "Authorization": f"{self.auth_scheme} {self._masked(headers.get('Authorization','').split(' ',1)[-1])}",
                    },
                )
                if self.log_full_body or len(body) <= 8192:
                    log.debug("  body: %s", body.decode("utf-8", "ignore"))
                else:
                    log.debug(
                        "  body.len=%d (showing 8KB)\n%s",
                        len(body),
                        body[:8192].decode("utf-8", "ignore"),
                    )

                # cURL equivalente (útil para testar no terminal)
                try:
                    log.debug("  curl: %s", self._curl_equiv(base, path, headers, body))
                except Exception:
                    pass

            conn.request("POST", path, body=body, headers=headers)
            resp = conn.getresponse()
            resp_body = resp.read() or b""
            try:
                log.debug(
                    "  HTTP %d %s; body: %s",
                    resp.status,
                    resp.reason,
                    resp_body.decode("utf-8", "ignore"),
                )
            except Exception:
                log.debug(
                    "  HTTP %d %s; body.len=%d (binário)",
                    resp.status,
                    resp.reason,
                    len(resp_body),
                )
            return resp.status, resp.reason, resp_body
        finally:
            try:
                conn.close()
            except Exception:
                pass

    # ---------- público ----------
    def publish(
        self,
        *,
        domain_id: str,
        channel_id: str,
        client_secret: str,
        senml: List[Dict[str, Any]],
        subtopic: str = "",
    ) -> Tuple[bool, str | None]:
        # 1) massage payload para o formato que o Magistrala persiste
        fixed = self._massage_senml(senml)

        # 2) headers & body
        headers = {
            "Authorization": f"{self.auth_scheme} {client_secret}",
            "Content-Type": "application/senml+json",
        }
        body = json.dumps(fixed, ensure_ascii=False).encode("utf-8")

        # 3) envia no estilo “dash” (igual à dashboard)
        if self.style in ("dash", "auto"):
            status, reason, resp_body = self._do_post(
                self.base, self._path_dash(domain_id, channel_id), headers, body
            )
            if 200 <= status < 300:
                return True, None
            # fallback api se configurado como "auto"
            if status in (400, 404, 405) and self.style == "auto":
                status2, reason2, resp_body2 = self._do_post(
                    self.base, self._path_api(domain_id, channel_id), headers, body
                )
                if 200 <= status2 < 300:
                    return True, None
                return (
                    False,
                    f"HTTP {status2} {reason2}; body: {resp_body2.decode('utf-8','ignore')}",
                )
            return (
                False,
                f"HTTP {status} {reason}; body: {resp_body.decode('utf-8','ignore')}",
            )

        # 4) estilo “api” explícito
        status, reason, resp_body = self._do_post(
            self.base, self._path_api(domain_id, channel_id), headers, body
        )
        if 200 <= status < 300:
            return True, None
        return (
            False,
            f"HTTP {status} {reason}; body: {resp_body.decode('utf-8','ignore')}",
        )
