from __future__ import annotations
from pathlib import Path
from typing import Dict
import os


class KeyManager:
    def __init__(self, key_path: Path) -> None:
        self.key_path = key_path
        self.key = self._load_or_create()

    def _load_or_create(self) -> bytes:
        if self.key_path.exists():
            return self.key_path.read_bytes()
        key = os.urandom(32)  # AES-256
        self.key_path.write_bytes(key)
        os.chmod(self.key_path, 0o600)
        return key


def encrypt_string(km: KeyManager, plaintext: str) -> Dict[str, str]:
    """RETORNA um dicionário {v, alg, nonce, ct}. (Implementar AES-256-GCM de verdade depois.)"""
    # TODO: implementar com cryptography (AESGCM). Aqui guardamos como stub base64.
    import base64, os

    nonce = os.urandom(12)
    ct = plaintext.encode("utf-8")  # provisório — substitua pelo AESGCM
    return {
        "v": "1",
        "alg": "aes-256-gcm",
        "nonce": base64.b64encode(nonce).decode(),
        "ct": base64.b64encode(ct).decode(),
    }
