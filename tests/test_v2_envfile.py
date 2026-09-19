from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from smarter_adapter.envfile import EnvFileError, load_default_env, load_env_file, parse_env_file


class EnvFileTests(unittest.TestCase):
    def test_parse_supports_comments_export_and_quotes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / ".env"
            path.write_text(
                """
# comment
export SMA_ENVIRONMENT=irrigap
ATOM_PASSWORD='12345678'
SMA_API_TOKEN=\"token value\" # inline comment
EMPTY=
""".lstrip(),
                encoding="utf-8",
            )
            values = parse_env_file(path)
        self.assertEqual(values["SMA_ENVIRONMENT"], "irrigap")
        self.assertEqual(values["ATOM_PASSWORD"], "12345678")
        self.assertEqual(values["SMA_API_TOKEN"], "token value")
        self.assertEqual(values["EMPTY"], "")

    def test_existing_environment_wins_by_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / ".env"
            path.write_text("A=file\nB=file\n", encoding="utf-8")
            environ = {"A": "shell"}
            loaded = load_env_file(path, environ=environ)
        self.assertEqual(environ["A"], "shell")
        self.assertEqual(environ["B"], "file")
        self.assertNotIn("A", loaded)
        self.assertEqual(loaded["B"], "file")

    def test_default_env_is_optional_but_explicit_file_is_required(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            environ = {}
            path, loaded = load_default_env(root, environ=environ)
            self.assertEqual(path, root / ".env")
            self.assertEqual(loaded, {})

            environ["SMA_CONFIG_FILE"] = "missing.env"
            with self.assertRaises(EnvFileError):
                load_default_env(root, environ=environ)

    def test_relative_explicit_config_resolves_from_repository_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "config" / "lab.env"
            config.parent.mkdir()
            config.write_text("SMA_MQTT_HOST=broker.example\n", encoding="utf-8")
            environ = {"SMA_CONFIG_FILE": "config/lab.env"}
            path, loaded = load_default_env(root, environ=environ)
        self.assertEqual(path, config.resolve())
        self.assertEqual(environ["SMA_MQTT_HOST"], "broker.example")
        self.assertEqual(loaded["SMA_MQTT_HOST"], "broker.example")


if __name__ == "__main__":
    unittest.main()
