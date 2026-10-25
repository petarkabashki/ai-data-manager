"""`pdm.yaml` — read at start, rewritten when a client changes schedules or
live subscriptions.

Writes are atomic (`.tmp` + replace) and keep every top-level key the server
doesn't manage. Comments in a hand-edited file do not survive a rewrite —
YAML round-tripping with comments needs ruamel, a dependency this one file
doesn't justify — so the header points readers at `pdm.example.yaml`, which
keeps the documentation.
"""

from __future__ import annotations

import threading
from pathlib import Path

import yaml

HEADER = (
    "# PDM server config — managed: the server rewrites this file when clients\n"
    "# change schedules or live subscriptions (comments are not preserved).\n"
    "# Documented template: pdm.example.yaml. Validate: tools/pdm_server.py --check\n"
)


class ConfigStore:
    def __init__(self, path: Path | None):
        self.path = Path(path) if path else None
        self._lock = threading.Lock()
        self.data: dict = self._read()

    def _read(self) -> dict:
        if not self.path or not self.path.exists():
            return {}
        data = yaml.safe_load(self.path.read_text()) or {}
        if not isinstance(data, dict):
            raise ValueError(f"{self.path}: top level must be a mapping")
        return data

    def get(self, key: str, default=None):
        return self.data.get(key, default)

    def set(self, key: str, value) -> None:
        with self._lock:
            self.data[key] = value
            if not self.path:
                return
            body = yaml.safe_dump(self.data, sort_keys=False, default_flow_style=None)
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            tmp.write_text(HEADER + body)
            tmp.replace(self.path)
