"""Test fixtures.

These tests never touch `price-data/` and never hit the network. Anything that
needs a data root gets a tmp_path one via `PRICE_DATA_ROOT`, so the suite runs
on a clean checkout with no symlink present.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
# A developer's or server's local .env must not leak into the suite.
os.environ.setdefault("PDM_NO_DOTENV", "1")
os.environ["PDM_TOKEN_FILE"] = os.devnull  # never pick up the real client token
sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture
def ohlcv():
    """Factory for canonical frames: ohlcv(start_ms, n, step_ms)."""

    def _make(start_ms: int = 0, n: int = 5, step_ms: int = 86_400_000, close: float = 1.0):
        ts = [start_ms + i * step_ms for i in range(n)]
        return pd.DataFrame(
            {
                "timestamp": pd.Series(ts, dtype="int64"),
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": close,
                "volume": 100.0,
            }
        )

    return _make
