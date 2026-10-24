"""Shared console output helpers for the CLI tools in tools/."""

from __future__ import annotations

import sys
from typing import Any, Sequence


def allow_broken_pipe() -> None:
    """Exit quietly when our output is piped into something that stops reading.

    `tool.py --json | head` otherwise ends in a BrokenPipeError traceback, which
    looks like a tool failure to anything parsing stderr. POSIX only; a no-op
    elsewhere.
    """
    try:
        from signal import SIG_DFL, SIGPIPE, signal

        signal(SIGPIPE, SIG_DFL)
    except (ImportError, AttributeError, ValueError):
        # No SIGPIPE (Windows), or not on the main thread. Python's default
        # BrokenPipeError handling applies instead.
        pass


def format_row(cells: Sequence[Any], widths: Sequence[int]) -> str:
    return "  ".join(str(c).ljust(w) for c, w in zip(cells, widths))


def render_table(headers: Sequence[str], rows: Sequence[Sequence[Any]], separator: bool = True) -> str:
    """Left-aligned fixed-width table, sized to its widest cell per column."""
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))

    lines = [format_row(headers, widths)]
    if separator:
        lines.append("  ".join("-" * w for w in widths))
    lines.extend(format_row(row, widths) for row in rows)
    return "\n".join(lines)


def print_table(headers: Sequence[str], rows: Sequence[Sequence[Any]], separator: bool = True) -> None:
    print(render_table(headers, rows, separator))
