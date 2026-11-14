#!/usr/bin/env python3
"""MCP server for the data-triage agent: two typed tools, no shell.

`dm_relay` tells the owner (desktop + Slack DM) and acknowledges event ids.
`dm_remediate` runs one allow-listed fix (see REMEDIATIONS in dm_triage.py), with a cooldown.

The agent gets these instead of a shell, so the worst it can do is send a message or run the one pre-approved fix.
"""

from __future__ import annotations

import io
import sys
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from typing import Literal

sys.path.insert(0, str(Path(__file__).resolve().parent))

from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations

import dm_triage

mcp = MCPServer(
    "dm-triage",
    instructions="Relay data-management alerts to the owner, and run the one allow-listed fix for an event that names it.",
)


@mcp.tool(annotations=ToolAnnotations(read_only_hint=False, destructive_hint=False, open_world_hint=True))
def dm_relay(severity: Literal["info", "warn", "critical"], text: str, event_ids: list[str]) -> str:
    """Send one plain-language message to the owner (desktop + Slack DM) and acknowledge the events it covers.

    Pass EVERY event id the message covers; ids you leave out are re-sent as raw alerts by the dispatcher.
    Keep `text` under 600 characters and free of credentials.
    """
    if len(text) > 600:
        raise ValueError("text is over 600 characters; shorten it")
    dm_triage.relay(severity, text, list(event_ids), via="agent")
    return "relayed" + ("" if dm_triage.WEBHOOK_FILE.exists() else " (desktop only: no slack webhook configured)")


@mcp.tool(annotations=ToolAnnotations(read_only_hint=False, destructive_hint=False, open_world_hint=True))
def dm_remediate(name: Literal["ibkr-workers-restart"]) -> str:
    """Run one allow-listed fix. Only call it for an event that lists this remediation.

    Returns the outcome; `skipped: ... already ran` means it already ran within the cooldown, so do not retry.
    """
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        code = dm_triage.cmd_remediate(SimpleNamespace(name=name))
    return f"exit {code}: " + (out.getvalue() + err.getvalue()).strip()[-500:]


def main() -> None:
    mcp.run("stdio")


if __name__ == "__main__":
    main()
