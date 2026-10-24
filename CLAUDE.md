# ai-data-manager

Price data management workspace across asset classes and providers.

## Python environment

Always use the project venv. Never use system Python.

```bash
uv run python <script>          # preferred for one-off scripts
uv run pytest                   # test suite (no network, no price-data/ needed)
source .venv/bin/activate       # for interactive sessions
```

## Everything else

[`AGENTS.md`](AGENTS.md) is the source of truth: layout, the provider-first
path rule, the canonical OHLCV schema, catalog shapes, the tool table, and how
to add a tool or an agent skill. Read it before writing code that touches
`price-data/`.

[`ROADMAP.md`](ROADMAP.md) has the current work plan and the known gaps —
check it before assuming something is missing by accident.

Skills live in `.agent/skills/<name>/SKILL.md`, harness-neutral. To use them
here: `ln -s ../.agent/skills .claude/skills`.

Never hardcode an absolute path: every machine-specific value resolves through
`utils/config.py` from the environment (`.env.example` lists them).
