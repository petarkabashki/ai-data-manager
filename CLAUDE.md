# ai-data-manager

Price data management workspace across asset classes and providers.

## Python environment

Always use the project venv. Never use system Python.

```bash
uv run python <script>          # preferred for one-off scripts
source .venv/bin/activate       # for interactive sessions
```

## Layout

- `.agent/` — agent skills, prompts, agentic configs
- `tools/` — standalone CLI tools (runnable scripts)
- `utils/` — shared importable Python utilities
- `price-data/` — symlink to `../price-data/` (data + metadata)

See [agents.md](agents.md) for full data conventions, asset class coverage,
and how to add tools/agent skills.

## Data root

```python
DATA_ROOT = Path(__file__).parent / "price-data"
ASSETS_YAML = DATA_ROOT / "assets.yaml"
```

## Key metadata files

- `price-data/assets.yaml` — master registry (includes per-class YAML files)
- `price-data/crypto_binance.yaml`, `sp500.yaml`, etc. — per-class catalogs

## Running tools

```bash
uv run python tools/<tool>.py [args]
```
