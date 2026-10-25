"""pdm/alerts.py: failed jobs and series that stop updating reach the owner once."""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from pdm.alerts import Alerter


def _job(state, source="schedule:yf-daily", **kw):
    return SimpleNamespace(id=kw.pop("id", "j1"), tool="download_yf", args=["--limit", "2"], state=state,
                           source=source, returncode=kw.pop("returncode", 1), error=None,
                           log_path=kw.pop("log_path", None))


def _alerter(tmp_path, **kw):
    sent = []
    return Alerter(lambda topic, a: sent.append((topic, a)), tmp_path, **kw), sent


def test_a_failing_schedule_alerts_once_then_on_recovery(tmp_path):
    log = tmp_path / "job.log"
    log.write_text("fetching AAPL\nValueError: --full requires --since\n\n")
    alerts, sent = _alerter(tmp_path)

    first = alerts.on_job(_job("failed", log_path=str(log)))
    assert first["kind"] == "job" and first["text"].endswith("job j1: ValueError: --full requires --since")
    assert alerts.on_job(_job("timed_out")) is None                    # still failing: quiet
    assert alerts.on_job(_job("running")) is None
    assert alerts.on_job(_job("succeeded"))["kind"] == "recovered"
    assert alerts.on_job(_job("succeeded")) is None
    assert alerts.on_job(_job("cancelled")) is None
    assert [a["kind"] for _, a in sent] == ["job", "recovered"]
    assert {t for t, _ in sent} == {"server/alert"}


def test_one_off_jobs_alert_every_time_and_state_survives_a_restart(tmp_path):
    alerts, _ = _alerter(tmp_path)
    assert alerts.on_job(_job("failed", source="client"))
    assert alerts.on_job(_job("failed", source="client"))
    alerts.on_job(_job("failed"))

    again, _ = _alerter(tmp_path)
    assert again.on_job(_job("failed")) is None                         # remembered as failing
    assert again.on_job(_job("succeeded"))["kind"] == "recovered"
    assert [a["kind"] for a in again.recent()] == ["recovered", "job", "job", "job"]   # newest first


def test_only_series_that_stop_updating_alert(tmp_path):
    today = datetime.now(timezone.utc).date()

    def row(symbol, days_behind, timeframe="1d"):
        end = (today - timedelta(days=days_behind)).isoformat()
        return {"catalog": "sp500", "symbol": symbol, "timeframe": timeframe, "provider": "yf",
                "session": None, "end_date": end}

    alerts, _ = _alerter(tmp_path, stale_days=4)
    assert alerts.check_stale([row("AAPL", 1), row("MSFT", 1), row("DEAD", 400)]) == []    # baseline
    out = alerts.check_stale([row("AAPL", 5), row("MSFT", 1), row("DEAD", 401), row("NEW", 90)])
    assert [a["kind"] for a in out] == ["stale"]
    assert out[0]["series"] == ["sp500/AAPL/1d/yf/-"] and "sp500 1d x1" in out[0]["text"]
    assert alerts.check_stale([row("AAPL", 6), row("MSFT", 1)]) == []                      # already told

    again, _ = _alerter(tmp_path, stale_days=4)                                           # restart
    back = again.check_stale([row("AAPL", 0), row("MSFT", 1)])
    assert [a["kind"] for a in back] == ["recovered"] and back[0]["count"] == 1


def test_notify_command_gets_text_on_stdin_and_json_in_env(tmp_path):
    out = tmp_path / "notified.json"
    script = ("import json,os,sys; json.dump({'text': sys.stdin.read(), 'alert': json.loads(os.environ['PDM_ALERT'])},"
              f" open({str(out)!r}, 'w'))")
    alerts, _ = _alerter(tmp_path, notify_cmd=f"{sys.executable} -c \"{script}\"")
    alert = alerts.raise_("test", "hello")
    deadline = time.time() + 10
    while not out.exists() and time.time() < deadline:
        time.sleep(0.05)
    got = json.loads(out.read_text())
    assert got["text"] == "hello" and got["alert"]["id"] == alert["id"]


def test_the_alert_log_is_trimmed(tmp_path):
    alerts, _ = _alerter(tmp_path, keep=3)
    for i in range(7):
        alerts.raise_("test", f"n{i}")
    assert len((tmp_path / "pdm_alerts.jsonl").read_text().splitlines()) <= 6
    assert [a["text"] for a in alerts.recent(2)] == ["n6", "n5"]
