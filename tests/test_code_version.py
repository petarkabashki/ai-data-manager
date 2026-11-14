"""A long-running writer notices a deploy that changed python, and stops between items."""
from __future__ import annotations

import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))

from utils.code_version import (  # noqa: E402
    EXIT_CODE_CHANGED, CodeVersion, git_head, head_at, reflog, running_writers,
)


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], check=True,
                          capture_output=True, text=True).stdout.strip()


def _commit(repo: Path, name: str, text: str) -> str:
    (repo / name).parent.mkdir(parents=True, exist_ok=True)
    (repo / name).write_text(text)
    _git(repo, "add", name)
    _git(repo, "commit", "-q", "-m", f"change {name}")
    return _git(repo, "rev-parse", "HEAD")


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    r = tmp_path / "repo"
    r.mkdir()
    _git(r, "init", "-q", "-b", "main")
    _git(r, "config", "user.email", "t@example.invalid")
    _git(r, "config", "user.name", "t")
    _commit(r, "tools/w.py", "x = 1\n")
    return r


def test_git_head_reads_ref_detached_and_packed(repo: Path):
    head = _git(repo, "rev-parse", "HEAD")
    assert git_head(repo) == head
    _git(repo, "pack-refs", "--all")
    assert not (repo / ".git/refs/heads/main").exists()
    assert git_head(repo) == head
    _git(repo, "checkout", "-q", "--detach")
    assert git_head(repo) == head


def test_unchanged_checkout_is_not_a_change(repo: Path):
    assert CodeVersion(repo).changed() is None


def test_a_commit_without_python_moves_the_baseline(repo: Path):
    code = CodeVersion(repo)
    _commit(repo, "reviews/pending.yaml", "- a\n")
    assert code.changed() is None
    _commit(repo, "docs/x.md", "doc\n")
    assert code.changed() is None


def test_a_python_change_is_seen_even_after_a_baseline_move(repo: Path):
    code = CodeVersion(repo)
    _commit(repo, "reviews/pending.yaml", "- a\n")
    assert code.changed() is None
    new = _commit(repo, "utils/catalog.py", "KEYS = ('data_flags',)\n")
    assert code.changed() == new


def test_an_unrelated_tool_moves_the_baseline(repo: Path):
    code = CodeVersion(repo)
    assert "utils/" in code.code_paths() and "pdm/" in code.code_paths()
    _commit(repo, "tools/other.py", "y = 1\n")  # not imported by this process
    assert code.changed() is None
    assert code._baseline == git_head(repo)


def test_exit_if_changed_saves_then_exits_75(repo: Path):
    code = CodeVersion(repo)
    saved = []
    code.exit_if_changed(before_exit=lambda: saved.append(1))  # nothing changed
    assert saved == []
    _commit(repo, "utils/catalog.py", "x = 2\n")
    with pytest.raises(SystemExit) as exc:
        code.exit_if_changed(before_exit=lambda: saved.append(1))
    assert exc.value.code == EXIT_CODE_CHANGED == 75
    assert saved == [1]


def test_crypto_checkpoint_stops_after_the_pair_on_a_code_change(repo: Path):
    from download_crypto import CatalogCheckpoint

    saves = []
    cp = CatalogCheckpoint(lambda: saves.append(1), every_s=3600, code=CodeVersion(repo))
    cp()  # first pair done, code unchanged: no save (interval not reached), no exit
    assert saves == []
    _commit(repo, "pdm/jobs.py", "x = 3\n")
    with pytest.raises(SystemExit) as exc:
        cp()
    assert exc.value.code == 75 and saves == [1]


def test_head_at_follows_the_reflog(repo: Path):
    log = reflog(repo)
    assert log and log[-1][1] == git_head(repo)
    first_t = log[0][0]
    assert head_at(datetime.fromtimestamp(first_t - 10, tz=timezone.utc), log) is None
    assert head_at(datetime.now(tz=timezone.utc), log) == git_head(repo)


@pytest.mark.skipif(not Path("/proc/self/stat").exists(), reason="needs /proc")
def test_running_writers_lists_repo_and_foreign_writers(repo: Path, tmp_path: Path):
    data = tmp_path / "price-data"
    data.mkdir()
    (repo / "tools/sleeper.py").write_text("import time\ntime.sleep(30)\n")
    time.sleep(1.1)  # /proc start times rest on whole-second btime; start after the commit
    ours = subprocess.Popen([sys.executable, "tools/sleeper.py", "--flag"], cwd=repo)
    # a process from elsewhere holding a data file open for writing
    foreign = subprocess.Popen(
        [sys.executable, "-c",
         f"import time; f = open({str(data / 'x.parquet')!r}, 'w'); time.sleep(30)"],
        cwd=tmp_path)
    try:
        for _ in range(50):
            rows = {r["pid"]: r for r in running_writers(repo, data_root=data)}
            if ours.pid in rows and foreign.pid in rows:
                break
            time.sleep(0.1)
        mine = rows[ours.pid]
        assert mine["kind"] == "repo" and mine["script"] == "tools/sleeper.py"
        assert mine["args"] == "--flag"
        assert mine["code_head"] == git_head(repo) and mine["stale"] is False
        assert rows[foreign.pid]["kind"] == "foreign"
        assert rows[foreign.pid]["writes"].endswith("x.parquet")

        time.sleep(1.1)  # the reflog has second resolution
        _commit(repo, "tools/other.py", "y = 1\n")  # another tool: not stale
        rows = {r["pid"]: r for r in running_writers(repo, data_root=data)}
        assert rows[ours.pid]["stale"] is False
        _commit(repo, "utils/catalog.py", "changed = True\n")
        rows = {r["pid"]: r for r in running_writers(repo, data_root=data)}
        assert rows[ours.pid]["stale"] is True
    finally:
        ours.kill()
        foreign.kill()
