"""Which code a long-running writer loaded, and whether the checkout has moved on.

A python process keeps the modules it imported at start. A worker that runs for
days therefore keeps writing with the catalog and merge code of the day it
started, after a deploy has changed what that code must preserve. On 2026-11-13
one such worker dropped `data_flags` and refilled deleted bars.

Two defences live here:

* `CodeVersion` records the git HEAD at start. A writer calls `changed()`
  between items; when the python it runs on (the repo modules it imported, plus
  utils/ and pdm/) differs between that HEAD and the deployed one, the writer
  finishes the item it is on, logs, and exits with `EXIT_CODE_CHANGED` (75,
  the code `tools/ibkr_worker.sh` already relaunches on). Any other commit (a
  review yaml, a doc, an unrelated tool) moves the baseline instead, so
  routine commits do not restart anything.
* `running_writers()` lists every running python process whose script lives in
  this repo, with its start time and the HEAD it started on (from the reflog),
  whether or not it calls `CodeVersion`, plus any other process seen holding a
  file under the data root open for writing ("foreign writer").
  `tools/status.py` reports it.

HEAD is read from `.git` directly: no subprocess on the per-item path.
"""
from __future__ import annotations

import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from utils.config import REPO_ROOT

EXIT_CODE_CHANGED = 75


def _git_dir(repo: Path) -> Path | None:
    git = repo / ".git"
    if git.is_file():  # a worktree: "gitdir: <path>"
        text = git.read_text().strip()
        if text.startswith("gitdir:"):
            return (repo / text.split(":", 1)[1].strip()).resolve()
        return None
    return git if git.is_dir() else None


def git_head(repo: Path = REPO_ROOT) -> str | None:
    """The commit HEAD points at, or None when it can't be read."""
    git = _git_dir(Path(repo))
    if git is None:
        return None
    try:
        head = (git / "HEAD").read_text().strip()
    except OSError:
        return None
    if not head.startswith("ref:"):
        return head or None
    ref = head.split(":", 1)[1].strip()
    common = git
    if (git / "commondir").is_file():
        common = (git / (git / "commondir").read_text().strip()).resolve()
    for base in (git, common):
        try:
            return (base / ref).read_text().strip() or None
        except OSError:
            pass
    try:
        for line in (common / "packed-refs").read_text().splitlines():
            if line.endswith(" " + ref):
                return line.split(" ", 1)[0]
    except OSError:
        pass
    return None


SHARED_CODE = ("utils/", "pdm/")


def python_changed(repo: Path, old: str, new: str, paths: list[str] | None = None) -> bool:
    """True when python under `paths` (default: every .py) differs between two commits.

    Also True when git can't tell: an unknown answer must not keep old code running.
    """
    specs = [p for p in (paths or []) if p.endswith((".py", "/"))] or ["*.py"]
    specs = [f"{p}*.py" if p.endswith("/") else p for p in specs]
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo), "diff", "--quiet", old, new, "--", *specs],
            capture_output=True, timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired):
        return True
    return proc.returncode != 0


@dataclass
class CodeVersion:
    """The HEAD a process started on. Call `changed()` between work items."""

    repo: Path = REPO_ROOT
    start: str | None = None
    started_at: datetime | None = None

    def __post_init__(self) -> None:
        self.repo = Path(self.repo)
        if self.start is None:
            self.start = git_head(self.repo)
        if self.started_at is None:
            self.started_at = datetime.now(tz=timezone.utc)
        self._baseline = self.start

    def changed(self) -> str | None:
        """The deployed HEAD when its python code differs from what this process loaded.

        None while the code is the same. A HEAD that moved without touching
        python becomes the new baseline, so the next call is cheap again.
        """
        if self._baseline is None:
            return None
        now = git_head(self.repo)
        if now is None or now == self._baseline:
            return None
        if python_changed(self.repo, self.start, now, self.code_paths()):
            return now
        self._baseline = now
        return None

    def code_paths(self) -> list[str]:
        """What this process runs on: the repo modules it imported, plus the shared packages.

        The shared packages count even where not yet imported, so a later import
        can't mix new library code into an old process.
        """
        root = self.repo.resolve()
        out = set(SHARED_CODE)
        for mod in list(sys.modules.values()):
            f = getattr(mod, "__file__", None)
            if not f:
                continue
            try:
                out.add(str(Path(f).resolve().relative_to(root)))
            except ValueError:
                pass
        return sorted(out)

    def message(self, now: str) -> str:
        return (f"code changed: this process loaded {self.start[:9]} at "
                f"{self.started_at:%Y-%m-%dT%H:%M:%SZ}, the checkout is now {now[:9]}; "
                f"exiting {EXIT_CODE_CHANGED} so a fresh process continues on the new code")

    def exit_if_changed(self, before_exit=None) -> None:
        """Log and exit EXIT_CODE_CHANGED when the code changed; `before_exit()` saves state first."""
        now = self.changed()
        if now is None:
            return
        print(f"\n{self.message(now)}", flush=True)
        if before_exit is not None:
            before_exit()
        sys.exit(EXIT_CODE_CHANGED)


# ── running writers ──────────────────────────────────────────────────────────

def _boot_time() -> float | None:
    try:
        for line in Path("/proc/stat").read_text().splitlines():
            if line.startswith("btime "):
                return float(line.split()[1])
    except OSError:
        pass
    return None


def _start_time(proc_dir: Path, boot: float) -> datetime | None:
    try:
        stat = (proc_dir / "stat").read_text()
    except OSError:
        return None
    fields = stat.rsplit(")", 1)[1].split()
    ticks = int(fields[19])  # field 22 overall; the first two precede ")"
    return datetime.fromtimestamp(boot + ticks / os.sysconf("SC_CLK_TCK"), tz=timezone.utc)


def reflog(repo: Path = REPO_ROOT) -> list[tuple[float, str]]:
    """(unix time, new HEAD) per HEAD reflog entry, oldest first."""
    git = _git_dir(Path(repo))
    out: list[tuple[float, str]] = []
    if git is None:
        return out
    try:
        lines = (git / "logs" / "HEAD").read_text(errors="replace").splitlines()
    except OSError:
        return out
    for line in lines:
        meta = line.split("\t", 1)[0].split()
        if len(meta) >= 5:
            try:
                out.append((float(meta[-2]), meta[1]))
            except ValueError:
                pass
    return out


def head_at(when: datetime, log: list[tuple[float, str]]) -> str | None:
    """The HEAD the checkout had at `when`, from the reflog; None before its first entry."""
    t = when.timestamp()
    head = None
    for ts, new in log:
        if ts > t:
            break
        head = new
    return head


def _script(argv: list[str], cwd: Path | None, repo: Path) -> str | None:
    """The repo-relative script of a python argv, or None when it isn't one of ours."""
    if not argv or not Path(argv[0]).name.startswith("python"):
        return None
    for arg in argv[1:]:
        if arg.startswith("-"):
            continue
        if not arg.endswith(".py"):
            return None
        path = Path(arg)
        if not path.is_absolute() and cwd is not None:
            path = cwd / path
        try:
            return str(path.resolve().relative_to(repo.resolve()))
        except ValueError:
            return None
    return None


def _writes_under(proc_dir: Path, data_root: Path) -> str | None:
    """A path under `data_root` this process holds open for writing, or has as cwd; else None.

    Only processes whose /proc entries this user may read can be seen; a
    snapshot also misses a writer between two writes.
    """
    root = str(data_root.resolve())
    try:
        cwd = os.readlink(proc_dir / "cwd")
        if cwd == root or cwd.startswith(root + os.sep):
            return cwd
    except OSError:
        pass
    try:
        fds = list((proc_dir / "fd").iterdir())
    except OSError:
        return None
    for fd in fds:
        try:
            target = os.readlink(fd)
        except OSError:
            continue
        if not target.startswith(root + os.sep):
            continue
        try:
            flags = next(line for line in (proc_dir / "fdinfo" / fd.name).read_text().splitlines()
                         if line.startswith("flags:"))
            if int(flags.split()[1], 8) & os.O_ACCMODE in (os.O_WRONLY, os.O_RDWR):
                return target
        except (OSError, StopIteration, ValueError):
            continue
    return None


def running_writers(repo: Path = REPO_ROOT, proc_root: Path = Path("/proc"),
                    data_root: Path | None = None) -> list[dict]:
    """Every running python process whose script is in `repo`, with the code it started on,
    and every other process seen writing under `data_root` (kind "foreign").

    `stale` is True when python code changed between the HEAD at the process's
    start and the current HEAD; None when either is unknown or the writer is foreign.
    """
    repo = Path(repo)
    if data_root is None:
        from utils.config import DATA_ROOT as data_root
    data_root = Path(data_root)
    boot = _boot_time()
    if boot is None:
        return []
    log = reflog(repo)
    current = git_head(repo)
    me = os.getpid()
    rows = []
    changed: dict[str, dict[str, bool]] = {}
    for entry in proc_root.iterdir():
        if not entry.name.isdigit() or int(entry.name) == me:
            continue
        pid = int(entry.name)
        try:
            argv = (entry / "cmdline").read_bytes().decode(errors="replace").split("\0")
            argv = [a for a in argv if a]
            try:
                cwd = Path(os.readlink(entry / "cwd"))
            except OSError:
                cwd = None
        except OSError:
            continue
        if not argv:
            continue
        script = _script(argv, cwd, repo)
        started = _start_time(entry, boot)
        if script is None:
            path = _writes_under(entry, data_root)
            if path is None:
                continue
            rows.append({"pid": pid, "kind": "foreign", "script": argv[0],
                         "args": " ".join(argv[1:])[:200],
                         "started_at": started.isoformat() if started else None,
                         "code_head": None, "current_head": current, "stale": None,
                         "writes": path})
            continue
        head = head_at(started, log) if started else None
        stale = None
        if head and current:
            if head not in changed:
                changed[head] = {}
            if script not in changed[head]:
                changed[head][script] = head != current and python_changed(
                    repo, head, current, [*SHARED_CODE, script])
            stale = changed[head][script]
        script_at = next(i for i, a in enumerate(argv) if a.endswith(".py"))
        rows.append({
            "pid": pid,
            "kind": "repo",
            "script": script,
            "args": " ".join(argv[script_at + 1:])[:200],
            "started_at": started.isoformat() if started else None,
            "code_head": head,
            "current_head": current,
            "stale": stale,
        })
    rows.sort(key=lambda r: r["started_at"] or "")
    return rows
