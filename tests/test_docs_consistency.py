"""Guards against the documentation drifting away from the code.

The 2026-09 review found the tool table duplicated across four files, already
inconsistent: `download_ibkr_index.py` appeared in exactly one of them, and the
crypto symbol counts in the agent skill disagreed with the ones in README.md.
Fixing that once achieves nothing on its own — the next tool drifts the same
way. These tests make the drift fail the build instead.

Deliberately narrow: they check that things are *mentioned*, not that prose is
accurate. A test that tried to verify wording would be rewritten every time
someone edited a sentence, and would be ignored within a month.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
AGENTS_MD = REPO_ROOT / "AGENTS.md"
SKILLS_DIR = REPO_ROOT / ".agent" / "skills"

#: Tools that are deliberately undocumented in the table.
EXEMPT_TOOLS = {
    "migrate_price_data_layout.py",  # one-off 2026-07 migration, kept for reference
}


def tool_scripts() -> list[Path]:
    return sorted(
        p for p in (REPO_ROOT / "tools").glob("*.py")
        if not p.name.startswith("_") and p.name not in EXEMPT_TOOLS
    )


def util_modules() -> list[Path]:
    return sorted(
        p for p in (REPO_ROOT / "utils").glob("*.py")
        if p.name != "__init__.py"
    )


def skill_dirs() -> list[Path]:
    return sorted(p for p in SKILLS_DIR.iterdir() if p.is_dir())


# --- the tool table -----------------------------------------------------------

def test_agents_md_is_the_real_file():
    """The lowercase stub must not be the one carrying the content."""
    assert AGENTS_MD.exists()
    stub = REPO_ROOT / "agents.md"
    if stub.exists():
        assert len(stub.read_text().splitlines()) < 20, (
            "agents.md should be a pointer stub; the content lives in AGENTS.md"
        )


def _table_entries(heading: str, prefix: str) -> set[str]:
    """Filenames listed in the markdown table under a given `## heading`.

    Parsed from the table rather than searched for anywhere in the file: a
    usage example elsewhere in the document would otherwise satisfy the check,
    and the test would pass while the table it names stayed wrong.
    """
    text = AGENTS_MD.read_text()
    start = text.index(f"## {heading}")
    rest = text[start + 1:]
    end = rest.find("\n## ")
    section = rest[: end if end != -1 else len(rest)]

    return set(re.findall(rf"^\|\s*`{re.escape(prefix)}([\w.]+)`", section, re.M))


def test_the_tool_table_is_parseable():
    """Guard the guard: if the table shape changes, fail loudly here rather
    than letting every membership test silently pass on an empty set."""
    assert len(_table_entries("Tools", "tools/")) >= 5
    assert len(_table_entries("Utilities (importable)", "utils/")) >= 5


@pytest.mark.parametrize("tool", tool_scripts(), ids=lambda p: p.name)
def test_every_tool_is_in_the_tool_table(tool):
    """A new tool that nobody documented is the drift this test exists to stop."""
    assert tool.name in _table_entries("Tools", "tools/"), (
        f"{tool.name} is not in the Tools table in AGENTS.md. Add a row there "
        f"(and a section in docs/cli-reference.md), or add it to EXEMPT_TOOLS "
        f"with a reason."
    )


@pytest.mark.parametrize("tool", tool_scripts(), ids=lambda p: p.name)
def test_every_tool_is_in_the_cli_reference(tool):
    reference = (REPO_ROOT / "docs" / "cli-reference.md").read_text()
    assert tool.name in reference, f"{tool.name} has no docs/cli-reference.md section"


@pytest.mark.parametrize("module", util_modules(), ids=lambda p: p.name)
def test_every_util_module_is_listed(module):
    assert module.name in _table_entries("Utilities (importable)", "utils/"), (
        f"utils/{module.name} is missing from the Utilities table in AGENTS.md"
    )


def test_every_tool_has_a_docstring_and_a_main():
    """`--help` is the tool's own documentation; it can't be allowed to be empty."""
    for tool in tool_scripts():
        source = tool.read_text()
        assert source.lstrip().startswith('#!') and '"""' in source, tool.name
        assert "def main(" in source, f"{tool.name} has no main()"


# --- skills -------------------------------------------------------------------

def test_skills_directory_is_not_empty():
    assert skill_dirs(), "no skills found under .agent/skills/"


@pytest.mark.parametrize("skill", skill_dirs(), ids=lambda p: p.name)
def test_skill_has_valid_frontmatter(skill):
    """Frontmatter is what makes a skill discoverable by any harness.

    The old flat `.agent/crypto_download.md` had none, so it was findable only
    by reading a table — which meant no harness could find it at all.
    """
    path = skill / "SKILL.md"
    assert path.exists(), f"{skill.name} has no SKILL.md"

    match = re.match(r"^---\n(.*?)\n---\n", path.read_text(), re.S)
    assert match, f"{skill.name}/SKILL.md has no YAML frontmatter block"

    meta = yaml.safe_load(match.group(1))
    assert meta.get("name") == skill.name, (
        f"frontmatter name {meta.get('name')!r} != directory {skill.name!r}"
    )
    description = meta.get("description", "")
    assert len(description) > 40, "description should say what it does AND when to use it"


def _skill_table_entries() -> set[str]:
    """Skill names in the first column of the Agent Skills table.

    Parsed, not searched: the skill's own file path appears in the second
    column, so a substring check over the whole document passes even when the
    name has been removed from the column that matters.
    """
    text = AGENTS_MD.read_text()
    start = text.index("## Agent Skills")
    rest = text[start + 1:]
    end = rest.find("\n## ")
    section = rest[: end if end != -1 else len(rest)]

    return {
        name.strip()
        for name in re.findall(r"^\|\s*([\w-]+)\s*\|", section, re.M)
        if name.strip() not in {"Skill", "File", "Description"}
    }


def test_the_skill_table_is_parseable():
    """Guard the guard — an unparseable table must fail here, not silently
    make every registration check pass against an empty set."""
    assert _skill_table_entries(), "could not parse the Agent Skills table"


@pytest.mark.parametrize("skill", skill_dirs(), ids=lambda p: p.name)
def test_skill_is_registered_in_agents_md(skill):
    assert skill.name in _skill_table_entries(), (
        f"{skill.name} is not in the Agent Skills table in AGENTS.md"
    )


# --- counts that go stale -----------------------------------------------------

HARDCODED_COUNT = re.compile(r"~\d{2,}\s*(?:symbols|pairs)", re.I)


@pytest.mark.parametrize(
    "doc",
    [p for p in [*(REPO_ROOT / ".agent").rglob("*.md"), *(REPO_ROOT / "docs").glob("*.md")]],
    ids=lambda p: str(p.relative_to(REPO_ROOT)),
)
def test_docs_do_not_hardcode_symbol_counts(doc):
    """Counts drift the moment anything is downloaded.

    The agent skill claimed ~392 binance symbols while README.md said 435.
    Point at `query.py --list-catalogs` instead of writing a number down.
    """
    found = HARDCODED_COUNT.findall(doc.read_text())
    assert not found, (
        f"{doc.name} hardcodes a symbol count {found}; these drift. "
        f"Reference `tools/query.py --list-catalogs` instead."
    )


# --- claims that contradicted the code ---------------------------------------

def test_use_rth_claim_matches_the_code():
    """AGENTS.md and workflows.md both claimed `useRTH=0` unconditionally while
    the downloader hardcoded RTH for daily bars. Keep them honest."""
    source = (REPO_ROOT / "tools" / "download_ibkr_stock_overnight.py").read_text()
    assert "--use-rth" in source

    for doc in (AGENTS_MD, REPO_ROOT / "docs" / "workflows.md"):
        text = doc.read_text()
        assert "useRTH=0" not in text or "--use-rth" in text, (
            f"{doc.name} still claims unconditional useRTH=0"
        )
