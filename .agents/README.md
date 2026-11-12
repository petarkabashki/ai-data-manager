# `.agents/`

- `library.yaml`: this project's agent-library opt-in (collections it uses).
- `library/skills/<name>/SKILL.md`: this project's own skills, harness-neutral, tracked.

`agent-library link --project . --apply` delivers library and project skills to every harness:
copies in `.agents/skills/`, symlinks in `.claude/skills/`. Those are untracked (listed in
`.git/info/exclude`); edit the source in `library/skills/`, never the delivered copy.
