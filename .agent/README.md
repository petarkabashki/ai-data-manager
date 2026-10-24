# `.agent/`

Harness-neutral agent assets for this repo. Layout matches the sibling
`py-qwant` repo, so the two are navigable the same way.

```
.agent/
└── skills/<name>/SKILL.md    # one directory per skill, frontmatter first
```

## Skills

Each `SKILL.md` opens with YAML frontmatter carrying `name` and `description`.
The frontmatter is what makes a skill **discoverable**: a harness can scan this
directory and know what each skill is for, instead of depending on someone
having read the table in `AGENTS.md`.

The body is plain markdown with no harness-specific directives, so the same
file serves Claude Code, Codex, or anything else that reads a skill folder.

## Wiring it to a harness

Claude Code reads `.claude/skills/`, Codex reads `.codex/skills/`. Point them
here rather than keeping copies that drift:

```bash
ln -s ../.agent/skills .claude/skills
ln -s ../.agent/skills .codex/skills
```

## Adding a skill

1. `.agent/skills/<name>/SKILL.md`, frontmatter first:

   ```yaml
   ---
   name: <name>
   description: What it does, and when to reach for it — a harness matches
     against this, so name the triggers explicitly.
   ---
   ```

2. Body: the tool chain, the standard invocations, the rules that apply to
   every write, and the gotchas. Concrete commands beat prose.
3. Add a row to the Agent Skills table in [`AGENTS.md`](../AGENTS.md).

`tests/test_docs_consistency.py` fails if a skill is missing frontmatter or
absent from that table.
