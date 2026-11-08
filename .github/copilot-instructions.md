# Copilot / AI instructions — telecom-lakehouse-streaming-platform

Read `README.md` and the linked issue first. `README.md` holds the architecture,
design decisions and pinned versions; issues and milestones are the phase plan.

## Workflow
- `main` is always working. Work on a branch (`feat/...`, `fix/...`, `docs/...`,
  `chore/...`) and merge through a pull request. No direct pushes to `main`.
- Commits follow Conventional Commits (`feat: add HDFS and YARN compose file`).
- One task = one issue = one PR. The PR body says `Closes #<issue>`.
- Squash-merge. Delete the branch after merging.
- Keep `README.md` in step with what the PR changes.

## Code rules
- Never commit secrets, `.env`, or data and volume folders.
- Jobs must be idempotent: a rerun gives the same result and never duplicates rows.
- Pin image and package versions; record them in `README.md`.
- Comment only what the code can't show; keep comments to one short line.

## Code review (applies to Copilot's reviewer and the `pr-reviewer` agent)
This is a solo portfolio project, so review for real problems, not perfection.

**Blocking** (must fix before merge):
- Bugs, or a stack/job that won't start or rerun.
- Secrets, tokens or personal data (real phone numbers) in the diff.
- Breaks the design questions in `README.md`: reruns/idempotency, duplicate
  writes, late data, safe backfill, behaviour at 100x volume.
- Unpinned or unrelated dependencies; change unrelated to the linked issue.

**Suggestions** (worth doing, don't block): clarity, naming, missing test or
doc for new behaviour, simpler approach.

**Nits** (ignore unless trivial): style and formatting.

Do not request changes for nits alone. Say what is good, too.
