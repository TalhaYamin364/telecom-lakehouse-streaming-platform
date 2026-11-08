---
name: pr-review
description: "Use when: reviewing a pull request, reviewing a diff, giving PR feedback, or checking a branch before merge in telecom-lakehouse-streaming-platform. Produces a short review sorted into blocking, suggestions and nits, with a verdict."
---

# PR review

Review rules live in `.github/copilot-instructions.md` under "Code review". Read
them first. This skill gives the steps and the output format.

## Steps
1. Get the PR: description, linked issue, and the diff (`gh pr view <n>`,
   `gh pr diff <n>`).
2. Check the PR does what the linked issue asks, and nothing unrelated.
3. Check the diff against the blocking list in `copilot-instructions.md`.
4. For compose, Spark or Kafka changes, confirm how it was run and that a rerun
   is safe. If the author didn't say, ask; don't assume it works.
5. Check `README.md` was updated if the PR changes architecture, versions or
   design answers.

## Output format
```
Verdict: Approve | Approve with suggestions | Changes requested

Good: <1-3 short points>

Blocking
- <file:line> <problem> — <why> — <fix>

Suggestions
- <file:line> <suggestion>

Nits
- <file:line> <nit>
```
Leave a section out if empty. "Changes requested" only if Blocking is non-empty.
Keep the whole review short; skip anything you are not confident about.
