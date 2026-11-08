---
name: pr-review
description: "Use when: reviewing a pull request, reviewing a diff, giving PR feedback, checking a branch before merge, or preparing a pre-merge briefing (\"brief me\", \"is this ready to merge\", checking Copilot comments and CI) in telecom-lakehouse-streaming-platform. Produces a short review sorted into blocking, suggestions and nits with a verdict, or a briefing table of every finding and CI result."
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

## Pre-merge briefing
Use when asked to prepare a PR for merge. Read-only; post nothing without approval.
1. Collect: `gh pr view <n> --json state,mergeable,mergeStateStatus`, `gh pr checks <n>`,
   all reviews (`gh api repos/<owner>/<repo>/pulls/<n>/reviews`), inline comments
   (`.../pulls/<n>/comments`) and conversation comments (`.../issues/<n>/comments`).
   Read Copilot's full review text, including findings listed as "previously missed".
2. Put every finding in one of three states: fixed (name the commit), deferred (link
   the issue; create one if none exists), or disagreed (one-line reason). Never leave
   a finding unclassified.
3. Output the CI result and merge state, then one table (Item, Source, Status), then
   "What to check in the diff": the files and what changed in each.
4. If CI is failing or a finding is unclassified, say the PR is not ready.
