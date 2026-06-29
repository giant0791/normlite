# Cutting a release

This guide describes how to publish a new release of **normlite** using the
project's CD pipeline. The pipeline is fully automated through GitHub Actions:
you trigger one workflow, review the pull request it opens, and merge it. Tagging
and documentation deployment happen on their own.

## What the pipeline does

Releases are driven by [Commitizen](https://commitizen-tools.github.io/commitizen/)
on top of [conventional commits](https://www.conventionalcommits.org/). The
version is derived from the commit history — you never edit the version number by
hand.

Relevant `pyproject.toml` configuration:

```toml
[tool.commitizen]
name = "cz_conventional_commits"
tag_format = "$version"          # tags are bare, e.g. 0.11.0 (no "v" prefix)
version_scheme = "pep440"
version_provider = "uv"
update_changelog_on_bump = true
major_version_zero = true        # breaking changes bump the minor while 0.x
```

Three workflows cooperate:

| Workflow | File | Trigger | Role |
|----------|------|---------|------|
| **CI** | `.github/workflows/ci.yml` | push / PR to `main` | Quality gate: tests, coverage, docs build |
| **Release** | `.github/workflows/release.yml` | manual (`workflow_dispatch`) | Bumps version, writes changelog, opens the release PR |
| **Tag and Deploy** | `.github/workflows/tag-and-deploy.yml` | merge of a `release/v*` PR | Re-points the tag to the merge commit, deploys docs |

> **Note:** the pipeline does **not** publish to PyPI. It produces a git tag, an
> updated `CHANGELOG`, and a deployed documentation site. Distribution to a package
> index, if needed, is a separate manual step.

## Prerequisites

- Everything you want in the release is **merged into `main`** as
  [conventional commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`,
  `docs:`, `refactor:`, etc.). The commit types determine the version bump.
- CI is green on `main`.
- You have permission to run workflows in the repository.

While the project is on `0.x` (`major_version_zero = true`):

- `fix:` → patch bump (e.g. `0.10.0` → `0.10.1`)
- `feat:` → minor bump (e.g. `0.10.0` → `0.11.0`)
- A breaking change (`feat!:` or a `BREAKING CHANGE:` footer) → **minor** bump,
  not major, until the project reaches `1.0.0`.

If you want to preview the next version locally before triggering anything:

```bash
uv run cz bump --dry-run --yes
```

## Step 1 — Trigger the Release workflow

1. Go to the repository's **Actions** tab on GitHub.
2. Select the **Release** workflow.
3. Click **Run workflow**, making sure the branch is **`main`** (the workflow
   aborts if triggered from any other branch).
4. Run it.

The workflow then, automatically:

1. Verifies it is running from `main`.
2. Computes the next version from the commit history (`cz bump --dry-run`).
3. Creates a `release/v<version>` branch.
4. Runs `cz bump --yes`, which updates the version in `pyproject.toml`, updates
   the changelog, and creates the version tag locally.
5. Pushes the branch and tag.
6. Opens a pull request titled `release: v<version>` against `main`.

## Step 2 — Review the release PR

Open the pull request the workflow created and check:

- The **version bump** in `pyproject.toml` is what you expected.
- The **changelog** entries are correct and readable.
- CI on the PR is green.

If something is wrong (e.g. a commit was mis-typed and produced the wrong bump),
**close the PR, delete the `release/v<version>` branch and tag**, fix the commit
history on `main`, and re-run the Release workflow.

## Step 3 — Merge the release PR

Merging the PR into `main` triggers **Tag and Deploy**, which:

1. Re-points the version tag to the merge commit (so the tag lands on `main`,
   not on the now-merged release branch).
2. Builds the Sphinx documentation.
3. Deploys it to GitHub Pages (the `github-pages` environment).

Once this workflow finishes, the release is complete: the tag exists on `main`
and the published documentation reflects the new version.

## Troubleshooting

- **Release workflow aborts immediately** — it was not triggered from `main`.
  Re-run with `main` selected.
- **"No commits found" / no version bump** — there are no release-worthy
  conventional commits since the last tag. Ensure your changes use `feat:` /
  `fix:` (etc.) commit messages.
- **Wrong version computed** — a commit message used the wrong type. Verify with
  `uv run cz bump --dry-run --yes`, fix the history on `main`, and re-run.
- **Tag/branch left over from an aborted attempt** — delete the remote branch
  and tag before retrying:

  ```bash
  git push origin --delete release/v<version>
  git push origin :refs/tags/<version>
  ```

- **Docs didn't update after merge** — check the **Tag and Deploy** run; the
  `deploy-docs` job deploys to the `github-pages` environment. A standalone
  docs-only deploy is also available via the manual **Build and Deploy
  Documentation** workflow (`.github/workflows/docs.yml`).
