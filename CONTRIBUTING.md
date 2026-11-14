> **Status:**
> This project is currently in an early stage and has **no external contributors yet**.
> The workflow described below serves as a **personal checklist for the maintainer**,
> but it is written with future contributors in mind.

Thank you for your interest in **normlite**!
Even though the project is still evolving, this page documents the development and release workflow so that the process is transparent, reproducible, and easy to adopt as the project grows.

As soon as the project matures, this guide will serve as the foundation for an open and welcoming contributor experience.

---

## Development Philosophy

* Keep the contribution workflow **simple, predictable, and automatable**.
* Use **small, self-contained branches** for new features and fixes.
* Validate every branch using lightweight CI to maintain code quality.
* Prepare releases through a clean and structured process to ensure confidence and repeatability.

---

## Tooling Overview

The project uses the following development tools:

* **uv** — dependency & environment manager
* **pytest** + **pytest-cov** — testing & coverage
* **ruff** — linting (incrementally introduced)
* **ty** — static type checking (incrementally introduced)
* **commitizen (cz)** — semantic versioning, changelog, release automation
* **Sphinx** — documentation engine
* **GitHub Actions** — CI/CD automation for testing, docs, and releases

---

## Branch & CI/CD Workflow

The workflow is intentionally simple to reduce friction (currently a one-person project) but structured enough to scale when contributors join.

Below is the complete flow used for preparing and releasing new versions.

---

### 1. Feature & Fix Development

**Branch from `main`:**

```
git checkout -b feature/my-feature
```

#### CI runs on the pull request (ci.yml)

You do not start `ci.yml` by hand. It runs on each pull request to `main`
and on each push to `main`. It does these checks:

* test execution
* test coverage (the workflow posts a comment on the PR)
* Sphinx documentation build

Lint (`ruff`) and type checks (`ty`) are not in CI yet.
They are not in the project dependencies yet.

> GitHub runs workflows only on refs that are on the remote.
> Push your branch (`git push -u origin <branch>`) before you open the PR.

#### When ready → Open a Pull Request

Merge feature branches into `main` through a PR.

Before you merge, make sure the remote branch has all your local commits.
`gh pr merge` merges the **remote** tip, not your local `HEAD`:

```bash
git fetch
git log origin/<branch>..<branch>   # must print nothing
```

> *Developers are encouraged (but not required at this stage) to update documentation relevant to their feature while developing.*

---

### 2. Release

The full procedure is in `docs/releasing.md` ("Cutting a release" in the documentation site).
This section is a summary.

**Do not create the release branch yourself. Do not create the tag yourself.**
The `release.yml` workflow creates the `release/vX.Y.Z` branch.
The `tag-and-deploy.yml` workflow creates the tag on `main`.

Commitizen calculates the version from the conventional commits since the
last tag. You do not select the version. Tags are bare (`0.14.0`, no `v`).

#### 2.1 Prepare `main`

1. Merge all the work for the release into `main`.
   Close or move the open issues of the release milestone.
2. Make sure CI on `main` is green.
3. Make sure all workflow edits (for example, action version bumps) are on
   `main`. `tag-and-deploy.yml` runs the workflow files from `main`.
4. Preview the version and the changelog:

   ```bash
   git checkout main && git pull
   uv run cz bump --dry-run --yes
   ```

   If the version is not the milestone version, stop.
   Find the commit with the wrong type before you continue.

#### 2.2 Start the Release workflow (release.yml)

Start it on `main`. The workflow stops on all other branches.

```bash
gh workflow run release.yml --ref main
gh run watch
```

Or: **Actions → Release → Run workflow → Branch: main**.

The workflow does these steps:

* calculates the next version (`cz bump --dry-run`)
* creates the `release/vX.Y.Z` branch on the remote
* updates the version in `pyproject.toml` and adds the release to `CHANGELOG.md`
* opens the PR `release: vX.Y.Z` into `main`

#### 2.3 Review the release PR

* Check the version in `pyproject.toml`.
* Check the new `CHANGELOG.md` section. Its newest entry must be your newest commit.
* Wait for CI on the PR. You may have to approve the workflow run.

If the PR is wrong: close it, delete the `release/vX.Y.Z` branch, fix `main`,
and start the Release workflow again. No remote tag exists yet.

#### 2.4 Merge the release PR (tag-and-deploy.yml)

Merge with a **merge commit**. Do not squash. Do not rebase.

```bash
gh pr merge <PR#> --merge
```

The merge starts `tag-and-deploy.yml`. It does these steps:

* creates the tag `X.Y.Z` on the merge commit and pushes it
* builds the Sphinx documentation
* deploys it to GitHub Pages

#### 2.5 Verify

```bash
git checkout main && git pull --tags
git tag --sort=-creatordate | head -1   # X.Y.Z
gh run list --workflow tag-and-deploy.yml --limit 1
```

Then close the release milestone.

The pipeline does **not** upload to PyPI.

#### Documentation-only deploy (docs.yml)

`docs.yml` builds the documentation and deploys it to GitHub Pages.
It does no linkcheck and no spelling check. Use it only to deploy the
documentation again without a release:

```bash
gh workflow run docs.yml --ref main
```

---

## Documentation Guidelines

Documentation is a core part of the project and evolves alongside code.

### Where docs should be updated

* Feature-specific docs → during the feature PR if possible
* Final polishing → in a normal PR to `main`, before step 2.2
* Final deployment → automatic, when the release PR merges into `main`

### Build system

The docs are kept in the `docs/` folder.
Automated builds ensure the published docs always reflect the latest version on `main`.

No separate long-lived documentation branches are used.

---

## Testing Guidelines

Normlite aims for reliable testing with:

* **pytest** for test execution
* **pytest-cov** to measure coverage

Tests should be:

* small
* deterministic
* independent
* written next to the code they test where practical

---

## Code Quality

The project uses modern Python tooling, introduced in phases:

* **ruff** for linting
* **ty** for static typing
* **cz commit** for commit formatting

Linting and typing may be optional early on but will become required as the project matures.

---

## Roadmap for Future Contributors

Once the project becomes ready for external contributors:

* This guide will be simplified to reflect only the contributor-facing parts
* A dedicated “How to contribute” section will be added
* Issue templates and PR templates will be fully opened
* Contribution boundaries, expectations, and code-of-conduct will be clarified

---

## Thank You

Even though external contributions are not enabled yet, your interest in the project matters.
This guide helps maintain a clean, professional workflow — and serves as the foundation for a healthy contributor ecosystem in the future.

If you have ideas or suggestions, feel free to open a discussion or issue!