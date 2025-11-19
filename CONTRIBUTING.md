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

#### Manual Step: Run CI (ci.yml)

Run the `ci.yml` workflow manually on your feature branch.
It validates:

* test execution
* test coverage
* linting (if enabled)
* typing (if enabled in this phase)

Fix any issues until the workflow passes.

#### When ready → Open a Pull Request

Merge feature branches into `main` through a PR.

> *Developers are encouraged (but not required at this stage) to update documentation relevant to their feature while developing.*

---

### 2. Release Preparation (Release Branch)

Once all features are merged and you want to prepare a release, create a dedicated release branch:

```
git checkout -b release/vX.Y.Z
```

#### Manual Step: Build & Validate Documentation (docs.yml)

Run the `docs.yml` workflow on the release branch:

* Sphinx build
* linkcheck
* spelling
* API reference generation
* any warnings promoted to errors

Fix any doc issues and re-run until clean.

Documentation polishing should be finished before releasing.

---

### 3. Finalize Release (release.yml)

Trigger `release.yml` manually on the release branch.

This workflow performs:

* version bump via Commitizen
* changelog update
* tagging
* packaging & validation
* optional PyPI upload (if enabled)

Fix any issues and re-run as needed.

---

### 4. Merge Release Branch → Main

Once the release branch is validated and tagged:

1. Open a “Release PR” into `main`
2. Merge after review (or after re-running CI)
3. Optionally trigger docs deployment to GitHub Pages (if configured)

At this point, the release is complete and the documentation on `main` is up to date.

---

## Documentation Guidelines

Documentation is a core part of the project and evolves alongside code.

### Where docs should be updated

* Feature-specific docs → during the feature PR if possible
* Final polishing → in the release branch during step 2
* Final deployment → after merging release branch into `main`

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