# Tangle Codebase: Contribution Readiness Plan

**Date:** February 24, 2026
**Status:** Proposal

---

## Overview

This document outlines gaps in the Tangle codebase that make it difficult for new contributors to ramp up and maintain consistent quality. The goal is to bring the project up to modern Python standards so that developers can contribute confidently with proper guardrails in place.

Tangle is a ~10K-line Python 3.10+ FastAPI backend using SQLAlchemy and uv. While the core functionality is solid, the project is missing most of the tooling and infrastructure that developers expect in a modern open-source project.

---

## Current State

### What exists today

- **Package management:** uv with `pyproject.toml` and `uv.lock`
- **Framework:** FastAPI with SQLAlchemy ORM
- **Testing:** 7 pytest test files (~1,400 lines), primarily covering instrumentation and the component library API
- **CI:** A single GitHub Actions workflow that runs `pytest` on push to master/stable/dev
- **Docs:** README, CONTRIBUTING.md, GOVERNANCE.md, SECURITY.md, RELEASING.md

### What's missing

| Category | Status |
|----------|--------|
| Linting (ruff, flake8, pylint) | None configured |
| Formatting enforcement (black, ruff format) | black is a dependency but not configured or enforced |
| Type checking (mypy, pyright) | None configured |
| Import sorting (isort, ruff) | None configured |
| Pre-commit hooks | None |
| Database migrations (Alembic) | None — uses `metadata.create_all()` with manual index creation |
| Test fixtures / conftest.py | None |
| Test coverage reporting | None |
| CI on pull requests | Not triggered — only runs on push to specific branches |
| CI linting / type checking | Not included |
| `.env.example` | None — env vars are undocumented |
| GitHub issue/PR templates | Referenced in CONTRIBUTING.md but don't exist |
| Dependabot | Not configured |
| Dockerfile / docker-compose | None |

---

## Key Issues in Detail

### 1. No Linting or Static Analysis

There is no linter configured anywhere. `black` is listed as a dev dependency but there's no configuration for it in `pyproject.toml` and nothing enforces its use. Contributors have zero guidance on code style and no automated checks to catch common mistakes.

**Impact:** Inconsistent code style, avoidable bugs slip through, PR reviews become style debates.

### 2. No Type Checking

No type checker (mypy, pyright) is configured. Many functions — especially in `orchestrator_sql.py` and `database_ops.py` — are missing return type annotations. Nobody has ever run a type checker against this codebase.

**Impact:** Type errors are only caught at runtime. Refactoring is risky without type safety.

### 3. No Database Migration Tooling

The current approach in `database_ops.py` is:

1. Call `metadata.create_all()` to create tables from ORM models
2. Manually create specific indexes in `migrate_db()`

There is no Alembic setup, no migration versioning, and no rollback capability. The `migrate_db()` function is mostly commented-out code. A contributor adding a new model field would have no idea how to handle the schema change.

**Impact:** Schema changes are untracked and irreversible. Multi-developer work on models will cause conflicts.

### 4. No Pre-commit Hooks

There is no `.pre-commit-config.yaml`. Nothing prevents a contributor from pushing unformatted code, lint violations, or type errors.

**Impact:** Quality enforcement is entirely manual and relies on PR reviewers catching everything.

### 5. Minimal Test Infrastructure

- No `conftest.py` for shared fixtures (database setup, test client, etc.)
- No pytest configuration in `pyproject.toml`
- No coverage reporting
- Estimated test coverage is well under 20%
- Core API endpoints (`api_server_sql.py` — 1,562 lines) have only 58 lines of tests

**Impact:** Contributors can't easily write tests. Regressions go undetected.

### 6. CI Only Runs Tests on Push

The GitHub Actions workflow only triggers on push to `master`, `stable`, and `dev`. It does not run on pull requests, does not check linting or formatting, and does not report coverage.

**Impact:** PRs are not validated before merge. Issues are only caught after code lands on main branches.

### 7. Undocumented Environment Variables

The codebase uses several environment variables with no documentation:

- `TANGLE_OTEL_EXPORTER_ENDPOINT`
- `TANGLE_OTEL_EXPORTER_PROTOCOL`
- `TANGLE_ENV`
- `CLOUD_PIPELINES_BACKEND_DATA_DIR`
- `TANGLE_BACKEND_DATA_DIR`

There is no `.env.example` file and no validation at startup.

**Impact:** New contributors have to read the source code to figure out configuration.

### 8. Error Handling Gaps

- `errors.py` defines only 3 bare exception classes with no docstrings or error codes
- `PermissionError` shadows Python's built-in `PermissionError`
- Bare `except:` clauses exist in `orchestrator_sql.py` (catches everything silently)
- No centralized FastAPI exception handlers for structured error responses

**Impact:** Debugging is harder than it needs to be. Errors may be swallowed silently.

### 9. Large Files

Several files are over 1,000 lines with multiple concerns mixed together:

| File | Lines | Notes |
|------|-------|-------|
| `api_server_sql.py` | 1,562 | Multiple service classes in one file |
| `kubernetes_launchers.py` | 1,429 | All K8s launcher variants in one file |
| `orchestrator_sql.py` | 1,061 | Monolithic orchestrator |

**Impact:** Hard to navigate, review, and modify without unintended side effects.

### 10. Missing GitHub Configuration

CONTRIBUTING.md links to issue templates (`bug_report.md`, `feature_request.md`) that don't exist. There's no PR template and no dependabot configuration.

**Impact:** Inconsistent issue/PR quality. Dependencies go stale.

---

## Proposed Plan

### Phase 1: Developer Tooling

**Goal:** Every contributor gets immediate feedback on code quality.

- [ ] Add **ruff** as the linter and import sorter (replaces flake8 + isort in a single, fast tool)
- [ ] Configure ruff in `pyproject.toml` with sensible defaults
- [ ] Add **mypy** for type checking with gradual strictness (start with `--ignore-missing-imports`)
- [ ] Add **pytest-cov** for coverage reporting
- [ ] Add pytest configuration to `pyproject.toml` (asyncio mode, test paths, markers)
- [ ] Create **`.pre-commit-config.yaml`** with ruff, ruff-format, and mypy hooks
- [ ] Run ruff auto-fix across the codebase and fix remaining issues
- [ ] Add missing return type annotations to public functions

### Phase 2: Database Migrations

**Goal:** Schema changes are versioned, trackable, and reversible.

- [ ] Add **Alembic** as a dependency
- [ ] Initialize Alembic and configure it to use the existing SQLAlchemy models
- [ ] Generate a baseline migration from current models
- [ ] Replace `metadata.create_all()` with Alembic's `upgrade head`
- [ ] Document the migration workflow for contributors

### Phase 3: CI/CD Hardening

**Goal:** Every PR is validated before merge.

- [ ] Add `pull_request` trigger to the GitHub Actions workflow
- [ ] Add a **linting job** (`ruff check`)
- [ ] Add a **formatting job** (`ruff format --check`)
- [ ] Add a **type checking job** (`mypy`)
- [ ] Add **coverage reporting** (upload results, optionally enforce a minimum)

### Phase 4: Testing & Error Handling

**Goal:** Contributors have good testing patterns to follow and errors are handled properly.

- [ ] Create `tests/conftest.py` with shared fixtures (in-memory DB, test FastAPI client, etc.)
- [ ] Replace bare `except:` clauses with specific exception types
- [ ] Rename `PermissionError` to avoid shadowing the built-in
- [ ] Add FastAPI exception handlers for structured error responses
- [ ] Add tests for core API endpoints (pipeline CRUD, execution lifecycle)
- [ ] Set a coverage target (suggest starting at 40% and increasing over time)

### Phase 5: Documentation & Developer Experience

**Goal:** A new contributor can go from clone to running in under 5 minutes.

- [ ] Create **`.env.example`** documenting all environment variables
- [ ] Add **GitHub issue templates** (bug report, feature request)
- [ ] Add a **PR template** with a review checklist
- [ ] Add **`dependabot.yml`** for automated dependency updates
- [ ] Add a **Dockerfile** and **docker-compose.yml** for one-command local development
- [ ] Improve inline documentation for complex business logic

---

## Effort Estimates

| Phase | Effort | Dependencies |
|-------|--------|-------------|
| Phase 1: Developer Tooling | 2–3 days | None |
| Phase 2: Database Migrations | 1–2 days | None |
| Phase 3: CI/CD Hardening | 0.5–1 day | Phase 1 |
| Phase 4: Testing & Error Handling | 3–5 days | Phase 1 |
| Phase 5: Documentation & DX | 1–2 days | None |

Phases 1, 2, and 5 can be worked on in parallel. Phase 3 depends on Phase 1 (need linting configured before CI can check it). Phase 4 benefits from Phase 1 but can start in parallel.

---

## Recommendation

Start with **Phase 1** (developer tooling). It's the foundation for everything else and gives the highest immediate return — every contributor gets automated feedback from day one. Phase 2 (migrations) should follow closely since the current approach to schema management will cause problems as soon as multiple people are working on models.

---

## Questions for the Team

1. **Ruff vs. Black + flake8 + isort:** Ruff replaces all three in a single tool with 10–100x speed. Any objections to consolidating?
2. **Mypy strictness:** Start lenient and tighten over time, or go strict from the beginning?
3. **Coverage target:** What minimum coverage percentage should we enforce in CI?
4. **Alembic:** Any concerns about adopting Alembic? Are there existing databases in production that need a migration path?
5. **Docker:** Should local development require Docker, or should we keep the current `uv run` approach as the primary path?
