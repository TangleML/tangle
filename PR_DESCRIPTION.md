## Add Ruff linter with CI enforcement

### Summary

- Add [Ruff](https://docs.astral.sh/ruff/) as a dev dependency and configure it in `pyproject.toml` with rules for pycodestyle, pyflakes, import sorting, pyupgrade, flake8-bugbear, flake8-simplify, and ruff-specific checks
- Fix all 222 linting violations across the codebase (156 auto-fixed, 66 manually resolved)
- Add a `lint` CI job that runs `ruff check .` on every push alongside the existing test matrix
- Move misplaced module-level imports to the top of files per PEP 8

### What changed

**Linter config (`pyproject.toml`):**
- Rule groups enabled: `E`, `W`, `F`, `I`, `UP`, `B`, `SIM`, `RUF`
- Rules suppressed for SQLAlchemy compatibility: `E711`/`E712` (`== None` and `== True` are required for SQL generation), `RUF012` (mutable class defaults on ORM models)
- Per-file ignores for `start_local.py` (intentional section-based import pattern) and `tests/` (common test patterns)

**Bug fixes:**
- 8 bare `except:` blocks changed to `except Exception:` — prevents accidentally swallowing `KeyboardInterrupt`/`SystemExit`
- 4 lambda functions in dict comprehensions fixed to bind loop variables via default args, preventing potential late-binding bugs
- 1 mutable `ContextVar` default (`default={}`) replaced with a lazy-init helper to avoid shared state across contexts

**Code quality improvements:**
- Import sorting and grouping enforced across all files (stdlib / third-party / first-party)
- Module-level imports moved to file tops (previously scattered mid-file in several modules)
- 5 unused variables removed
- Modernized type annotations: `Optional[X]` to `X | None`, `List`/`Dict` to `list`/`dict`
- Cleaned up f-strings missing placeholders, redundant open modes, unnecessary encode args, `yield` over for-loop replaced with `yield from`

**CI (`.github/workflows/run_tests_on_push.yaml`):**
- New `lint` job runs `ruff check .` in parallel with existing test jobs

### Test plan

- [x] `uv run ruff check .` passes with zero violations
- [x] Existing tests pass (34/34 on local, 2 test files skipped due to pre-existing local env module resolution issue unrelated to this change)
- [ ] CI pipeline passes on push
