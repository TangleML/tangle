# Agent notes

## Keep platform specifics out of the generic core

The orchestrator (`cloud_pipelines_backend/orchestrator_sql.py`) and the launcher
interface (`cloud_pipelines_backend/launchers/interfaces.py`) are
platform-agnostic. Do not put Kubernetes, HTTP, or cloud-SDK details in them.

Platform logic belongs in the concrete launcher (e.g. `kubernetes_launchers.py`),
which turns backend failures into typed `LauncherError`s. The orchestrator acts
on the typed error, never on a raw status code.
