@echo off
setlocal EnableDelayedExpansion

REM Set the backend data dir to $(pwd)/data
set "CLOUD_PIPELINES_BACKEND_DATA_DIR=%CD%\data"

REM `cd $(dirname) $0`
cd /d "%~dp0"

REM ! The app does not work when accessed via http://0.0.0.0:8000 (but http://127.0.0.1:8000 or https://* work).
REM We can either use `fastapi run --host 127.0.0.1` or `fastapi dev` to make the correct URL show up in the log.
call uv run --frozen fastapi dev start_local.py %*

exit /b %ERRORLEVEL%
