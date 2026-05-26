@echo off
title Aegis Observability Platform Backend
echo ======================================================================
echo 🛡️  Aegis -- AI-Native Incident Correlation & Observability Platform
echo ======================================================================
echo.

:: Check Python installation
where python >nul 2>nul
if %errorlevel% neq 0 (
    echo [ERROR] Python is not installed or not in your system PATH.
    echo Please install Python 3.9+ to run the backend.
    pause
    exit /b 1
)

echo [1/2] Installing requirements...
python -m pip install -r requirements.txt --quiet
if %errorlevel% neq 0 (
    echo [WARNING] Dependency installation encountered warnings/errors. Proceeding anyway...
)

echo.
echo [2/2] Starting Aegis FastAPI Server on http://127.0.0.1:8000
echo.
python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
if %errorlevel% neq 0 (
    echo.
    echo [ERROR] Uvicorn failed to start.
    pause
)
