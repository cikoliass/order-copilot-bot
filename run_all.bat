@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo ====================================================
echo    B2B HORECA ORDER CO-PILOT ENTERPRISE
echo ====================================================
echo.

if not exist venv\Scripts\python.exe (
    echo [ERROR] Virtual environment venv not found!
    echo Run: python -m venv venv and pip install -r requirements.txt
    pause
    exit /b 1
)

echo [1/2] Starting Mock 1C server on port 8080...
start "Mock 1C Gateway" venv\Scripts\python.exe -m uvicorn mock_server.main:app --host 127.0.0.1 --port 8080

echo [2/2] Waiting 2 seconds for 1C to initialize...
ping 127.0.0.1 -n 3 >nul

echo.
echo ====================================================
echo  * 1C Jury Screen:   http://localhost:8080/orders
echo  * Swagger API Docs: http://localhost:8001/docs
echo  * Healthcheck:      http://localhost:8001/health
echo ====================================================
echo.
echo Starting FastAPI Order Co-Pilot on port 8001...
echo.

venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8001 --reload
pause
