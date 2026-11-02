@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo Активация виртуального окружения...
if exist venv\Scripts\activate.bat (
    call venv\Scripts\activate.bat
)

echo Запускаю Mock-сервер 1С в отдельном окне...
start "Mock 1C Server" cmd /k "venv\Scripts\python.exe -m uvicorn mock_server.main:app --host 127.0.0.1 --port 8080 --reload"

echo Ждем 2 секунды и запускаем Telegram-бота...
timeout /t 2 /nobreak >nul
venv\Scripts\python.exe -m app.main
pause