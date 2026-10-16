@echo off
title SportZone Dashboard
cd /d "%~dp0"

echo.
echo  ==========================================
echo   SportZone Data Engineering Platform
echo  ==========================================
echo.

REM Activate virtual environment
if exist "..\\.venv\\Scripts\\activate.bat" (
    call "..\\.venv\\Scripts\\activate.bat"
) else if exist ".venv\\Scripts\\activate.bat" (
    call ".venv\\Scripts\\activate.bat"
)

REM Get local IP
for /f "tokens=2 delims=:" %%a in ('ipconfig ^| findstr /i "IPv4" ^| findstr /v "127.0.0.1"') do (
    set LOCAL_IP=%%a
    goto :found_ip
)
:found_ip
set LOCAL_IP=%LOCAL_IP: =%

echo  Local:    http://localhost:8501
echo  Network:  http://%LOCAL_IP%:8501
echo.
echo  Press Ctrl+C to stop
echo.

python -m streamlit run dashboard/app.py ^
    --server.port 8501 ^
    --server.address 0.0.0.0 ^
    --server.headless false ^
    --server.enableCORS false ^
    --server.enableXsrfProtection false

pause
