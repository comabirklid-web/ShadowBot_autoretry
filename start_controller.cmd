@echo off
setlocal
set "ROOT=%~dp0"

where pythonw >nul 2>nul
if errorlevel 1 (
  echo Python 3.11 was not found on PATH. Install Python and run: python -m pip install -r requirements.txt
  pause
  exit /b 1
)

if not exist "%ROOT%retry_config.json" (
  echo Missing retry_config.json.
  echo Copy retry_config.example.json to retry_config.json and edit the two ShadowBot paths first.
  pause
  exit /b 1
)

python -c "import pywinauto" >nul 2>nul
if errorlevel 1 (
  echo Missing dependency. Run: python -m pip install -r requirements.txt
  pause
  exit /b 1
)

start "ShadowBot Retry Controller" /b pythonw "%ROOT%retry_controller_service.py"
echo Controller start requested. Logs: %ROOT%logs\retry_controller.log
timeout /t 2 /nobreak >nul
