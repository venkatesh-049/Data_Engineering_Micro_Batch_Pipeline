@echo off
setlocal enabledelayedexpansion

REM Move to script directory so relative paths are stable.
cd /d "%~dp0"

echo =============================================
echo Data Pipeline Full Workflow Launcher
echo =============================================
echo Working directory: %CD%

echo.
echo [1/5] Generate sample batches...
python.exe run_pipeline.py --generate --reset-data --batches 2 --batch-size 100
if errorlevel 1 goto :fail

echo.
echo [2/5] Process batches...
python.exe run_pipeline.py --process
if errorlevel 1 goto :fail

echo.
echo [3/5] Run tests...
python.exe -m pytest -v tests/test_pipeline.py
if errorlevel 1 goto :fail

echo.
echo [4/5] Validate expected outputs...
python.exe scripts/validate_outputs.py
if errorlevel 1 goto :fail

echo.
echo [5/5] Generate operator dashboard...
python.exe scripts/operator_dashboard.py
if errorlevel 1 goto :fail

echo.
echo Workflow completed successfully.

echo.
echo Opening generated reports...
if exist "reports\validation_report.json" start "Validation Report" "reports\validation_report.json"
if exist "reports\operator_dashboard.json" start "Operator Dashboard" "reports\operator_dashboard.json"

echo.
echo Done. Press any key to close.
pause >nul
exit /b 0

:fail
echo.
echo Workflow failed at step with error code %errorlevel%.
echo Fix the reported issue and run this launcher again.
echo Press any key to close.
pause >nul
exit /b 1
