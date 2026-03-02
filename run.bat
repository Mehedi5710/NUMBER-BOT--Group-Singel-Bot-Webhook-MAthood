@echo off
title NUMBER BOT Runner
echo ========================================
echo           N U M B E R   B O T
echo ========================================
echo    Advanced OTP Number Management System
echo ========================================
echo.

REM Check if we're in the right directory
if not exist "bot.Py" (
    echo ERROR: bot.Py not found in current directory
    echo Please run this batch file from the MAIN BOT 2.0 directory
    pause
    exit /b 1
)

REM Pick Python interpreter (venv if present, otherwise system py)
if exist ".venv\Scripts\python.exe" (
    set "PYTHON_EXE=.venv\Scripts\python.exe"
    REM Check if requirements are installed (basic check)
    if not exist ".venv\Lib\site-packages\telebot" (
        echo WARNING: Telebot package may not be installed
        echo You might need to run: pip install -r requirements.txt
        timeout /t 3 >nul
    )
) else (
    echo WARNING: Virtual environment not found - using system Python
    set "PYTHON_EXE=py"
)

REM If py launcher is broken or missing, try python
if "%PYTHON_EXE%"=="py" (
    py -c "import sys" >nul 2>&1
    if errorlevel 1 (
        set "PYTHON_EXE=python"
    )
)

echo Starting bot...
echo Press Ctrl+C to stop the bot
echo.

REM Run the bot
echo Using: %PYTHON_EXE%
%PYTHON_EXE% bot.Py

REM If bot exits, show message
echo.
echo ========================================
echo    Bot has stopped running
echo ========================================
