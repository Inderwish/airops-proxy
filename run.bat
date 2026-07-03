@echo off
chcp 65001 >nul
cd /d "%~dp0"
pwsh -NoProfile -File "%~dp0start.ps1"
pause
