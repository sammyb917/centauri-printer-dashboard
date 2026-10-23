@echo off
cd /d "%~dp0"
start "Printer Dashboard" /min py -3 printer-dashboard.py
if errorlevel 1 start "Printer Dashboard" /min python printer-dashboard.py
