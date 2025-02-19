@echo off
cd /d "%~dp0"
echo Cargando entorno virtual...

call .venv\Scripts\activate.bat

echo Cargando variables de entorno...
setlocal
for /f "usebackq delims=" %%a in (".env") do set %%a

echo Ejecutando script volvo2.0.py...
python volvo2.0.py

echo Desactivando entorno virtual...
deactivate

echo Proceso finalizado.
pause
