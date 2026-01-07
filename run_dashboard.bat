@echo off
echo 🚀 Iniciando Dashboard del Data Lake...
echo.
echo 📊 El dashboard se abrirá en: http://localhost:8501
echo.
echo ⚠️  Para detener: Ctrl+C
echo.

cd /d "%~dp0"
call .venv\Scripts\activate.bat
streamlit run dashboard\app.py

pause