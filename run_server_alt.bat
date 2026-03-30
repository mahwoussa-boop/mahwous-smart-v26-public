@echo off
cd /d "%~dp0"
echo تشغيل Streamlit على المنفذ 8503 (نسخة ثانية بجانب 8501/8502 الافتراضية)
streamlit run app.py --server.port 8503 --server.address localhost
pause
