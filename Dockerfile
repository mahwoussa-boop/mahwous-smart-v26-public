# Bypasses Railpack (avoids missing railpack-plan.json in the build pipeline).
FROM python:3.12-slim-bookworm

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install --with-deps chromium

COPY . .

EXPOSE 8080

# Railway sets PORT; default for local docker run
CMD ["sh", "-c", "exec streamlit run app.py --server.port ${PORT:-8080} --server.address 0.0.0.0"]
