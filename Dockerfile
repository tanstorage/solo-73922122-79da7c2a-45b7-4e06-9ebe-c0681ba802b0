FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DATA_DIR=/data \
    SEED_DIR=/app/seed

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY seed ./seed

EXPOSE 8000

# 单进程 uvicorn；两个 API 进程由 docker compose 的两个服务分别承担
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
