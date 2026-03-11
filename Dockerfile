FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/app
CMD ["python", "-m", "src.orchestration.pipeline", "--start-month", "2024-01", "--end-month", "2024-01"]
