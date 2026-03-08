FROM python:3.11-slim

WORKDIR /app

# system deps (optional, for zoneinfo etc already included)
RUN pip install --no-cache-dir --upgrade pip

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app /app/app
COPY scripts /app/scripts
COPY .env.example /app/.env.example
COPY README.md /app/README.md

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "app.main"]
