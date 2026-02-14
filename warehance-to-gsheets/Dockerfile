FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default: run once. Override with --schedule for daemon mode.
ENTRYPOINT ["python", "agent.py"]
