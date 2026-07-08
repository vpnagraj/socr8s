FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pull_dispatcher.py .

ENTRYPOINT ["python", "-u", "pull_dispatcher.py"]
