FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY build_dispatcher.py .

ENTRYPOINT ["python", "-u", "build_dispatcher.py"]
