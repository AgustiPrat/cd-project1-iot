# Ingestor: cloud MQTT broker -> Kafka (raw-readings, user-settings)
FROM python:3.10-slim
WORKDIR /ingestor
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY ingestor.py ingestor.py
# -u: unbuffered output, so logs show up immediately in docker logs
CMD ["python3", "-u", "ingestor.py"]
