# Gateway: bridges the home MQTT broker and the cloud MQTT broker
FROM python:3.10.0-slim
WORKDIR /gateway
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY gateway.py gateway.py
# -u: unbuffered output, so print() shows up immediately in docker logs
# exec form: python is PID 1 and receives SIGTERM directly on docker stop
CMD ["python3", "-u", "gateway.py"]
