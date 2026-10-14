FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir confluent-kafka==2.16.0
COPY producer.py .
CMD ["python", "-u", "producer.py"]
