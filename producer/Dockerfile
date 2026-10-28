FROM python:3.13-slim
WORKDIR /app
RUN pip install --no-cache-dir "confluent-kafka>=2.6" "PyYAML>=6.0"
COPY producer/ /app/producer/
COPY config/ /app/config/
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "/app/producer/producer.py"]
