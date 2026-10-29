# Imagen oficial de Spark con Python. El conector de Kafka se descarga al iniciar
# (spark-submit --packages) y queda en el volumen ivy-cache del docker-compose.
FROM apache/spark:3.5.6-scala2.12-java17-python3-ubuntu
USER root
RUN pip3 install --no-cache-dir "PyYAML>=6.0"
WORKDIR /app
