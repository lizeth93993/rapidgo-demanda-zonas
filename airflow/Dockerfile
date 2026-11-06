# Airflow 3.1.8 + librerias para leer el Data Lake (Parquet) desde el DAG.
# Se instala apache-airflow con la misma version para que pip no la cambie.
FROM apache/airflow:3.1.8-python3.12
RUN pip install --no-cache-dir "apache-airflow==3.1.8" "pandas==2.3.3" "pyarrow==18.1.0" "PyYAML>=6.0"
