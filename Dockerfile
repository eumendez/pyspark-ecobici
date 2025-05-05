FROM bitnami/spark:3.5

USER root
WORKDIR /app

# Dependencias 
RUN install_packages python3-pip && \
    pip install --no-cache-dir pandas pyarrow matplotlib==3.9.* notebook

COPY src/ ./src/

# Ejecutar análisis completo
ENV SPARK_DRIVER_MEMORY=4g
ENTRYPOINT ["spark-submit", "--master", "local[*]"]
CMD        ["/app/src/analysis.py"]
