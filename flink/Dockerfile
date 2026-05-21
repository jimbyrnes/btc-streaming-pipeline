FROM python:3.10-slim-bullseye

RUN apt-get update && apt-get install -y --no-install-recommends \
        default-jre-headless \
        wget \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir apache-flink==1.17.0

# Drop the Kafka SQL connector into PyFlink's lib directory so the
# embedded mini-cluster picks it up automatically at runtime.
RUN PYFLINK_LIB=$(python3 -c "import os, pyflink; print(os.path.join(os.path.dirname(pyflink.__file__), 'lib'))") && \
    wget -q -P "$PYFLINK_LIB" \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.0/flink-sql-connector-kafka-1.17.0.jar

COPY jobs/ /opt/jobs/

CMD ["python3", "/opt/jobs/btc_aggregator.py"]
