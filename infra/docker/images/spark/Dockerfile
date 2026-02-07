FROM spark:3.5.7-java17

USER root

# Install curl for fetching jars at build-time
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Put extra JARs under $SPARK_HOME/jars (the official image sets SPARK_HOME)
# We'll use /opt/spark/jars explicitly.
RUN mkdir -p /opt/spark/jars \
 && curl -fL -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
 && curl -fL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && curl -fL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.697.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.697/aws-java-sdk-bundle-1.12.697.jar

# --- Kafka Structured Streaming connector (version aligned to Spark 3.5.x) ---
# Connector + token provider + Kafka client
 && curl -fL -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -fL -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -fL -o /opt/spark/jars/kafka-clients-3.5.1.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar

WORKDIR /workspace

