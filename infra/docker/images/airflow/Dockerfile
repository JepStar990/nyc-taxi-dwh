# infra/docker/images/airflow/Dockerfile
FROM apache/airflow:2.9.3-python3.11

USER root

# Tools: Java for Spark, curl/jq for health checks & queries
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless curl jq ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Install Spark 3.5.1 (Hadoop 3)
ENV SPARK_VERSION=3.5.1
ENV HADOOP_PROFILE=hadoop3
RUN curl -fL -o /tmp/spark.tgz \
      https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE}.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-${HADOOP_PROFILE} /opt/spark && \
    rm /tmp/spark.tgz
ENV SPARK_HOME=/opt/spark
ENV PATH=${SPARK_HOME}/bin:${PATH}

# Add required jars for Iceberg + S3 + OpenLineage (keep versions aligned with Spark 3.5)
RUN mkdir -p /opt/spark/jars && \
    curl -fL -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar && \
    curl -fL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.697.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.697/aws-java-sdk-bundle-1.12.697.jar && \
    curl -fL -o /opt/spark/jars/openlineage-spark_2.12-1.20.0.jar \
      https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/1.20.0/openlineage-spark_2.12-1.20.0.jar

# Airflow providers and helpful libs
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.7.1 \
    apache-airflow-providers-http==4.6.2 \
    requests==2.32.3 \
    feast==0.43.1 redis==5.0.1 \
    apache-airflow-providers-hashicorp==3.7.0

# OpenLineage Airflow integration
RUN pip install --no-cache-dir openlineage-airflow==1.20.0

# Set env for Spark to find Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
WORKDIR /opt/airflow
