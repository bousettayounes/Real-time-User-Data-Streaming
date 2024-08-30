FROM apache/airflow:2.10.0-python3.11

USER root

# Install necessary system packages
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-17-jdk && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Install Python packages
RUN pip install \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-apache-cassandra \
    pyspark==3.4.1 \
    kafka-python \
    requests \
    cassandra-driver \
    confluent-kafka \
    pyspark[avro] \
    avro-python3 \
    fastavro
