ARG SPARK_VERSION=4.1.3
FROM apache/spark:${SPARK_VERSION}
ARG SPARK_VERSION
ARG DELTA_VERSION=4.1.0
# The driver and executors run Python inside the image, so this is the version jobs run on.
ARG PYTHON_VERSION=3.12.3

USER root

# Delta Lake jars on Spark's global classpath (the Spark minor, e.g. 4.1, is part of the artifact name).
RUN SPARK_MINOR="$(echo "${SPARK_VERSION}" | cut -d. -f1-2)" && \
    curl -fsSL -o "/opt/spark/jars/delta-spark_${SPARK_MINOR}_2.13-${DELTA_VERSION}.jar" \
    "https://repo1.maven.org/maven2/io/delta/delta-spark_${SPARK_MINOR}_2.13/${DELTA_VERSION}/delta-spark_${SPARK_MINOR}_2.13-${DELTA_VERSION}.jar" && \
    curl -fsSL -o "/opt/spark/jars/delta-storage-${DELTA_VERSION}.jar" \
    "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar"

# S3A connector for MinIO. Versions must match Spark's bundled Hadoop (see /opt/spark/jars/hadoop-client-*).
ARG HADOOP_VERSION=3.4.2
ARG AWS_SDK_VERSION=2.29.52
RUN curl -fsSL -o "/opt/spark/jars/hadoop-aws-${HADOOP_VERSION}.jar" \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" && \
    curl -fsSL -o "/opt/spark/jars/aws-sdk-bundle-${AWS_SDK_VERSION}.jar" \
    "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar"

# The OS ships Python 3.10; put the pinned version first on PATH as python3.
COPY --from=ghcr.io/astral-sh/uv:0.13.5 /uv /usr/local/bin/uv
ENV UV_PYTHON_INSTALL_DIR=/opt/python
RUN uv python install "${PYTHON_VERSION}" && \
    ln -sf "$(uv python find "${PYTHON_VERSION}")" /usr/local/bin/python3 && \
    chmod -R a+rX /opt/python

USER spark
