FROM apache/airflow:2.9.3-python3.11

# ---- Java: PySpark runs on the JVM ---------------------------------------
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
# TARGETARCH is filled in by Docker (amd64 on most PCs, arm64 on Apple Silicon)
ARG TARGETARCH
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-${TARGETARCH}

# ---- Python packages (always as the airflow user, never as root) ---------
# The base image is already apache-airflow==2.9.3. Re-resolving that package
# in the same command as dbt makes pip 24.2 exhaust its resolver
# (ResolutionTooDeep): today's dbt-adapters / dbt-common ranges are very wide.
# pip 26 plus pins inside the lab's ~=1.8.0 band resolves cleanly.
# dbt-postgres 1.8.2 depends on psycopg2-binary, which this image already has.
# 1.8.1 instead forces the psycopg2 source package on Linux, and that package
# no longer ships a Linux wheel.
USER airflow
RUN pip install --no-cache-dir --upgrade "pip==26.3"
RUN pip install --no-cache-dir "pyspark==3.5.1"
RUN pip install --no-cache-dir \
    "dbt-core==1.8.10" \
    "dbt-postgres==1.8.2" \
    "dbt-adapters==1.16.3" \
    "dbt-common==1.27.1"

# ---- S3A connector JARs, baked into PySpark's own jars/ folder ------------
# Spark needs these two JARs to read/write s3a:// paths (MinIO or AWS S3).
# Baking them in means no download at job start-up and no internet at run time.
RUN JARS="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')" \
    && curl -fsSL -o "$JARS/hadoop-aws-3.3.4.jar" \
       https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -fsSL -o "$JARS/aws-java-sdk-bundle-1.12.262.jar" \
       https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
