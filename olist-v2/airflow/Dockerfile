FROM apache/airflow:2.10.5-python3.11

USER root
# Java dibutuhkan pyspark/spark-submit (driver berjalan di container Airflow, mode client); procps untuk Spark
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless procps curl \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
COPY requirements.txt requirements-dbt.txt requirements-datacontract.txt requirements-kaggle.txt /tmp/req/

# 1) lingkungan utama Airflow: dipasang dengan constraints resmi agar tidak merusak dependensi Airflow
RUN pip install --no-cache-dir -r /tmp/req/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"

# 1b) jar S3A di dalam paket pyspark (Hadoop 3.3.4 bawaan pyspark 3.5): s3a:// ke MinIO tanpa unduhan saat runtime
ARG HADOOP_AWS=3.3.4
ARG AWS_SDK=1.12.262
RUN J=$(python -c "import pyspark,os;print(os.path.join(os.path.dirname(pyspark.__file__),'jars'))") \
 && curl -fsSL -o "$J/hadoop-aws-${HADOOP_AWS}.jar" "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS}/hadoop-aws-${HADOOP_AWS}.jar" \
 && curl -fsSL -o "$J/aws-java-sdk-bundle-${AWS_SDK}.jar" "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK}/aws-java-sdk-bundle-${AWS_SDK}.jar"

# 2) dbt dan datacontract-cli di virtualenv SENDIRI: pin dependensinya sering bentrok dengan Airflow
RUN python -m venv /opt/airflow/venvs/dbt \
 && /opt/airflow/venvs/dbt/bin/pip install --no-cache-dir -r /tmp/req/requirements-dbt.txt
RUN python -m venv /opt/airflow/venvs/datacontract \
 && /opt/airflow/venvs/datacontract/bin/pip install --no-cache-dir -r /tmp/req/requirements-datacontract.txt
RUN python -m venv /opt/airflow/venvs/kaggle \
 && /opt/airflow/venvs/kaggle/bin/pip install --no-cache-dir -r /tmp/req/requirements-kaggle.txt
