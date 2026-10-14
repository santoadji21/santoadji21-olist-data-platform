FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
# Pasangan versi yang cocok dengan Hadoop 3.3.4 bawaan Spark 3.5.x
ARG HADOOP_AWS=3.3.4
ARG AWS_SDK=1.12.262
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS}/hadoop-aws-${HADOOP_AWS}.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK}/aws-java-sdk-bundle-${AWS_SDK}.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/hadoop-aws-*.jar /opt/spark/jars/aws-java-sdk-bundle-*.jar
USER spark
