FROM quay.io/jupyter/minimal-notebook:python-3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER ${NB_UID}
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
WORKDIR /home/jovyan/work
