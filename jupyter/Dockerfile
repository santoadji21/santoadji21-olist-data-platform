# Jupyter untuk Olist Data Platform: client Postgres/Kafka/S3 + pandas/pyarrow/matplotlib sudah terpasang.
FROM quay.io/jupyter/minimal-notebook:python-3.13
USER root
RUN pip install --no-cache-dir "psycopg[binary]==3.2.*" kafka-python-ng==2.2.3 boto3==1.35.* \
        pandas==2.2.* pyarrow==18.* matplotlib==3.9.* && \
    fix-permissions "${CONDA_DIR}" && fix-permissions "/home/${NB_USER}"
USER ${NB_UID}
WORKDIR /home/jovyan/work
