# Airflow 3.1.8 on top of the pinned Spark image, so a task can run spark-submit on YARN directly, with no Docker socket.
FROM telecom-spark:4.0.5

ARG AIRFLOW_VERSION=3.1.8

USER root
# virtualenv rather than python3-venv: this image's Python has no ensurepip, and an apt package would be unpinned.
# Airflow's own constraints file for this version and Python 3.10 pins every dependency.
RUN python3 -m pip install --no-cache-dir "virtualenv==20.26.6" "distlib==0.4.4" "filelock==3.32.7" "platformdirs==4.12.4" \
 && python3 -m virtualenv /opt/airflow-venv \
 && /opt/airflow-venv/bin/pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.10.txt" \
 && mkdir -p /opt/airflow-home /opt/airflow-dags \
 && chown -R spark:spark /opt/airflow-home

COPY --chmod=755 start.sh /opt/airflow-start.sh

USER spark
ENV AIRFLOW_HOME=/opt/airflow-home \
    PATH="/opt/airflow-venv/bin:${PATH}" \
    AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow-dags \
    AIRFLOW__CORE__LOAD_EXAMPLES=False
