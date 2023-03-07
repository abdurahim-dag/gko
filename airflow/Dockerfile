FROM apache/airflow:2.5.1
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
USER root
RUN mkdir /data \
  && chown airflow: /data
USER airflow