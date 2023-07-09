FROM python:3.10

RUN export AIRFLOW_HOME=/airflow

COPY ./requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

RUN airflow db init

COPY ./airflow.cfg /root/airflow/airflow.cfg

RUN mkdir /airflow
RUN mkdir /airflow/dags

EXPOSE 8080:8080

CMD airflow db init && airflow users create \
    --username user \
    --password user \
    --firstname user \
    --lastname user \
    --role Admin \
    --email not@used.com && (airflow scheduler &) && airflow webserver
