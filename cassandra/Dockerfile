FROM bitnami/cassandra:4.1.5

USER root

RUN apt-get update && apt-get install -y cron

RUN mkdir /initdb

COPY ./init_db /initdb
COPY ./crons /crons

RUN chmod 777 /initdb/*
RUN chmod 777 /crons/*

USER 1001

ENTRYPOINT ["/bin/bash", "-c", "/opt/bitnami/scripts/cassandra/entrypoint.sh /opt/bitnami/scripts/cassandra/run.sh"]