FROM bitnami/cassandra:4.1.5

USER root

RUN mkdir /initdb

COPY ./init_db /initdb

RUN chmod +x /initdb/*
RUN chmod 777 /initdb/*

USER 1001

ENTRYPOINT ["/initdb/initdb.sh"]