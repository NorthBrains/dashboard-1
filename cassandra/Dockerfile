FROM bitnami/cassandra:latest

USER root

RUN mkdir /docker-entrypoint-initdb

COPY ./init_db/init.cql /docker-entrypoint-initdb/
COPY ./init_db/init.sh /docker-entrypoint-initdb/

RUN chmod 777 /docker-entrypoint-initdb/init.sh
RUN chmod 777 /docker-entrypoint-initdb/init.cql

USER 1001

ENTRYPOINT ["/bin/bash", "-c", "/opt/bitnami/scripts/cassandra/entrypoint.sh /opt/bitnami/scripts/cassandra/run.sh && /docker-entrypoint-initdb.d/init.sh"]