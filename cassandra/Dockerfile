FROM bitnami/cassandra:latest

RUN mkdir /docker-entrypoint-initdb

COPY ./init_db/init.cql /docker-entrypoint-initdb/
COPY ./init_db/init.sh /docker-entrypoint-initdb/

RUN chmod +x /docker-entrypoint-initdb/init.sh
RUN chmod +x /docker-entrypoint-initdb/init.cql

ENTRYPOINT ["/bin/bash", "-c", "/opt/bitnami/scripts/cassandra/entrypoint.sh /opt/bitnami/scripts/cassandra/run.sh && /docker-entrypoint-initdb.d/init.sh"]