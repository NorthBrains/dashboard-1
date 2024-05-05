FROM bitnami/cassandra:latest

COPY ./init_db/init.cql /docker-entrypoint-initdb.d/
COPY ./init_db/init.sh /docker-entrypoint-initdb.d/

RUN chmod +x /docker-entrypoint-initdb.d/init.sh
RUN chmod +x /docker-entrypoint-initdb.d/init.cql

ENTRYPOINT ["/bin/bash", "-c", "/opt/bitnami/scripts/cassandra/entrypoint.sh /opt/bitnami/scripts/cassandra/run.sh && /docker-entrypoint-initdb.d/init.sh"]