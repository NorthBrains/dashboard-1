FROM bitnami/cassandra:latest

COPY ./init_db/init.cql /docker-entrypoint-initdb.d/
COPY ./init_db/init.sh /docker-entrypoint-initdb.d/

RUN chmod +x /docker-entrypoint-initdb.d/init.sh

ENTRYPOINT ["/bin/bash", "-c", "/docker-entrypoint-initdb.d/init.sh && /opt/bitnami/scripts/cassandra/entrypoint.sh /opt/bitnami/scripts/cassandra/run.sh"]