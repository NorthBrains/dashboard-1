FROM bitnami/cassandra:latest

USER root

RUN mkdir /initdb

COPY /init_db /initdb

RUN chmod 777 /initdb/init.sh
RUN chmod 777 /initdb/init.cql

USER 1001

CMD ["./initdb/init.sh"]

ENTRYPOINT ["/bin/bash", "-c", "/opt/bitnami/scripts/cassandra/entrypoint.sh /opt/bitnami/scripts/cassandra/run.sh"]