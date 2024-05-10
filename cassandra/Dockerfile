FROM bitnami/cassandra:latest

USER root

RUN mkdir /initdb

COPY ./init_db /initdb

RUN chmod 777 /initdb/init.sh
RUN chmod 777 /initdb/init.cql

EXPOSE 7000 9042

USER 1001

ENTRYPOINT [ "/opt/bitnami/scripts/cassandra/entrypoint.sh" ]
CMD ["/bin/bash", "-c", "/opt/bitnami/scripts/cassandra/run.sh && /initdb/init.sh" ]