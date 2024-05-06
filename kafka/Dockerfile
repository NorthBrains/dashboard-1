FROM bitnami/kafka:latest

USER root

COPY ./init_kafka/init.sh /docker-entrypoint-init.d/

RUN chmod 777 /docker-entrypoint-init.d/init.sh

USER 1001

ENTRYPOINT [ "/bin/bash", "-c", "docker-entrypoint-init.d/init.sh && /etc/confluent/docker/run"]