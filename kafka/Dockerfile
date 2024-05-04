FROM bitnami/kafka:latest
COPY ./init_kafka/init.sh /docker-entrypoint-init.d/

RUN chmod + x /docker-entrypoint-init.d/init.sh

ENTRYPOINT [ "/bin/bash", "-c", "docker-entrypoint-init.d/init.sh && /etc/confluent/docker/run"]