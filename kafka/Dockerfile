FROM bitnami/kafka:latest

USER root

RUN mkdir /scipts

COPY /init_kafka /scipts

RUN chmod 777 /scipts/init.sh

USER 1001

CMD ["./scipts/init.sh"]

ENTRYPOINT [ "/opt/bitnami/scripts/kafka/run.sh" ]