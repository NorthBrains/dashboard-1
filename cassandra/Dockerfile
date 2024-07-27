FROM bitnami/cassandra:latest

USER root

RUN apt-get update && apt-get install -y cron
RUN mkdir /initdb

COPY ./init_db /initdb

RUN chmod 755 /initdb/init.sh /initdb/init.cql /initdb/cron_del_data.sh
RUN echo "*/45 * * * * /initdb/cron_del_data.sh >> /initdb/cron_del_data.log 2>&1" > /etc/cron.d/cron_del_data
RUN chmod 0644 /etc/cron.d/cron_del_data
RUN crontab /etc/cron.d/cron_del_data
RUN touch /var/log/cron.log
RUN service cron start

EXPOSE 7000 9042

USER 1001

ENTRYPOINT [ "/opt/bitnami/scripts/cassandra/entrypoint.sh" ]
CMD [ "/bin/bash", "-c", "/opt/bitnami/scripts/cassandra/run.sh && /initdb/init.sh" ]
