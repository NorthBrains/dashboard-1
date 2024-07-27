FROM python:3.10

RUN adduser --disabled-password --gecos '' --uid 1001 appuser

USER root

RUN apt-get update && apt-get install -y cron
RUN mkdir /scripts && chmod 755 /scripts
RUN chown -R appuser:appuser /scripts

COPY ./scripts /scripts

RUN chmod 755 /scripts/requirements.txt \
     /scripts/sales_script.py /scripts/warehouse_script.py \
     /scripts/clean_log.sh /scripts/start_scripts.sh
RUN pip install -r /scripts/requirements.txt
RUN echo "0 0 */5 * * /scripts/clean_log.sh" >> /etc/crontab
RUN service cron start

USER 1001

ENTRYPOINT ["bash", "-c", "/scripts/start_scripts.sh"]