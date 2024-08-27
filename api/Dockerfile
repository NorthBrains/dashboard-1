FROM python:3.10

RUN adduser --disabled-password --gecos '' --uid 1001 appuser

RUN apt-get update && apt-get install -y cron

RUN mkdir /scripts && chmod 755 /scripts
RUN chown -R appuser:appuser /scripts

COPY ./scripts /scripts

RUN chmod +x /scripts/start_scripts.sh
RUN chmod 755 /scripts/requirements.txt \
    /scripts/sales_script.py /scripts/warehouse_script.py \
    /scripts/clean_log.sh /scripts/start_scripts.sh

RUN pip install -r /scripts/requirements.txt

COPY ./scripts/clean_log.sh /etc/cron.d/clean_log

RUN chown root:root /etc/cron.d/clean_log
RUN chmod 0644 /etc/cron.d/clean_log
RUN echo "0 0 */5 * * root /scripts/clean_log.sh" > /etc/cron.d/clean_log
RUN crontab /etc/cron.d/clean_log

CMD cron && /bin/bash /scripts/start_scripts.sh && tail -f /dev/null