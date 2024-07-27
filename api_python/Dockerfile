FROM python:3.10

RUN adduser --disabled-password --gecos '' --uid 1001 appuser

USER root

RUN apt-get update && apt-get install -y cron
RUN mkdir /scripts && chmod 755 /scripts

COPY ./scripts /scripts

RUN chmod 755 /scripts/requirements.txt /scripts/random_data.py /scripts/clean_log.sh
RUN pip install -r /scripts/requirements.txt
RUN echo "0 0 */5 * * /scripts/clean_log.sh" >> /etc/crontab
RUN service cron start

USER 1001

# Set entrypoint and redirect logs
ENTRYPOINT ["sh", "-c", "python3 /scripts/random_data.py > /scripts/random_data.log 2>&1"]