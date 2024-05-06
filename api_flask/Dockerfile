FROM python:3.10

USER root

RUN mkdir /scripts
RUN mkdir /scripts/logs

COPY /scripts /scripts

RUN chmod 777 /scripts/requirements.txt
RUN chmod 777 /scripts/random_data.py
RUN chmod 777 /scripts/logs/random_data.log

RUN pip install -r /scripts/requirements.txt

USER 1001

CMD ["nohup python3 /scripts/random_data.py 2&1> /scripts/logs/random_data.log &"]