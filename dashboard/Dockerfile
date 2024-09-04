FROM python:3.10

USER root

RUN mkdir /scripts
RUN mkdir /data
RUN mkdir /libs

COPY ./scripts /scripts
COPY ./data /data
COPY scripts/libs /libs

RUN chmod 777 /scripts/requirements.txt /scripts/app_warehouse.py /data/* /libs/*
RUN pip install -r /scripts/requirements.txt

EXPOSE 5000

USER 1001

CMD ["python3", "/scripts/app.py"]