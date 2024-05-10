FROM python:3.10

USER root

RUN mkdir /scripts

COPY ./scripts /scripts

RUN chmod 777 /scripts/requirements.txt
RUN chmod 777 /scripts/dashboard.py

RUN pip install -r /scripts/requirements.txt

USER 1001

CMD ["python3", "/scripts/dashboard.py"]