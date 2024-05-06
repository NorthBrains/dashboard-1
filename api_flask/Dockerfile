FROM python:3.10

USER root

COPY ./api_flask/scripts/requirements.txt /usr/src/app/requirements.txt
COPY ./api_flask/scripts/random_data.py /usr/src/app/random_data.py

RUN chmod 777 /usr/src/app/requirements.txt
RUN chmod 777 /usr/src/app/random_data.py

RUN pip install -r /usr/src/app/requirements.txt

USER 1001

CMD ["python3", "/usr/src/app/random_data.py"]