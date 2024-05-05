FROM python:3.10

COPY ./scripts/requirements.txt /usr/src/app/requirements.txt
COPY ./scripts/random_data.py /usr/src/app/random_data.py

RUN chmod +x /usr/src/app/requirements.txt
RUN chmod +x /usr/src/app/random_data.py

RUN pip install -r /usr/src/app/requirements.txt

CMD ["python3", "/usr/src/app/random_data.py"]