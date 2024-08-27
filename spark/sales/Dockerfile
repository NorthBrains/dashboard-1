FROM bitnami/spark:3.5.1

COPY ./files /files

USER root
RUN chmod +x /files/*

USER 1001

RUN pip install --no-cache-dir -r /files/requirements.txt

CMD ["/bin/bash", "/files/spark_submit_sales.sh"]