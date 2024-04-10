FROM python:3.9.7-slim

RUN apt-get update

RUN pip install --upgrade pip
RUN pip3 install pika
RUN pip3 install ujson

COPY / /

CMD ["python3", "./main.py"]