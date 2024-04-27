FROM python:3.9.7-slim

RUN apt-get update && apt-get install -y git

RUN pip install --upgrade pip
RUN pip3 install pika
RUN pip3 install ujson
RUN pip3 install pusher
RUN pip3 install python-dotenv

RUN pip3 install git+https://github.com/Trabajo-profesional-grupo-21/common.git@0.0.3#egg=common

COPY / /

CMD ["python3", "./main.py"]