FROM docker.io/bitnami/spark:3-debian-10
USER root
RUN apt-get update && apt-get install -y sudo
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt

#run -> docker build . --tag extending_spark:latest