FROM python:3.8-slim

WORKDIR /home/pipeline

COPY requirements.txt /home/pipeline/requirements.txt

RUN pip install -r requirements.txt && pip install --upgrade pip

RUN adduser --system --no-create-home pipeline

USER pipeline

COPY . /home/pipeline/