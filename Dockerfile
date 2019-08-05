FROM ubuntu

FROM python

RUN pip install --upgrade pip

WORKDIR /amaxa

COPY . /amaxa

RUN pip install amaxa
