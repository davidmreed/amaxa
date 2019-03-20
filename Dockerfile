FROM ubuntu

FROM python

RUN pip install --upgrade pip

WORKDIR /amaxa

COPY . /amaxa

RUN pip install amaxa

# TODO: add environmental variables from .envrc directly
# google "dockerfile .envrc best practice"
