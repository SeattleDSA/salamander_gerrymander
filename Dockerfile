FROM alpine:latest

RUN apk add --update \
    python \
    python-dev \
    py-pip \
    build-base \
  && pip install virtualenv

RUN pip install pandas
RUN pip install requests

RUN /bin/sh -c "mkdir /salamander_gerrymander"

WORKDIR /salamander_gerrymander

COPY geolocation_for_sdsa.py /salamander_gerrymander
