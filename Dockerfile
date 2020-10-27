FROM python:3.8-buster AS filoc_builder

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install  --quiet --no-cache-dir  -r /tmp/requirements.txt && rm /tmp/requirements.txt
