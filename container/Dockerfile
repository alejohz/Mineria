FROM python:3.9


COPY requirements.txt ./requirements.txt
RUN pip3 install -r requirements.txt

COPY . app/

WORKDIR /app

ARG ACCESS_KEY_ID
ARG SECRET_ACCESS_KEY
ARG AWS_SESSION_TOKEN

ENV ACCESS_KEY_ID ${ACCESS_KEY_ID}
ENV SECRET_ACCESS_KEY ${SECRET_ACCESS_KEY}
ENV AWS_SESSION_TOKEN ${AWS_SESSION_TOKEN}
ENV AWS_REGION='us-east-1'

CMD python3 group_move.py