ARG ecrRepo
FROM $ecrRepo/python:3.9-slim

RUN  apt-get -yq update && \
     apt-get -yqq install git ssh


RUN mkdir /code

RUN mkdir -p /root/.ssh && \
    chmod 0700 /root/.ssh && \
    ssh-keyscan github.com > /root/.ssh/known_hosts

ADD ssh_prv_key /root/.ssh/id_rsa
RUN chmod 600 /root/.ssh/id_rsa

COPY ./ /code
WORKDIR /code

RUN pip install -r requirements.txt
RUN chmod 777 -R /code/

CMD ["python3","migration_consumer.py"]