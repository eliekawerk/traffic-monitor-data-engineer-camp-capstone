FROM python:3.10

WORKDIR /producer

COPY /producer . 

RUN pip install -r requirements.txt 

ENV PYTHONPATH=/producer

ENTRYPOINT ["/bin/bash", "startup.sh"]