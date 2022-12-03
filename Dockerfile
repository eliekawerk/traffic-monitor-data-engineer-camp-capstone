FROM python:3.9

WORKDIR /producer

# ENV API_KEY="my_api_key"
# ENV NAME="your_name" 
# ENV CITY="your_city"

COPY /producer .

RUN pip install -r requirements.txt 

CMD ["python", "producer.py", "-f", "confluent.config", "-t", "traffic"]
