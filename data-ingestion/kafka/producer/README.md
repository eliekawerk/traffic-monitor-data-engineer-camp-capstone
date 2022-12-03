# How to run

## Create EC2 Instance
Create an EC2 instance with a Linux image on AWS utilising the Free tier.

## git 
Use git to clone this repo via HTTPS or SSH

## Confluent
Add your Confluent environment details to producer/confluent.config

## Install required packages
pip3 install -r requirements.txt

## Run the producer

```
python producer.py -f confluent.config -t traffic -d <duration_to_run_script_for_in_seconds>
```
