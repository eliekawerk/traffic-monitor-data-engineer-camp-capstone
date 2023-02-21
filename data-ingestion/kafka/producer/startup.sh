# python -m pip install awscli
export eval $(aws s3 cp s3://traffic-monitor-env-s3-bucket/.env - | sed -e 's/[\r\n]//g')
python producer.py -f confluent.config -t traffic-monitor-topic -d 1