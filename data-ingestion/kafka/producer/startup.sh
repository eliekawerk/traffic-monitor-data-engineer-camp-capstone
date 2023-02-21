python -m pip install awscli
#eval $(aws s3 cp s3://traffic-monitor-env-s3-bucket/.env - | sed 's/^[\r\n]/export /')
export eval $(aws s3 cp s3://traffic-monitor-env-s3-bucket/.env - | sed -e 's/[\r\n]//g')
echo $server
echo $username
echo $password
python producer.py -f confluent.config -t traffic-monitor-topic -d 1