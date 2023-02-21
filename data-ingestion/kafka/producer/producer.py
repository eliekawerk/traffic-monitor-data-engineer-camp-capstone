import os
os.environ['CONDA_DLL_SEARCH_MODIFICATION_ENABLE']='1'
import random
import time
import json
import logging
# import s3fs
# import boto3
# import pandas as pd
from datetime import datetime, timedelta
from faker import Faker
from faker_vehicle import VehicleProvider
from confluent_kafka.cimpl import Producer
from utility import ccloud_lib, dynamic_faker_providers




def run_producer():
    """
    Function to run the kafka producer and push data to confluent.
    """

    def generate_random_car_attributes(speed_factor:int)->dict:
        """
        Function to generate additional car attributes object with random attributes.
        """
        return {
            "license_plate": fake.license_plate(),
            "colour": fake.car_colour(),
            "fuel_type": fake.car_fuel_type(),
            "passenger_count": fake.car_passengers_count(),
            "travel_direction": fake.car_travel_direction(),
            "lane": fake.car_lane(),
            "speed": int(random.randint(50,90) + speed_factor),
            "datetimestamp": datetime.now().strftime("%d/%m/%Y %H:%M:%S.%f")
        }

    def acked(err, msg):
        """
        Delivery report handler called on successful or failed delivery of message.
        """
        global delivered_records

        if err is not None:
            logging.warning(f"Failed to deliver message: {err}")
        else:
            delivered_records += 1

    # Setup logging
    logging.basicConfig(
        format="[%(levelname)s][%(asctime)s][%(filename)s]: %(message)s", level=logging.INFO)

    try:

        logging.info('Producer has started.')

        # Read arguments and configurations and initialize
        args = ccloud_lib.parse_args()
        config_file = args.config_file
        topic = args.topic
        duration_in_minutes = int(args.duration)
        config = ccloud_lib.read_ccloud_config(config_file)

        # Create Producer instance
        config = ccloud_lib.pop_schema_registry_params_from_config(config)

        # s3 = client('s3', 
        #     aws_access_key_id = aws_access_id,
        #     aws_secret_access_key= aws_secret_key,
        #     region_name= aws_region   
        #     )

        # fs = s3fs.S3FileSystem(anon=True)
        # fs.ls('traffic-monitor-env-s3-bucket')
        # with fs.open('traffic-monitor-env-s3-bucket/.env', 'rb') as f:
        #     print(f.read())


        # # Creating the low level functional client
        # client = boto3.client(
        #     's3',
        #     aws_access_key_id = 'AKIAYB3ZZTCSZVNFOSLT',
        #     aws_secret_access_key = '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',
        #     region_name = 'ap-southeast-2'
        # )
            
        # # Creating the high level object oriented interface
        # s3= boto3.resource(
        #     's3',
        #     aws_access_key_id = 'AKIAYB3ZZTCSZVNFOSLT',
        #     aws_secret_access_key = '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',
        #     region_name = 'ap-southeast-2'
        # )

        # Fetch the list of existing buckets
        # clientResponse = client.list_buckets()
            
        # Print the bucket names one by one
        # print('Printing bucket names...')
        # for bucket in clientResponse['Buckets']:
        #     print(f'Bucket Name: {bucket["Name"]}')

        # my_bucket = s3.Bucket('traffic-monitor-env-s3-bucket')
        # print('print response')
        # response = client.get_object(Bucket='traffic-monitor-env-s3-bucket', Key='.env')
        # print('print status: ')
        # status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        # get_dates = pd.read_csv(response.get("Body"))

        # print(status)

        # for my_bucket_object in my_bucket.objects.all():
        #     print(my_bucket_object.key)

        # read file content 
        # raw_content = my_bucket.Object(key=".env").get()
        # raw_content = my_bucket.Object(key=".env").get().get('Body').read().decode('utf-8')
        # print('print raw content')
        # print(raw_content)

        # # Create the S3 object
        # obj = resource.get_object(
        #     Bucket = 'traffic-monitor-env-s3-bucket',
        #     Key = '.env'
        # )
            
        # # Read data from the S3 object
        # data = pd.read_csv(obj['Body'])
            
        # Print the data frame
        # print('Printing the data frame...')
        # print(data)


        # s3 = client('s3', 
        #     aws_access_key_id = 'AKIAYB3ZZTCS7H2ETNW4',
        #     aws_secret_access_key= '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',
        #     region_name='ap-northeast-1'
        #     )

        # path='s3://traffic-monitor-env-s3-bucket/.env'
        # print('passed this point')
        # df=pd.read_csv(path)
        # print("df was able to read file")
        # print(df.head())

        # response = s3.get_object(Bucket=aws_bucket, Key=aws_log_file)
        # status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        
        # if status == 200:
            
        #     logging.info(f"Successfully request and read from S3. Status - {status}")
        #     get_dates = pd.read_csv(response.get("Body"))
        #     try:
        #         latest_date = get_dates[get_dates["incremental_value"]== get_dates["incremental_value"].max()]["incremental_value"].values[0]
        #         return latest_date
        #     except:
        #         return None

        # else:
        #     logging.error(f"Request to get object from S3 has failed. Status - {status}")    

        config_env_params = {
            'bootstrap.servers': os.environ.get('server'),
            'sasl.username': os.environ.get('username'),
            'sasl.password': os.environ.get('password'),
        }

        # print(config_env_params)

        # Add env params to config
        config = dict(config, **config_env_params)
        
        print(config)

        producer = Producer(config)

        # Create topic if needed
        ccloud_lib.create_topic(config, topic)

        # Instanciate faker
        fake = Faker()

        # Add dynamic providers to faker
        fake.add_provider(dynamic_faker_providers.car_colour_provider)
        fake.add_provider(dynamic_faker_providers.car_fuel_type_provider)
        fake.add_provider(dynamic_faker_providers.car_passengers_count_provider)
        fake.add_provider(dynamic_faker_providers.car_travel_direction_provider)
        fake.add_provider(dynamic_faker_providers.car_lane)
        fake.add_provider(VehicleProvider)

        # Set run end time
        end_datetime = datetime.now() + timedelta(minutes=duration_in_minutes)

        # Loop n times generating car objects simulating cars driving under a gantry
        while datetime.now() < end_datetime:

            # Weekday
            if datetime.now().weekday() in [0, 1, 2, 3, 4]:
                # print('Weekday')
                # Peak
                if (datetime.now().hour >= 6 and datetime.now().hour <= 7) or (datetime.now().hour >= 16 and datetime.now().hour <= 17):
                    time_delay = random.randint(50, 90)
                    speed_factor = random.uniform(-20, 10)
                    # print('Peak')

                # Off-peak
                elif datetime.now().hour <= 4 or datetime.now().hour >= 22:
                    time_delay = random.randint(80, 100)
                    speed_factor = random.uniform(0, 30)
                    # print('Off Peak')

                # Shoulder
                else:
                    time_delay = random.randint(1, 70)
                    speed_factor = random.uniform(-10, 20)
                    # print('Shoulder')

            # Weekend
            else:
                # print('Weekend')
                if (datetime.now().hour >= 9 and datetime.now().hour <= 18):
                    time_delay = random.randint(1, 70)
                    speed_factor = random.uniform(-10, 10)

                else:
                    time_delay = random.randint(70, 100)
                    speed_factor = random.uniform(0, 30)

            car = fake.vehicle_object()
            car_attributes = generate_random_car_attributes(speed_factor=speed_factor)

            # Join car and attributes into one dictionary
            car_object = dict(car, **car_attributes)
            logging.info(car_object)

            producer.produce(topic, key=car_object['license_plate'], value=json.dumps(car_object), on_delivery=acked)
            producer.poll(0)

            # Time delay between car objects being created
            time.sleep(time_delay * 0.1)

        producer.flush()
        logging.info(f'Producer has finished. {delivered_records} messages were produced to topic: {topic}!')

    except Exception as e:
        logging.exception(f'{delivered_records} messages were produced to topic: {topic}, before throughing exception: {e}')

if __name__ == '__main__':
    delivered_records = 0
    run_producer()
