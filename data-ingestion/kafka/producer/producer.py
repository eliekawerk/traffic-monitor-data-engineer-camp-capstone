import os
os.environ["CONDA_DLL_SEARCH_MODIFICATION_ENABLE"]="1"
import random
import time
import logging
from datetime import datetime, timedelta
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from utility import ccloud_lib as ccloud


def run_producer():
    """
    Function to run the kafka producer and push data to confluent cloud.
    """

    # Setup logging
    logging.basicConfig(
        format="[%(levelname)s][%(asctime)s]: %(message)s", level=logging.INFO
    )

    try:

        logging.info("Producer has started.")

        # Read arguments and configurations and initialize
        args = ccloud.parse_args()
        config_file = args.config_file
        topic = args.topic
        duration_in_minutes = int(args.duration)

        ccloud_config = ccloud.read_ccloud_config(config_file)

        ccloud_config_env_params = {
            "bootstrap.servers": os.environ.get("bootstrap_servers"),
            "sasl.username": os.environ.get("sasl_username"),
            "sasl.password": os.environ.get("sasl_password"),
        }

        schema_registry_env_params = {
            "basic.auth.user.info": os.environ.get("basic_auth_user_info"),
            "schema.registry.url": os.environ.get("schema_registry_url"),
        }

        # Add env params to ccloud config dictionary
        config = dict(ccloud_config, **ccloud_config_env_params)

        schema_registry_config = {
            "url": schema_registry_env_params["schema.registry.url"],
            "basic.auth.user.info": schema_registry_env_params["basic.auth.user.info"],
        }
        schema_registry_client = SchemaRegistryClient(schema_registry_config)

        # Create producer
        producer = Producer(config)

        # Create topic if needed
        ccloud.create_topic(config, topic)

        # Set run end time
        end_datetime = datetime.now() + timedelta(minutes=duration_in_minutes)

        # Loop n times generating car objects simulating cars driving under a gantry
        while datetime.now() < end_datetime:

            hour = datetime.now().hour

            # Weekday
            if datetime.now().weekday() in [0,1,2,3,4]:
                
                match hour:                
                    case 0:
                        time_delay = random.randint(600, 800)
                        speed_factor = random.uniform(0, 10)
                        
                    case 1:
                        time_delay = random.randint(800, 1200)
                        speed_factor = random.uniform(0, 10)
                        
                    case 2:
                        time_delay = random.randint(1200, 1400)
                        speed_factor = random.uniform(0, 10)
                        
                    case 3:
                        time_delay = random.randint(1400, 1800)
                        speed_factor = random.uniform(0, 10)
                        
                    case 4:
                        time_delay = random.randint(400, 600)
                        speed_factor = random.uniform(0, 5)
                        
                    case 5:
                        time_delay = random.randint(100, 150)
                        speed_factor = random.uniform(-5, 5)
                        
                    case 6:
                        time_delay = random.randint(10, 20)
                        speed_factor = random.uniform(-15, -5)
                        
                    case 7:
                        time_delay = random.randint(5, 10)
                        speed_factor = random.uniform(-15, -10)
                        
                    case 8:
                        time_delay = random.randint(10, 20)
                        speed_factor = random.uniform(-10, -5)
                        
                    case 9:
                        time_delay = random.randint(10, 20)
                        speed_factor = random.uniform(-5, 0)
                        
                    case 10:
                        time_delay = random.randint(20, 30)
                        speed_factor = random.uniform(0, 5)
                        
                    case 11:
                        time_delay = random.randint(30, 40)
                        speed_factor = random.uniform(5, 10)
                        
                    case 12:
                        time_delay = random.randint(20, 30)
                        speed_factor = random.uniform(0, 5)
                        
                    case 13:
                        time_delay = random.randint(20, 30)
                        speed_factor = random.uniform(-5, 0)
                        
                    case 14:
                        time_delay = random.randint(30, 40)
                        speed_factor = random.uniform(-10, -5)
                        
                    case 15:
                        time_delay = random.randint(20, 30)
                        speed_factor = random.uniform(-5, 0)
                        
                    case 16:
                        time_delay = random.randint(10, 20)
                        speed_factor = random.uniform(-10, -5)
                        
                    case 17:
                        time_delay = random.randint(5, 10)
                        speed_factor = random.uniform(-15, -10)
                        
                    case 18:
                        time_delay = random.randint(10, 20)
                        speed_factor = random.uniform(-10, -5)
                        
                    case 19:
                        time_delay = random.randint(20, 30)
                        speed_factor = random.uniform(-5, 0)
                        
                    case 20:
                        time_delay = random.randint(30, 50)
                        speed_factor = random.uniform(0, 5)
                        
                    case 21:
                        time_delay = random.randint(50, 100)
                        speed_factor = random.uniform(0, 10)
                        
                    case 22:
                        time_delay = random.randint(100, 200)
                        speed_factor = random.uniform(0, 10)
                        
                    case 23:
                        time_delay = random.randint(200, 600)
                        speed_factor = random.uniform(0, 10)                     

            # Weekend
            else:   
                match hour:
                    case 0:
                        time_delay = random.randint(600, 800)
                        speed_factor = random.uniform(0, 10)
                        
                    case 1:
                        time_delay = random.randint(800, 1200)
                        speed_factor = random.uniform(0, 10)
                        
                    case 2:
                        time_delay = random.randint(1200, 1400)
                        speed_factor = random.uniform(0, 10)
                        
                    case 3:
                        time_delay = random.randint(1400, 1800)
                        speed_factor = random.uniform(0, 10)
                        
                    case 4:
                        time_delay = random.randint(400, 600)
                        speed_factor = random.uniform(0, 10)
                        
                    case 5:
                        time_delay = random.randint(250, 350)
                        speed_factor = random.uniform(0, 10)
                        
                    case 6:
                        time_delay = random.randint(100, 200)
                        speed_factor = random.uniform(0, 10)
                        
                    case 7:
                        time_delay = random.randint(50, 80)
                        speed_factor = random.uniform(0, 10)
                        
                    case 8:
                        time_delay = random.randint(20, 40)
                        speed_factor = random.uniform(0, 10)
                        
                    case 9:
                        time_delay = random.randint(20, 40)
                        speed_factor = random.uniform(0, 5)
                        
                    case 10:
                        time_delay = random.randint(20, 40)
                        speed_factor = random.uniform(0, 5)
                        
                    case 11:
                        time_delay = random.randint(20, 30)
                        speed_factor = random.uniform(0, 5)
                        
                    case 12:
                        time_delay = random.randint(20, 30)
                        speed_factor = random.uniform(0, 5)
                        
                    case 13:
                        time_delay = random.randint(20, 30)
                        speed_factor = random.uniform(0, 5)
                        
                    case 14:
                        time_delay = random.randint(20, 30)
                        speed_factor = random.uniform(0, 5)
                        
                    case 15:
                        time_delay = random.randint(20, 30)
                        speed_factor = random.uniform(0, 5)
                        
                    case 16:
                        time_delay = random.randint(20, 40)
                        speed_factor = random.uniform(0, 5)
                        
                    case 17:
                        time_delay = random.randint(20, 40)
                        speed_factor = random.uniform(0, 10)
                        
                    case 18:
                        time_delay = random.randint(20, 40)
                        speed_factor = random.uniform(0, 10)
                        
                    case 19:
                        time_delay = random.randint(20, 40)
                        speed_factor = random.uniform(0, 10)
                        
                    case 20:
                        time_delay = random.randint(30, 50)
                        speed_factor = random.uniform(0, 10)
                        
                    case 21:
                        time_delay = random.randint(50, 100)
                        speed_factor = random.uniform(0, 10)
                        
                    case 22:
                        time_delay = random.randint(100, 200)
                        speed_factor = random.uniform(0, 10)
                        
                    case 23:
                        time_delay = random.randint(200, 600)
                        speed_factor = random.uniform(0, 10)    


            # Create car object
            car_object = ccloud.generate_random_car_object(speed_factor=speed_factor)

            json_serializer = JSONSerializer(
                schema_str=ccloud.schema_str,
                schema_registry_client=schema_registry_client,
                to_dict=ccloud.object_to_dict,
            )

            producer.poll(0)

            # Send to confluent cloud topic
            producer.produce(
                topic=topic,
                key=car_object.license_plate,
                value=json_serializer(
                    car_object, SerializationContext(topic, MessageField.VALUE)
                ),
                on_delivery=ccloud.delivery_report
            )

            # Time delay between car objects being created
            time.sleep(time_delay / 10)

        producer.flush()
        logging.info(f"Producer has finished.")

    except Exception as e:
        logging.exception(f"An exception occured: {e}")


if __name__ == "__main__":
    run_producer()
