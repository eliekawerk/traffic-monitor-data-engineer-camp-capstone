import os

os.environ["CONDA_DLL_SEARCH_MODIFICATION_ENABLE"]="1"
import random
import time
import logging
import json
from uuid import uuid4
from datetime import datetime, timedelta
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
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

            # Weekday
            if datetime.now().weekday() in [0, 1, 2, 3, 4]:
                # print("Weekday")
                # Peak
                if (datetime.now().hour >= 6 and datetime.now().hour <= 7) or (
                    datetime.now().hour >= 16 and datetime.now().hour <= 17
                ):
                    time_delay = random.randint(50, 90)
                    speed_factor = random.uniform(-20, 10)
                    # print("Peak")

                # Off-peak
                elif datetime.now().hour <= 4 or datetime.now().hour >= 22:
                    time_delay = random.randint(80, 100)
                    speed_factor = random.uniform(0, 30)
                    # print("Off Peak")

                # Shoulder
                else:
                    time_delay = random.randint(1, 70)
                    speed_factor = random.uniform(-10, 20)
                    # print("Shoulder")

            # Weekend
            else:
                # print("Weekend")
                if datetime.now().hour >= 9 and datetime.now().hour <= 18:
                    time_delay = random.randint(1, 70)
                    speed_factor = random.uniform(-10, 10)

                else:
                    time_delay = random.randint(70, 100)
                    speed_factor = random.uniform(0, 30)

            # Create car object
            car_object = ccloud.generate_random_car_object(speed_factor=speed_factor)

            # Create car dictionary
            car_dictionary = ccloud.object_to_dict(car_object)

            string_serializer = StringSerializer("utf_8")
            json_serializer = JSONSerializer(
                schema_str=ccloud.schema_str,
                schema_registry_client=schema_registry_client,
                to_dict=ccloud.object_to_dict,
            )

            producer.poll(0)

            # Send to confluent cloud topic
            producer.produce(
                topic=topic,
                # key=string_serializer(str(uuid4()),None),
                # key=str(car_dictionary['license_plate']),
                # key=str(1),
                key=str(uuid4()),
                # key=string_serializer(str(1),SerializationContext(topic, MessageField.KEY)),
                value=json_serializer(
                    car_object, SerializationContext(topic, MessageField.VALUE)
                ),
                # value=json.dumps(car_dictionary),
                on_delivery=ccloud.delivery_report
            )

            # Time delay between car objects being created
            time.sleep(time_delay * 0.03)

        producer.flush()
        logging.info(f"Producer has finished.")

    except Exception as e:
        logging.exception(f"An exception occured: {e}")


if __name__ == "__main__":
    run_producer()
