import argparse, sys
import random
import logging
from datetime import datetime, timedelta
from confluent_kafka import KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker
from faker_vehicle import VehicleProvider
from utility import dynamic_faker_providers

# Set schema registry template
schema_str = """
    {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "description": "Car object schema.",
        "title": "carObject",
        "type": "object",
        "properties": {
            "category": {
            "description": "The string type is used for strings of text.",
            "type": "string"
            },
            "make": {
            "description": "The string type is used for strings of text.",
            "type": "string"
            },
            "model": {
            "description": "The string type is used for strings of text.",
            "type": "string"
            },
            "year": {
            "description": "The integer type is used for integral numbers.",
            "type": "integer"
            },
            "colour": {
            "description": "The string type is used for strings of text.",
            "type": "string"
            },
            "datetimestamp": {
            "description": "The string type is used for strings of text.",
            "type": "string"
            },
            "fuel_type": {
            "description": "The string type is used for strings of text.",
            "type": "string"
            },
            "lane": {
            "description": "The integer type is used for integral numbers.",
            "type": "integer"
            },
            "license_plate": {
            "description": "The string type is used for strings of text.",
            "type": "string"
            },
            "passenger_count": {
            "description": "The integer type is used for integral numbers.",
            "type": "integer"
            },
            "speed": {
            "description": "The number type is used for any numeric type, either integers or floating point numbers.",
            "type": "number"
            },
            "travel_direction": {
            "description": "The string type is used for strings of text.",
            "type": "string"
            }
        }                
    }
"""


class Car(object):
    """
    Car record

    """

    def __init__(
        self,
        category,
        make,
        model,
        year,
        colour,
        datetimestamp,
        fuel_type,
        lane,
        license_plate,
        passenger_count,
        speed,
        travel_direction,
    ):
        self.category = category
        self.make = make
        self.model = model
        self.year = year
        self.colour = colour
        self.datetimestamp = datetimestamp
        self.fuel_type = fuel_type
        self.lane = lane
        self.license_plate = license_plate
        self.passenger_count = passenger_count
        self.speed = speed
        self.travel_direction = travel_direction


def parse_args():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(
        description="Confluent Python Client example to produce messages to Confluent Cloud"
    )
    parser._action_groups.pop()
    required = parser.add_argument_group("required arguments")
    required.add_argument(
        "-f",
        dest="config_file",
        help="path to Confluent Cloud configuration file",
        required=True,
    )
    required.add_argument("-t", dest="topic", help="topic name", required=True)
    required.add_argument(
        "-d",
        dest="duration",
        help="duration in minutes for script to run",
        required=True,
    )
    args = parser.parse_args()

    return args


def read_ccloud_config(config_file):
    """Read Confluent Cloud configuration for librdkafka clients"""

    conf = {}
    with open(config_file, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split("=", 1)
                conf[parameter] = value.strip()

    return conf


def pop_schema_registry_params_from_config(config):
    """Remove potential Schema Registry related configurations from dictionary"""

    config.pop("schema.registry.url", None)
    config.pop("basic.auth.user.info", None)
    config.pop("basic.auth.credentials.source", None)

    return config


def create_topic(config, topic):
    """
    Create a topic if needed
    Examples of additional admin API functionality:
    https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/adminapi.py
    """

    admin_client_conf = pop_schema_registry_params_from_config(config.copy())
    a = AdminClient(admin_client_conf)

    fs = a.create_topics([NewTopic(topic, num_partitions=2, replication_factor=3)])

    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            logging.info(f"Topic [{topic}] created.")
        except Exception as e:
            # Continue if error code TOPIC_ALREADY_EXISTS, which may be true
            # Otherwise fail fast
            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                logging.info(f"Failed to create topic [{topic}]: {e}.")
                sys.exit(1)


def generate_random_car_object(speed_factor: int) -> object:
    """
    Function to generate a car object.
    """

    # Instanciate faker
    fake = Faker()

    # Add dynamic providers to faker
    fake.add_provider(dynamic_faker_providers.car_colour_provider)
    fake.add_provider(dynamic_faker_providers.car_fuel_type_provider)
    fake.add_provider(dynamic_faker_providers.car_passengers_count_provider)
    fake.add_provider(dynamic_faker_providers.car_travel_direction_provider)
    fake.add_provider(dynamic_faker_providers.car_lane)
    fake.add_provider(VehicleProvider)

    vehicle = fake.vehicle_object()

    car_object = Car(
        category=vehicle["Category"],
        make=vehicle["Make"],
        model=vehicle["Model"],
        year=vehicle["Year"],
        license_plate=fake.license_plate(),
        colour=fake.car_colour(),
        fuel_type=fake.car_fuel_type(),
        passenger_count=fake.car_passengers_count(),
        travel_direction=fake.car_travel_direction(),
        lane=fake.car_lane(),
        speed=int(random.randint(50, 90) + speed_factor),
        datetimestamp=(datetime.utcnow() + timedelta(hours=+8)).strftime(
            "%d/%m/%Y %H:%M:%S.%f"
        ),
    )

    return car_object


def object_to_dict(car_object: object, ctx) -> dict:
    """
    Fucntion to returns a dict representation of a Car instance for serialization.
    """
    return dict(
        category=car_object.category,
        make=car_object.make,
        model=car_object.model,
        year=car_object.year,
        colour=car_object.colour,
        datetimestamp=car_object.datetimestamp,
        fuel_type=car_object.fuel_type,
        lane=car_object.lane,
        license_plate=car_object.license_plate,
        passenger_count=car_object.passenger_count,
        speed=car_object.speed,
        travel_direction=car_object.travel_direction,
    )


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """
    if err is not None:
        logging.warning(f"Failed to deliver record: {msg.key()}:{err}")
    else:
        logging.info(
            f"Record [{msg.key()}] successfully produced to topic [{msg.topic()}] on partition [{msg.partition()}] at offset [{msg.offset()}]."
        )
