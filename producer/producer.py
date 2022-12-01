import os
os.environ['CONDA_DLL_SEARCH_MODIFICATION_ENABLE']='1'

from faker import Faker
from faker.providers import DynamicProvider
from faker_vehicle import VehicleProvider
from datetime import datetime, timedelta
from confluent_kafka.cimpl import Producer
import ccloud_lib
import random
import time
import json

# Configuration
duration_in_minutes = 1

# Prepare additional providers
car_colour_provider = DynamicProvider(
     provider_name="car_colour",
     elements=["white", "sliver", "black", "red", "blue", "yellow","green"],
)

car_fuel_type_provider = DynamicProvider(
     provider_name="car_fuel_type",
     elements=["petrol", "diesel", "electric"],
)

car_passengers_count_provider = DynamicProvider(
     provider_name="car_passengers_count",
     elements=[1, 2, 3, 4, 5]
)

car_travel_direction_provider = DynamicProvider(
     provider_name="car_travel_direction",
     elements=["Northbound", "Southbound", "Eastbound", "Westbound"]
)

# Instanciate faker
fake = Faker()

# Add dynamic providers to faker
fake.add_provider(car_colour_provider)
fake.add_provider(car_fuel_type_provider)
fake.add_provider(car_passengers_count_provider)
fake.add_provider(car_travel_direction_provider)
fake.add_provider(VehicleProvider)

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
        "speed": int(random.randint(50,90) + speed_factor),
        "datetimestamp": datetime.now().strftime("%d/%m/%Y %H:%M:%S.%f")
    }

if __name__ == '__main__':
    
    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Producer instance
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    producer = Producer(producer_conf)

    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

    delivered_records = 0

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    # def acked(err, msg):
    #     global delivered_records
    #     """
    #     Delivery report handler called on successful or failed delivery of message.
    #     """
    #     if err is not None:
    #         print(f"Failed to deliver message: {err}")
    #     else:
    #         delivered_records += 1
    #         # print(f"Produced record to topic {msg.topic()} partition [{msg.partition()}] @ offset {msg.offset}")

    end_datetime = datetime.now() + timedelta(minutes=duration_in_minutes)

    # Loop n times generating car objects simulating cars driving under a gantry
    while datetime.now() < end_datetime:

        # Peak
        if (datetime.now().hour >= 6 and datetime.now().hour <= 7) or (datetime.now().hour >= 16 and datetime.now().hour <= 17):
            time_delay = random.randint(50,90)
            speed_factor = random.uniform(-20,10)
            # print('Peak')
        
        # Off-peak
        elif datetime.now().hour <= 4 or datetime.now().hour >= 22:
            time_delay = random.randint(80,100)
            speed_factor = random.uniform(0,30)
            # print('Off Peak')

        # Shoulder
        else:
            time_delay = random.randint(1,70)
            speed_factor = random.uniform(-10,20)
            # print('Shoulder')

        # print(f'Time Delay: {time_delay} Speed Factor: {speed_factor}')

        car = fake.vehicle_object()
        car_attributes = generate_random_car_attributes(speed_factor=speed_factor)

        # Join car object and attributes into one dictionary
        car_object = dict(car, **car_attributes)
        print(car_object)

        # producer.produce(topic, key=car_object['license_plate'], value=json.dumps(car_object), on_delivery=acked)
        producer.produce(topic, key=car_object['license_plate'], value=json.dumps(car_object))
        # producer.poll(0)
        
        # Time delay between car objects being created
        time.sleep(time_delay * 0.1)
    
    # producer.flush()
