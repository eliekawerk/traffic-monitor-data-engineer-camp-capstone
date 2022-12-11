# DE-Capstone-2022
This project showcases the ingestion of a live mock traffic dataset with a analytical (reporting) use-case.

It is entirely cloud-hosted, and includes the components shown in the diagram below.

## Solution Architecture Diagram
![arch-capstone (3)](https://user-images.githubusercontent.com/106643739/206931961-db43b67b-1b96-4dc8-8a53-8c7571a6e8b2.png)

* Ingestion - a python-based Kafka producer running on a Linux based AWS EC2 instance and Confluent with a Clickhouse http sink connector 
* Data Storage - Clickhouse for raw, staging and serving tables
* Data Modelling - dbt running on GitHub Actions, which also facilitates CI/CD
* Data Quality Testing - dbt running on GitHub Actions
* Reporting - Preset Dashboards
* CI/CD via Github actions

## Use-case 
The data will be used to monitor traffic in real-time and for analysis to manage speed and help mitigate congestion on the roads.

## Consumers 
Transport and infrastructure planners as well as traffic enforcement.

## Exploratory analysis 
Monitoring traffic on the roads to analyse vehicle speed, congestion and passenger count to ascertain if:
* road infrastructure upgrades are required
* additional public transport services are required
* flexible arrangements for workers should be recommended
* additional speed cameras are required

## Source datasets 
The live data source is generated using a python library called Faker which will generate cars and simulate them in traffic driving past a car counter/speed/toll gantry. Check out https://faker.readthedocs.io/en/master/ for more details.

This project used the Faker library as a base to improve upon the practicality of the data, for example allowing for fluctuations of traffic amounts and speed during peak hours.

## Transformations
The data is ingested in the ELT technique into Clickhouse where the data is transformed through raw/staging/serving layers via dbt modelling. This dbt project is run via GitHub actions. The serving layers are intended for output to Preset Dashboards for consumption by end users.

If you are running this project from scratch, run this script on Clickhouse to set up the raw table:

```
CREATE TABLE IF NOT EXISTS default.raw_traffic
(
    Year                UInt32,
    Make                LowCardinality(String),
    Model               String,
    Category            LowCardinality(String),
    license_plate       String,
    colour              Enum('white' = 1, 'sliver' = 2, 'black' = 3, 'red' = 4, 'blue' = 5, 'yellow' = 6, 'green' = 7),
    fuel_type           Enum('electric' = 1,'petrol' = 2,'diesel' = 3),
    passenger_count     UInt32,
    travel_direction    Enum('Northbound' = 1,'Westbound' = 2,'Southbound' = 3, 'Eastbound' = 4),
    speed               UInt32,
    datetimestamp       String
) ENGINE = MergeTree ORDER BY (license_plate, Year, Make, Model, datetimestamp)
```

## Confluent schema registry
The expected schema was registered in Confluent to enable interfacing with external sources and sinks. A stream catalog API endpoint was set up. This enables developers to query the endpoint to view the latest schema.

Endpoint <br>
<img width="192" alt="shot_221212_093546" src="https://user-images.githubusercontent.com/106643739/206933121-f2f7eef6-3e3f-48e6-9c59-6ae1885ba0be.png">

Schema <br>
<img width="377" alt="shot_221212_093815" src="https://user-images.githubusercontent.com/106643739/206933128-88689ee8-fcbd-462d-9838-6f099fc295a2.png">

Examples <br>
<img width="245" alt="shot_221212_093840" src="https://user-images.githubusercontent.com/106643739/206933134-e8b4ba66-41fa-4c7b-8749-213fd810fe90.png">

# Results

## Preset dashboards

<img width="777" alt="shot_221212_092809" src="https://user-images.githubusercontent.com/106643739/206932565-118fd4a6-dbff-4439-ad62-a17799a87c52.png">

<img width="791" alt="shot_221212_092927" src="https://user-images.githubusercontent.com/106643739/206932607-68fafa93-5727-4406-b5c1-e29294e54a65.png">

<img width="794" alt="shot_221212_093014" src="https://user-images.githubusercontent.com/106643739/206932771-e0116818-6d35-4527-b377-8d946aab811d.png">

<img width="794" alt="shot_221212_093059" src="https://user-images.githubusercontent.com/106643739/206932682-82b2eb8e-ffd9-49b0-8493-b01842cfa245.png">
