# DE-Capstone-2022
This project showcases the ingestion of a live mock traffic dataset with a analytical (reporting) use-case.

It is entirely cloud-hosted, and includes the components shown in the diagram below.

## Solution Architecture Diagram
![arch-capstone](https://user-images.githubusercontent.com/106643739/206892676-af59a22b-586a-4736-8608-280e82aa026e.png)

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

## Results
<Preset dashboards>
