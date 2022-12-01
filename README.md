# DE-Capstone-2022

# Project plan 

## Objective 
What would you like people to do with the data you have produced? Are you supporting BI or ML use-cases? 

The data will be used to monitor traffic in real-time and for analysis to manage speed and help mitigate congestion on the roads.

<br>

## Consumers 
What users would find your data useful?

Transport and infrastructure planners as well as traffic enforcement.

<br>

## Questions 
What questions are you trying to solve with your data? 

Trying to monitor traffic on the roads to analyse vehicle speed, congestion and passenger count to see if:
* road infrastructure upgrades are required
* additional public transport services are required
* flexi time for workers should be recommended
* additional speed cameras are required 

<br>

## Source datasets 
What datasets are you sourcing from?

We are generating our own data using a python library called Faker which will generate cars and simulate them in traffic driving past a car counter/speed/toll gantry.

<br>

## Solution architecture
How are we going to get data flowing from source to serving? What components and services will we combine to implement the solution?

* Ingestion - a python script Kafka producers running on AWS ECS and Confluent with a Clickhouse http sink connector 
* Data Storage - Clickhouse for raw, staging and serving tables
* Data Modelling - dbt running locally or on AWS ECS
* Orchestration - Not really required. The Ingestion could run indefinitely once the ECS task is manually triggered and I believe the dbt modelling could also be scheduled with an ECS task
* CICD: Github actions if time permits

<br>

## Breakdown of tasks 
How is your project broken down? Who is doing what?

* Generating source data (producer) - Luke
* Containerising producer and ECS deploy - Anoop
* Confluent Source and Sink connector - Luke
* Clickhouse - Anoop
* Dbt-Clickhouse modelling (locally or ECS deploy) - Anoop + Luke
* Dbt Data quality tests - Luke + Anoop
* Dashboard - Luke + Anoop (probably 1 page each)
* CICD - Leaving for last as we first want to ensure we have a demonstratable project

