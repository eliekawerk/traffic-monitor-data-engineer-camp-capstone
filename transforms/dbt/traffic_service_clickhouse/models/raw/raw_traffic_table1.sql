CREATE TABLE IF NOT EXISTS default.raw_traffic1
(
    Year                Int,
    Make                LowCardinality(String),
    Model               String,
    Category            LowCardinality(String),
    license_plate       String,
    colour              Enum('white' = 1, 'silver' = 2, 'black' = 3, 'red' = 4, 'blue' = 5, 'yellow' = 6, 'green' = 7 ),
    fuel_type           Enum('petrol' = 1, 'diesel' = 2, 'electric' = 3),
    passenger_count     Int,
    travel_direction    Enum('Northbound' = 1, 'Southbound' = 2, 'Eastbound' = 3, 'Westbound' = 4),
    speed               Int,
    datetimestamp       String
) ENGINE = MergeTree ORDER BY (license_plate, Year, Make, Model, datetimestamp)