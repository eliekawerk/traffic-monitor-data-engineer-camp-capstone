CREATE TABLE IF NOT EXISTS default.raw_traffic
(
    Year                Int,
    Make                String,
    Model               String,
    Category            String,
    license_plate       String,
    colour              String,
    fuel_type           String,
    passenger_count     Int,
    travel_direction    String,
    speed               Int,
    datetimestamp       String
) ENGINE = MergeTree ORDER BY (license_plate, Year, Make, Model, datetimestamp)