{{ config(
    materialized = "table",
    engine = "MergeTree",
    order_by = [ "Make" ]
) }}

SELECT distinct Make from raw_traffic