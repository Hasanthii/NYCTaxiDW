USE NYCTaxiDW;
GO

CREATE TABLE stg_trip (
    vendor_id              INT            NULL,
    pickup_datetime        DATETIME2(0)   NULL,
    dropoff_datetime       DATETIME2(0)   NULL,
    passenger_count        INT            NULL,
    trip_distance          DECIMAL(10,2)  NULL,
    rate_code_id           INT            NULL,
    pickup_location_id     INT            NULL,
    dropoff_location_id    INT            NULL,
    payment_type_id        INT            NULL,
    fare_amount            DECIMAL(10,2)  NULL,
    extra                  DECIMAL(10,2)  NULL,
    mta_tax                DECIMAL(10,2)  NULL,
    tip_amount             DECIMAL(10,2)  NULL,
    tolls_amount           DECIMAL(10,2)  NULL,
    improvement_surcharge  DECIMAL(10,2)  NULL,
    congestion_surcharge   DECIMAL(10,2)  NULL,
    airport_fee            DECIMAL(10,2)  NULL,
    total_amount           DECIMAL(10,2)  NULL,
    trip_duration_minutes  DECIMAL(10,2)  NULL,
    pickup_hour            INT            NULL,
    source_file            VARCHAR(100)   NULL
);
GO