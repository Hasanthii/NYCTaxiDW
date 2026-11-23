USE NYCTaxiDW;
GO

CREATE TABLE fact_trip (
    trip_key               BIGINT         IDENTITY(1,1) PRIMARY KEY,

    -- Foreign keys to dimensions
    pickup_date_key        INT            NOT NULL REFERENCES dim_date(date_key),
    vendor_key             INT            NOT NULL REFERENCES dim_vendor(vendor_key),
    rate_code_key          INT            NOT NULL REFERENCES dim_rate_code(rate_code_key),
    payment_type_key       INT            NOT NULL REFERENCES dim_payment_type(payment_type_key),
    pickup_location_key    INT            NOT NULL REFERENCES dim_location(location_key),
    dropoff_location_key   INT            NOT NULL REFERENCES dim_location(location_key),

    -- Time details
    pickup_datetime        DATETIME2(0)   NOT NULL,
    dropoff_datetime       DATETIME2(0)   NOT NULL,
    pickup_hour            TINYINT        NOT NULL,

    -- Measures
    passenger_count        TINYINT        NULL,
    trip_distance          DECIMAL(10,2)  NOT NULL,
    trip_duration_minutes  DECIMAL(10,2)  NOT NULL,
    fare_amount            DECIMAL(10,2)  NOT NULL,
    extra                  DECIMAL(10,2)  NOT NULL,
    mta_tax                DECIMAL(10,2)  NOT NULL,
    tip_amount             DECIMAL(10,2)  NOT NULL,
    tolls_amount           DECIMAL(10,2)  NOT NULL,
    improvement_surcharge  DECIMAL(10,2)  NOT NULL,
    congestion_surcharge   DECIMAL(10,2)  NULL,
    airport_fee            DECIMAL(10,2)  NULL,
    total_amount           DECIMAL(10,2)  NOT NULL,

    -- Which file the row came from
    source_file            VARCHAR(100)   NOT NULL
);
GO