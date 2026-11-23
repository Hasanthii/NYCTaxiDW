USE NYCTaxiDW;
GO

CREATE TABLE dim_date (
    date_key            INT          NOT NULL PRIMARY KEY,  -- e.g. 20240115
    full_date           DATE         NOT NULL,
    day_of_month        TINYINT      NOT NULL,
    month_number        TINYINT      NOT NULL,
    month_name          VARCHAR(10)  NOT NULL,
    quarter_number      TINYINT      NOT NULL,
    year_number         SMALLINT     NOT NULL,
    day_of_week_number  TINYINT      NOT NULL,
    day_name            VARCHAR(10)  NOT NULL,
    is_weekend          BIT          NOT NULL
);

CREATE TABLE dim_vendor (
    vendor_key   INT          NOT NULL PRIMARY KEY,
    vendor_name  VARCHAR(50)  NOT NULL
);

CREATE TABLE dim_rate_code (
    rate_code_key   INT          NOT NULL PRIMARY KEY,
    rate_code_name  VARCHAR(50)  NOT NULL
);

CREATE TABLE dim_payment_type (
    payment_type_key   INT          NOT NULL PRIMARY KEY,
    payment_type_name  VARCHAR(50)  NOT NULL
);

CREATE TABLE dim_location (
    location_key  INT           NOT NULL PRIMARY KEY,  -- TLC zone ID
    borough       VARCHAR(50)   NULL,
    zone_name     VARCHAR(100)  NULL,
    service_zone  VARCHAR(50)   NULL
);
GO