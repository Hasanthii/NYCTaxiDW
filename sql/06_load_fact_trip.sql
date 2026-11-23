USE NYCTaxiDW;
GO

-- 1. PRE-CHECK: staging rows with no matching dimension row (expect all 0)
SELECT
    SUM(CASE WHEN v.vendor_key        IS NULL THEN 1 ELSE 0 END) AS missing_vendor,
    SUM(CASE WHEN r.rate_code_key     IS NULL THEN 1 ELSE 0 END) AS missing_rate_code,
    SUM(CASE WHEN p.payment_type_key  IS NULL THEN 1 ELSE 0 END) AS missing_payment_type,
    SUM(CASE WHEN pl.location_key     IS NULL THEN 1 ELSE 0 END) AS missing_pickup_loc,
    SUM(CASE WHEN dl.location_key     IS NULL THEN 1 ELSE 0 END) AS missing_dropoff_loc,
    SUM(CASE WHEN d.date_key          IS NULL THEN 1 ELSE 0 END) AS missing_date
FROM stg_trip s
LEFT JOIN dim_vendor       v  ON s.vendor_id           = v.vendor_key
LEFT JOIN dim_rate_code    r  ON s.rate_code_id        = r.rate_code_key
LEFT JOIN dim_payment_type p  ON s.payment_type_id     = p.payment_type_key
LEFT JOIN dim_location     pl ON s.pickup_location_id  = pl.location_key
LEFT JOIN dim_location     dl ON s.dropoff_location_id = dl.location_key
LEFT JOIN dim_date         d  ON d.date_key =
      YEAR(s.pickup_datetime) * 10000
    + MONTH(s.pickup_datetime) * 100
    + DAY(s.pickup_datetime);
GO

-- 2. LOAD: insert staging rows into the fact table (skips files already loaded)
INSERT INTO fact_trip (
    pickup_date_key, vendor_key, rate_code_key, payment_type_key,
    pickup_location_key, dropoff_location_key,
    pickup_datetime, dropoff_datetime, pickup_hour,
    passenger_count, trip_distance, trip_duration_minutes,
    fare_amount, extra, mta_tax, tip_amount, tolls_amount,
    improvement_surcharge, congestion_surcharge, airport_fee,
    total_amount, source_file
)
SELECT
    YEAR(s.pickup_datetime) * 10000
      + MONTH(s.pickup_datetime) * 100
      + DAY(s.pickup_datetime),
    s.vendor_id, s.rate_code_id, s.payment_type_id,
    s.pickup_location_id, s.dropoff_location_id,
    s.pickup_datetime, s.dropoff_datetime, s.pickup_hour,
    TRY_CAST(s.passenger_count AS TINYINT),
    s.trip_distance, s.trip_duration_minutes,
    s.fare_amount, s.extra, s.mta_tax, s.tip_amount, s.tolls_amount,
    s.improvement_surcharge, s.congestion_surcharge, s.airport_fee,
    s.total_amount, s.source_file
FROM stg_trip s
WHERE NOT EXISTS (
    SELECT 1 FROM fact_trip f WHERE f.source_file = s.source_file
);
GO

-- 3. RECONCILE: staging and fact should match
SELECT 'stg_trip'  AS table_name, COUNT(*) AS row_count, SUM(total_amount) AS total_revenue FROM stg_trip
UNION ALL
SELECT 'fact_trip', COUNT(*), SUM(total_amount) FROM fact_trip;
GO