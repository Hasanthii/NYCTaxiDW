USE NYCTaxiDW;
GO

CREATE OR ALTER PROCEDURE usp_load_fact_trip
    @file_name      VARCHAR(100),
    @rows_raw       INT,
    @rows_clean     INT,
    @rows_rejected  INT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;   -- any error rolls the whole transaction back

    DECLARE @staged  INT;
    DECLARE @orphans INT;
    DECLARE @loaded  INT;

    -- 1. Never load the same file twice
    IF EXISTS (SELECT 1 FROM etl_file_log
               WHERE file_name = @file_name AND status = 'SUCCESS')
        THROW 50001, 'This file was already loaded successfully.', 1;

    -- 2. Staging must hold exactly the clean rows for this file
    SELECT @staged = COUNT(*) FROM stg_trip WHERE source_file = @file_name;
    IF @staged <> @rows_clean
        THROW 50002, 'Staging row count does not match the clean row count.', 1;

    -- 3. Every code must exist in its dimension
    SELECT @orphans = COUNT(*)
    FROM stg_trip s
    WHERE s.source_file = @file_name
      AND (   NOT EXISTS (SELECT 1 FROM dim_vendor       WHERE vendor_key       = s.vendor_id)
           OR NOT EXISTS (SELECT 1 FROM dim_rate_code    WHERE rate_code_key    = s.rate_code_id)
           OR NOT EXISTS (SELECT 1 FROM dim_payment_type WHERE payment_type_key = s.payment_type_id)
           OR NOT EXISTS (SELECT 1 FROM dim_location     WHERE location_key     = s.pickup_location_id)
           OR NOT EXISTS (SELECT 1 FROM dim_location     WHERE location_key     = s.dropoff_location_id)
           OR NOT EXISTS (SELECT 1 FROM dim_date
                          WHERE date_key = YEAR(s.pickup_datetime) * 10000
                                         + MONTH(s.pickup_datetime) * 100
                                         + DAY(s.pickup_datetime)));
    IF @orphans > 0
        THROW 50003, 'Some staging rows have codes that are missing from a dimension.', 1;

    -- 4. Load the fact table and write the log row in ONE transaction
    BEGIN TRANSACTION;

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
            YEAR(s.pickup_datetime) * 10000 + MONTH(s.pickup_datetime) * 100 + DAY(s.pickup_datetime),
            s.vendor_id, s.rate_code_id, s.payment_type_id,
            s.pickup_location_id, s.dropoff_location_id,
            s.pickup_datetime, s.dropoff_datetime, s.pickup_hour,
            TRY_CAST(s.passenger_count AS TINYINT),
            s.trip_distance, s.trip_duration_minutes,
            s.fare_amount, s.extra, s.mta_tax, s.tip_amount, s.tolls_amount,
            s.improvement_surcharge, s.congestion_surcharge, s.airport_fee,
            s.total_amount, s.source_file
        FROM stg_trip s
        WHERE s.source_file = @file_name;

        SET @loaded = @@ROWCOUNT;

        -- remove an old FAILED row for this file (if any), then log the success
        DELETE FROM etl_file_log WHERE file_name = @file_name;

        INSERT INTO etl_file_log
            (file_name, status, rows_raw, rows_clean, rows_rejected, rows_loaded)
        VALUES
            (@file_name, 'SUCCESS', @rows_raw, @rows_clean, @rows_rejected, @loaded);

    COMMIT TRANSACTION;
END;
GO