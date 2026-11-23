USE NYCTaxiDW;
GO

-- Every check returns the number of rows that FAIL it. Expected result: 0.

SELECT 'Log rows_loaded does not match fact_trip rows' AS check_name,
       COUNT(*) AS failed
FROM etl_file_log l
LEFT JOIN (SELECT source_file, COUNT(*) AS n
           FROM fact_trip GROUP BY source_file) f
       ON f.source_file = l.file_name
WHERE l.status = 'SUCCESS' AND ISNULL(f.n, -1) <> l.rows_loaded

UNION ALL
SELECT 'Pickup date outside the month in the file name',
       COUNT(*)
FROM fact_trip
WHERE CONVERT(VARCHAR(7), pickup_datetime, 120) <> SUBSTRING(source_file, 17, 7)

UNION ALL
SELECT 'Stored pickup hour differs from pickup_hour column (time shift)',
       COUNT(*)
FROM fact_trip
WHERE DATEPART(HOUR, pickup_datetime) <> pickup_hour

UNION ALL
SELECT 'pickup_date_key does not match pickup_datetime',
       COUNT(*)
FROM fact_trip
WHERE pickup_date_key <>
      YEAR(pickup_datetime) * 10000 + MONTH(pickup_datetime) * 100 + DAY(pickup_datetime)

UNION ALL
SELECT 'Dropoff before pickup',
       COUNT(*)
FROM fact_trip
WHERE dropoff_datetime < pickup_datetime

UNION ALL
SELECT 'Fare, distance or total amount is zero or negative',
       COUNT(*)
FROM fact_trip
WHERE fare_amount <= 0 OR trip_distance <= 0 OR total_amount <= 0

UNION ALL
SELECT 'Duplicate source_file values in etl_file_log',
       COUNT(*)
FROM (SELECT file_name FROM etl_file_log
      GROUP BY file_name HAVING COUNT(*) > 1) d;
GO