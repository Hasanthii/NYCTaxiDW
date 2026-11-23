USE NYCTaxiDW;
GO

SET DATEFIRST 1;  -- week starts on Monday: Monday = 1 ... Sunday = 7

WITH dates AS (
    SELECT CAST('2024-01-01' AS DATE) AS d
    UNION ALL
    SELECT DATEADD(DAY, 1, d)
    FROM dates
    WHERE d < '2026-12-31'
)
INSERT INTO dim_date (
    date_key, full_date, day_of_month, month_number, month_name,
    quarter_number, year_number, day_of_week_number, day_name, is_weekend
)
SELECT
    YEAR(d) * 10000 + MONTH(d) * 100 + DAY(d),
    d,
    DAY(d),
    MONTH(d),
    DATENAME(MONTH, d),
    DATEPART(QUARTER, d),
    YEAR(d),
    DATEPART(WEEKDAY, d),
    DATENAME(WEEKDAY, d),
    CASE WHEN DATEPART(WEEKDAY, d) IN (6, 7) THEN 1 ELSE 0 END
FROM dates
OPTION (MAXRECURSION 2000);
GO