USE NYCTaxiDW;
GO

CREATE TABLE etl_file_log (
    file_name       VARCHAR(100)  NOT NULL PRIMARY KEY,
    status          VARCHAR(20)   NOT NULL,   -- SUCCESS or FAILED
    rows_raw        INT           NULL,
    rows_clean      INT           NULL,
    rows_rejected   INT           NULL,
    rows_loaded     INT           NULL,
    loaded_at       DATETIME2(0)  NOT NULL DEFAULT SYSDATETIME(),
    error_message   VARCHAR(500)  NULL
);
GO

-- Record the January file we already loaded by hand
INSERT INTO etl_file_log
    (file_name, status, rows_raw, rows_clean, rows_rejected, rows_loaded)
VALUES
    ('yellow_tripdata_2024-01.parquet', 'SUCCESS', 2964624, 2869641, 94983, 2869641);
GO

SELECT * FROM etl_file_log;
GO