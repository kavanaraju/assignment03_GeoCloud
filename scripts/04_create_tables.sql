-- Part 4: Create BigQuery external tables
--
-- Create these tables in a dataset named `air_quality`.
-- Use wildcard URIs for the hourly data tables so a single table
-- spans all 31 days of files.
--
-- After creating the tables, verify they work by running:
--     SELECT count(*) FROM air_quality.<table_name>;


-- Hourly Observations — CSV
CREATE OR REPLACE EXTERNAL TABLE `air_quality.hourly_observations_csv`
(
    valid_date      STRING,
    valid_time      STRING,
    aqsid           STRING,
    site_name       STRING,
    gmt_offset      INTEGER,
    parameter_name  STRING,
    reporting_units STRING,
    value           FLOAT64,
    data_source     STRING
)
OPTIONS (
    format           = 'CSV',
    skip_leading_rows = 1,
    uris             = ['gs://kavanaraju_geocloud_bucket/air_quality/hourly/*.csv']
);


-- Hourly Observations — JSON-L
CREATE OR REPLACE EXTERNAL TABLE `air_quality.hourly_observations_jsonl`
(
    valid_date      STRING,
    valid_time      STRING,
    aqsid           STRING,
    site_name       STRING,
    gmt_offset      FLOAT64,
    parameter_name  STRING,
    reporting_units STRING,
    value           FLOAT64,
    data_source     STRING
)
OPTIONS (
    format = 'NEWLINE_DELIMITED_JSON',
    uris   = ['gs://kavanaraju_geocloud_bucket/air_quality/hourly/*.jsonl']
);


-- Hourly Observations — Parquet
CREATE OR REPLACE EXTERNAL TABLE `air_quality.hourly_observations_parquet`
OPTIONS (
    format = 'PARQUET',
    uris   = ['gs://kavanaraju_geocloud_bucket/air_quality/hourly/*.parquet']
);


-- Site Locations — CSV
CREATE OR REPLACE EXTERNAL TABLE `air_quality.site_locations_csv`
OPTIONS (
    format            = 'CSV',
    skip_leading_rows = 1,
    autodetect        = TRUE,
    uris              = ['gs://kavanaraju_geocloud_bucket/air_quality/sites/site_locations.csv']
);


-- Site Locations — JSON-L
CREATE OR REPLACE EXTERNAL TABLE `air_quality.site_locations_jsonl`
OPTIONS (
    format     = 'NEWLINE_DELIMITED_JSON',
    autodetect = TRUE,
    uris       = ['gs://kavanaraju_geocloud_bucket/air_quality/sites/site_locations.jsonl']
);


-- Site Locations — GeoParquet
-- Note: BigQuery reads GeoParquet as standard Parquet; the geometry column
-- is stored as WKB bytes and can be cast to GEOGRAPHY using ST_GeogFromWKB.
CREATE OR REPLACE EXTERNAL TABLE `air_quality.site_locations_geoparquet`
OPTIONS (
    format = 'PARQUET',
    uris   = ['gs://kavanaraju_geocloud_bucket/air_quality/sites/site_locations.geoparquet']
);


-- Cross-table join query
-- Average PM2.5 value by state for July 1, 2024,
-- joining hourly observations with site locations to get coordinates.
SELECT
    s.stateabbreviation                  AS state,
    ROUND(AVG(h.value), 3)              AS avg_pm25,
    COUNT(*)                            AS observation_count
FROM `air_quality.hourly_observations_parquet` AS h
JOIN `air_quality.site_locations_csv` AS s
    ON h.aqsid = s.aqsid
WHERE h.valid_date = '07/01/24'
    AND h.parameter_name = 'PM2.5'
GROUP BY state
ORDER BY avg_pm25 DESC;
