# Assignment 03 Responses

## Part 4: BigQuery External Tables

### Hourly Observations — CSV External Table SQL

```sql
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
    format            = 'CSV',
    skip_leading_rows = 1,
    uris              = ['gs://kavanaraju_geocloud_bucket/air_quality/hourly/*.csv']
);
```

### Hourly Observations — JSON-L External Table SQL

```sql
CREATE OR REPLACE EXTERNAL TABLE `air_quality.hourly_observations_jsonl`
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
    format = 'NEWLINE_DELIMITED_JSON',
    uris   = ['gs://kavanaraju_geocloud_bucket/air_quality/hourly/*.jsonl']
);
```

### Hourly Observations — Parquet External Table SQL

```sql
CREATE OR REPLACE EXTERNAL TABLE `air_quality.hourly_observations_parquet`
OPTIONS (
    format = 'PARQUET',
    uris   = ['gs://kavanaraju_geocloud_bucket/air_quality/hourly/*.parquet']
);
```

### Site Locations — CSV External Table SQL

```sql
CREATE OR REPLACE EXTERNAL TABLE `air_quality.site_locations_csv`
OPTIONS (
    format            = 'CSV',
    skip_leading_rows = 1,
    autodetect        = TRUE,
    uris              = ['gs://kavanaraju_geocloud_bucket/air_quality/sites/site_locations.csv']
);
```

### Site Locations — JSON-L External Table SQL

```sql
CREATE OR REPLACE EXTERNAL TABLE `air_quality.site_locations_jsonl`
OPTIONS (
    format     = 'NEWLINE_DELIMITED_JSON',
    autodetect = TRUE,
    uris       = ['gs://kavanaraju_geocloud_bucket/air_quality/sites/site_locations.jsonl']
);
```

### Site Locations — GeoParquet External Table SQL

```sql
-- BigQuery reads GeoParquet as standard Parquet; the geometry column
-- is stored as WKB bytes and can be cast to GEOGRAPHY using ST_GeogFromWKB.
CREATE OR REPLACE EXTERNAL TABLE `air_quality.site_locations_geoparquet`
OPTIONS (
    format = 'PARQUET',
    uris   = ['gs://kavanaraju_geocloud_bucket/air_quality/sites/site_locations.geoparquet']
);
```

### Cross-Table Join Query

```sql
-- Average PM2.5 value by state for July 1, 2024
SELECT
    s.stateabbreviation        AS state,
    ROUND(AVG(h.value), 3)    AS avg_pm25,
    COUNT(*)                  AS observation_count
FROM `air_quality.hourly_observations_parquet` AS h
JOIN `air_quality.site_locations_csv` AS s
    ON h.aqsid = s.aqsid
WHERE h.valid_date = '07/01/24'
    AND h.parameter_name = 'PM2.5'
GROUP BY state
ORDER BY avg_pm25 DESC;
```

---

## Part 5: Hive-Partitioned External Tables

### Hourly Observations — CSV (hive-partitioned)

```sql
CREATE OR REPLACE EXTERNAL TABLE `air_quality.hourly_observations_csv_hive`
WITH PARTITION COLUMNS (
    airnow_date DATE
)
OPTIONS (
    format                    = 'CSV',
    skip_leading_rows         = 1,
    uris                      = ['gs://kavanaraju_geocloud_bucket/air_quality/hourly/csv/*'],
    hive_partition_uri_prefix = 'gs://kavanaraju_geocloud_bucket/air_quality/hourly/csv'
);
```

### Hourly Observations — JSON-L (hive-partitioned)

```sql
CREATE OR REPLACE EXTERNAL TABLE `air_quality.hourly_observations_jsonl_hive`
WITH PARTITION COLUMNS (
    airnow_date DATE
)
OPTIONS (
    format                    = 'NEWLINE_DELIMITED_JSON',
    uris                      = ['gs://kavanaraju_geocloud_bucket/air_quality/hourly/jsonl/*'],
    hive_partition_uri_prefix = 'gs://kavanaraju_geocloud_bucket/air_quality/hourly/jsonl'
);
```

### Hourly Observations — Parquet (hive-partitioned)

```sql
CREATE OR REPLACE EXTERNAL TABLE `air_quality.hourly_observations_parquet_hive`
WITH PARTITION COLUMNS (
    airnow_date DATE
)
OPTIONS (
    format                    = 'PARQUET',
    uris                      = ['gs://kavanaraju_geocloud_bucket/air_quality/hourly/parquet/*'],
    hive_partition_uri_prefix = 'gs://kavanaraju_geocloud_bucket/air_quality/hourly/parquet'
);
```

---

## Part 6: Analysis & Reflection

### 1. File Sizes

**Hourly data (single day — 2024-07-01):**

| Format  | File Size |
|---------|-----------|
| CSV     | 18 MB     |
| JSON-L  | 42 MB     |
| Parquet | 640 KB    |

**Site locations:**

| Format     | File Size |
|------------|-----------|
| CSV        | 1.0 MB    |
| JSON-L     | 2.9 MB    |
| GeoParquet | 476 KB    |

**Analysis:**

Parquet is by far the smallest format. It stores data in columns rather than rows, which means all values for a given field are stored together and can be compressed very efficiently — repeated strings like `parameter_name` or `data_source` compress dramatically. Parquet also stores type metadata, so numeric values like `value` are stored as binary floats rather than ASCII characters.

JSON-L is the largest because it repeats every field name for every single row. A file with 175,000 rows repeats `"valid_date"`, `"valid_time"`, `"aqsid"`, etc. 175,000 times each. This key-per-value overhead more than doubles the size compared to CSV.

CSV sits in the middle — it encodes column names once in the header and then stores plain text values, but it uses no compression and represents all values (including numbers) as human-readable strings.

### 2. Format Anatomy

**CSV vs. Parquet:**

CSV (Comma-Separated Values) is a plain-text, row-oriented format. The first row is a header that names each column; every subsequent row is a record, with values separated by commas (or another delimiter like the pipe `|` used in the raw AirNow files). CSV has no built-in type information — every value is a string unless the consumer parses it. It is human-readable and universally supported, which makes it a reliable lowest-common-denominator format.

Parquet is a binary, columnar format. Rather than storing record 1 then record 2, it stores all values for column A, then all values for column B. Each column is compressed independently using algorithms (like Snappy or Zstd) that exploit the repetition and statistical structure within a single column. Parquet files carry a schema that specifies types (INTEGER, FLOAT, STRING, etc.), so consumers don't have to guess or parse. Parquet is not human-readable but is purpose-built for analytical queries: if a query only needs two of nine columns, BigQuery can skip the other seven entirely at the storage layer.

### 3. Choosing Formats for BigQuery

Parquet is generally preferred over CSV or JSON-L for two reasons: **performance** and **cost**.

**Performance:** BigQuery is a columnar engine. When you run `SELECT AVG(value) FROM hourly_observations WHERE parameter_name = 'PM2.5'`, it only needs two columns out of nine. With Parquet, BigQuery reads only those two columns from storage. With CSV or JSON-L, it must read every byte of every row and then discard the columns it doesn't need. Parquet queries return faster because they transfer far less data from storage to compute.

**Cost:** BigQuery charges for the number of bytes scanned per query. Since Parquet files are compressed and BigQuery can skip irrelevant columns, a query against the Parquet table might scan 640 KB per day instead of 18 MB per day for CSV — roughly a 28× cost reduction for the same result. Over a month of data and many queries, that difference is significant.

### 4. Pipeline vs. Warehouse Joins

In this assignment, hourly observations and site locations are kept as separate tables and joined in BigQuery at query time. An alternative is to join them during the `prepare` step — embedding latitude, longitude, and other site metadata into every observation row before upload (denormalization).

**Joining at query time (what we did):**
- Pros: No data duplication. Site metadata is stored once and shared across all observations. If a monitoring site's coordinates or metadata change, only the sites table needs updating. The prepare step is simpler and faster. Storage is much smaller — site metadata is not replicated across 175,000+ observation rows per day.
- Cons: Every analytical query must include a JOIN. Queries are slightly more complex to write.

**Joining during prepare (denormalization):**
- Pros: Simpler queries — latitude, longitude, and state are already in every observation row. Potentially slightly faster reads since no join is needed at query time.
- Cons: Dramatically larger files — all site metadata is duplicated for every observation. If site information is corrected or updated, you must re-run the prepare step for all historical dates. Violates the principle of single source of truth.

For this dataset, keeping them separate is the better design. Site metadata changes rarely, observations change daily, and the join is cheap in BigQuery. Denormalization would make sense only if the join were prohibitively expensive or if the query interface didn't support joins at all.

### 5. Choosing a Data Source

**a) A parent who wants a dashboard showing current air quality near their child's school:**

I would recommend building a pipeline on top of **AirNow hourly file downloads** (like this assignment), not the AirNow API directly. The dashboard could serve many users simultaneously, and querying the AirNow API on each page load would quickly hit rate limits and put unnecessary load on a public service. By downloading the hourly files on a schedule (e.g., every hour), loading them into BigQuery, and serving the dashboard from there, the parent gets current data without each view triggering a new upstream API call.

**b) An environmental justice advocate identifying neighborhoods with chronically poor air quality over the past decade:**

I would recommend **AQS bulk downloads**. AQS publishes quality-assured, QA/QC-reviewed historical data going back decades — exactly what's needed for a credible longitudinal analysis. AirNow data is near-real-time and not designed for long-term trend analysis; it lacks the rigorous quality review that makes AQS data suitable for advocacy and policy work. The AQS bulk CSV downloads can be loaded into BigQuery for analysis without hitting API rate limits.

**c) A school administrator who needs automated morning alerts when AQI exceeds a threshold:**

I would recommend the **AirNow API**. This is a targeted, low-volume use case: one query per morning for a specific location. The AirNow API is designed for exactly this kind of operational query, and a single daily check is well within its rate limits. There's no need to build a full pipeline for a use case that generates one request per day — the API is simpler, more direct, and appropriate here.
