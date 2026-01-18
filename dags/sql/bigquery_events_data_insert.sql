CREATE SCHEMA IF NOT EXISTS `{{ params.GCP_PROJECT }}.{{ params.BIGQUERY_DS }}`
OPTIONS (location = "US");

CREATE OR REPLACE EXTERNAL TABLE
  `{{ params.GCP_PROJECT }}.{{ params.BIGQUERY_EXTERNAL_DS }}.external_{{ params.BIGQUERY_TABLE }}`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://{{ params.GCS_BUCKET_NAME }}/case_data/daily/*_{{ params.SUFFIX }}.parquet']
);

CREATE TABLE IF NOT EXISTS
  `{{ params.GCP_PROJECT }}.{{ params.BIGQUERY_DS }}.{{ params.BIGQUERY_TABLE }}`
(
  request_id STRING,
  funnel_id STRING,
  session_id STRING,
  user_id INT64,
  user_agent STRING,
  device_type STRING,
  ip_address STRING,
  timestamp TIMESTAMP,
  event_date DATE,

  page_name STRING,
  subscriber_id INT64,

  has_email_contact_permission BOOL,
  has_phone_contact_permission BOOL,

  hotel_price_raw STRING,
  hotel_price NUMERIC,

  hotel_id INT64,
  currency STRING,
  country STRING,
  utm_source STRING,
  search_query STRING,
  num_guests INT64,
  destination_id INT64,
  hotel_name STRING,
  hotel_rating FLOAT64,
  selected_room_id INT64,
  nights INT64,
  price_per_night FLOAT64,
  payment_status STRING,
  confirmation_number STRING,

  source_day DATE,
  source_file STRING,
  ingested_at TIMESTAMP
)
PARTITION BY event_date
CLUSTER BY country, page_name, hotel_id, funnel_id;

-- Insert data
INSERT INTO
  `{{ params.GCP_PROJECT }}.{{ params.BIGQUERY_DS }}.{{ params.BIGQUERY_TABLE }}`
(
  request_id, funnel_id, session_id, user_id, user_agent, device_type, ip_address,
  timestamp, event_date, page_name, subscriber_id,
  has_email_contact_permission, has_phone_contact_permission,
  hotel_price_raw, hotel_price,
  hotel_id, currency, country, utm_source, search_query,
  num_guests, destination_id, hotel_name, hotel_rating,
  selected_room_id, nights, price_per_night,
  payment_status, confirmation_number,
  source_day, source_file, ingested_at
)
SELECT
  request_id,
  funnel_id,
  session_id,
  SAFE_CAST(user_id AS INT64) AS user_id,
  user_agent,
  device_type,
  ip_address,

  TIMESTAMP_MICROS(DIV(timestamp, 1000)) AS timestamp,
  DATE(TIMESTAMP_MICROS(DIV(timestamp, 1000))) AS event_date,

  page_name,
  SAFE_CAST(subscriber_id AS INT64) AS subscriber_id,

  CASE
    WHEN LOWER(TRIM(has_email_contact_permission)) IN ('yes','true','1') THEN TRUE
    WHEN LOWER(TRIM(has_email_contact_permission)) IN ('no','false','0') THEN FALSE
    ELSE NULL
  END,

  CASE
    WHEN LOWER(TRIM(has_phone_contact_permission)) IN ('yes','true','1') THEN TRUE
    WHEN LOWER(TRIM(has_phone_contact_permission)) IN ('no','false','0') THEN FALSE
    ELSE NULL
  END,

  hotel_price AS hotel_price_raw,

  SAFE_CAST(
    REPLACE(
      REGEXP_REPLACE(hotel_price, r'[^0-9,.\-]', ''),
      ',', '.'
    ) AS NUMERIC
  ) AS hotel_price,

  SAFE_CAST(hotel_id AS INT64),
  currency,
  country,
  utm_source,
  search_query,
  SAFE_CAST(num_guests AS INT64),
  SAFE_CAST(destination_id AS INT64),
  hotel_name,
  SAFE_CAST(hotel_rating AS FLOAT64),
  SAFE_CAST(selected_room_id AS INT64),
  SAFE_CAST(nights AS INT64),
  SAFE_CAST(price_per_night AS FLOAT64),
  payment_status,
  confirmation_number,

  PARSE_DATE(
    '%Y%m%d',
    REGEXP_EXTRACT(_FILE_NAME, r'(\d{8})_{{ params.SUFFIX }}\.parquet')
  ) AS source_day,

  _FILE_NAME AS source_file,
  CURRENT_TIMESTAMP() AS ingested_at
FROM
  `{{ params.GCP_PROJECT }}.{{ params.BIGQUERY_EXTERNAL_DS }}.external_{{ params.BIGQUERY_TABLE }}`;
