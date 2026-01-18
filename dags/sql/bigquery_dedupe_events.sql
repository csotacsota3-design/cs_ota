CREATE TABLE IF NOT EXISTS
  `{{ params.GCP_PROJECT }}.{{ params.BIGQUERY_DS }}.{{ params.BIGQUERY_TABLE }}_deduped`
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

DELETE FROM
  `{{ params.GCP_PROJECT }}.{{ params.BIGQUERY_DS }}.{{ params.BIGQUERY_TABLE }}_deduped`
WHERE TRUE;

INSERT INTO
  `{{ params.GCP_PROJECT }}.{{ params.BIGQUERY_DS }}.{{ params.BIGQUERY_TABLE }}_deduped`
SELECT * EXCEPT (rn)
FROM (
  SELECT
    t.*,
    ROW_NUMBER() OVER (
      PARTITION BY
        request_id,
        funnel_id,
        session_id,
        page_name,
        timestamp
      ORDER BY
        ingested_at DESC,
        source_day DESC,
        source_file DESC
    ) AS rn
  FROM
    `{{ params.GCP_PROJECT }}.{{ params.BIGQUERY_DS }}.{{ params.BIGQUERY_TABLE }}` t
)
WHERE rn = 1;
