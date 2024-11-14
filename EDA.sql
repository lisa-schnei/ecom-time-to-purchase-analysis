----------------------------------------------------
------------- PRODUCT ANALYST: PROJECT -------------
----------------------------------------------------

-- Author: Lisa Schneider
-- Date: November 2024
-- Tool used: BigQuery

-- EXPLORATORY ANALYSIS

# 270 154 unique user ids of 4 295 584 total rows
SELECT COUNT (DISTINCT user_pseudo_id) AS unique_ids,
FROM `turing_data_analytics.raw_events`;


# time frame for the data is 2020-11-01 to 2021-01-31.
SELECT MIN(event_date) AS min_date,
MAX(event_date) AS max_date
FROM `turing_data_analytics.raw_events`;

# 17 event categories available in the data 
SELECT DISTINCT event_name,
COUNT (*) AS event_count
FROM `turing_data_analytics.raw_events`
GROUP BY 1
ORDER BY 2 DESC;

# 109 countries are represented in the data
WITH ranked_events AS (
  SELECT *,
  RANK() OVER(PARTITION BY user_pseudo_id, event_name ORDER BY event_timestamp) AS row_num 
  FROM `turing_data_analytics.raw_events`
)

SELECT country,
COUNT(*) country_cnt
FROM ranked_events
WHERE row_num = 1
GROUP BY 1
ORDER BY 2 DESC;


# 3 categories - desktop, mobile, tablet. Most events on desktop, fewest tablet. 
WITH ranked_events AS (
  SELECT *,
  RANK() OVER(PARTITION BY user_pseudo_id, event_name ORDER BY event_timestamp) AS row_num 
  FROM `turing_data_analytics.raw_events`
)

SELECT category,
COUNT(*) category_cnt
FROM ranked_events
WHERE row_num = 1
GROUP BY 1
ORDER BY 2 DESC;


# 5 traffic sources: google, other, direct, shop.googlemerchandisestore.com, data deleted
WITH ranked_events AS (
  SELECT *,
  RANK() OVER(PARTITION BY user_pseudo_id, event_name ORDER BY event_timestamp) AS row_num 
  FROM `turing_data_analytics.raw_events`
)

SELECT traffic_source,
COUNT(*) AS source_cnt
FROM ranked_events
WHERE row_num = 1
GROUP BY 1
ORDER BY 2 DESC;

# Creating final table to be used for further visualisation in Tableau.

WITH purchase_data AS (
  SELECT PARSE_DATE('%Y%m%d', event_date) AS event_date
  , FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MICROS(event_timestamp)) AS event_time
  , MIN(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MICROS(event_timestamp))) 
        OVER (PARTITION BY user_pseudo_id) AS first_event_time
  , user_pseudo_id
  , event_name
  , event_value_in_usd
  , category
  , country
  FROM `turing_data_analytics.raw_events`
),

purchases AS (
  SELECT event_date
    , event_time  
    , first_event_time
    , user_pseudo_id
    , event_name
    , event_value_in_usd
    , category
    , country
    , CASE WHEN event_name = 'purchase' AND 
              DATE(first_event_time) = DATE(event_time) THEN 1 ELSE 0 END AS same_day_purchases
    , CASE WHEN event_name = 'purchase' AND 
              DATE(first_event_time) != DATE(event_time) THEN 1 ELSE 0 END AS different_day_purchases
  FROM purchase_data
  WHERE event_name = 'purchase'
  ORDER BY user_pseudo_id, event_date, event_time
)

SELECT *
, TIMESTAMP_DIFF(TIMESTAMP(event_time), TIMESTAMP(first_event_time), MINUTE) AS time_to_purchase_min
, TIMESTAMP_DIFF(TIMESTAMP(event_time), TIMESTAMP(first_event_time), DAY) AS time_to_purchase_days
FROM purchases;


# Creating a funnel overview table with selected events

WITH ranked_events AS (
  SELECT *,
  RANK() OVER(PARTITION BY user_pseudo_id, event_name ORDER BY event_timestamp) AS row_num 
  FROM `turing_data_analytics.raw_events`
),

funnel_table AS (
  SELECT
  CASE 
    WHEN event_name = 'session_start' THEN 1
    WHEN event_name = 'view_item' THEN 2
    WHEN event_name = 'add_to_cart' THEN 3
    WHEN event_name = 'begin_checkout' THEN 4
    WHEN event_name = 'add_payment_info' THEN 5
    WHEN event_name = 'purchase' THEN 6
    ELSE NULL
  END AS event_order,
  event_name,
  COUNT(DISTINCT user_pseudo_id) AS users
  FROM ranked_events
  WHERE row_num = 1
  GROUP BY 2
)

SELECT *
FROM funnel_table
WHERE event_order BETWEEN 1 AND 6
ORDER BY event_order;


