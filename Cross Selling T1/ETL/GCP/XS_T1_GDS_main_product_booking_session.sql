#standardSQL
/* 
Creator : royan.aldian@traveloka.com
Destination table: `tvlk-data-user-dev.xs_playground.XS_T1_GDS_main_product_booking_session`
Output Field: 
    * partition_date,
    * booking_id,
    * session_id,
    * main_product
    * locale,
    * fare_currency,
    * fare_amount
Data Processing:
    * ~78MB data processed per month
    * ~11M rows inserted per month
    * 1.8 sec for elapsed time per query hit (daily)
Est Cost Processing: 
    * Streaming Insert: $0.05/1GB * 0.078GB = $0.0039
    * Queries: $0,005/1GB * 0.078GB = $0.00039
*/

-- CREATE OR REPLACE TABLE `tvlk-data-user-dev.xs_playground.XS_T1_L3_main_product_booking_session`
-- PARTITION BY (partition_date) AS 

WITH
  session_base AS (
  SELECT
    * EXCEPT(rn)
  FROM (
    SELECT
      'FLIGHT' AS product,
      DATE(kafka_publish_timestamp) AS partition_date,
      booking_id,
      session_id,
      ROW_NUMBER() OVER (PARTITION BY booking_id ORDER BY timestamp) AS rn
    FROM
      `tvlk-realtime.nrtprod.flight_booking_submitted`
    WHERE
      _PARTITIONTIME = CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS TIMESTAMP) )
--       DATE(_PARTITIONTIME) BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
--       AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) )
  WHERE
    rn = 1
  UNION ALL
  SELECT
    * EXCEPT(rn)
  FROM (
    SELECT
      'TRIP' AS product,
      DATE(kafka_publish_timestamp) AS partition_date,
      booking_id,
      session_id,
      ROW_NUMBER() OVER (PARTITION BY booking_id ORDER BY client_timestamp) AS rn
    FROM
      `tvlk-realtime.nrtprod.trip_booking_submitted`
    WHERE
      _PARTITIONTIME = CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS TIMESTAMP) )
--       DATE(_PARTITIONTIME) BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
--       AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) )
  WHERE
    rn = 1
  UNION ALL
  SELECT
    * EXCEPT(rn)
  FROM (
    SELECT
      'HOTEL' AS product,
      DATE(kafka_publish_timestamp) AS partition_date,
      booking_id,
      session_id,
      ROW_NUMBER() OVER (PARTITION BY booking_id ORDER BY timestamp) AS rn
    FROM
      `tvlk-realtime.nrtprod.hotel_booking`
    WHERE
      _PARTITIONTIME = CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS TIMESTAMP) )
--       DATE(_PARTITIONTIME) BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
--       AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) )
  WHERE
    rn = 1
  UNION ALL
  SELECT
    * EXCEPT(rn)
  FROM (
    SELECT
      'TRAIN' AS product,
      DATE(kafka_publish_timestamp) AS partition_date,
      bookingid AS booking_id,
      session_id,
      ROW_NUMBER() OVER (PARTITION BY bookingid ORDER BY timestamp) AS rn
    FROM
      `tvlk-realtime.nrtprod.train_booking`
    WHERE
      _PARTITIONTIME = CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS TIMESTAMP) )
--       DATE(_PARTITIONTIME) BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
--       AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) )
  WHERE
    rn = 1
  UNION ALL
  SELECT
    * EXCEPT(rn)
  FROM (
    SELECT
      'BUS' AS product,
      DATE(kafka_publish_timestamp) AS partition_date,
      CAST(booking_id AS INT64) AS booking_id,
      session_id,
      ROW_NUMBER() OVER (PARTITION BY booking_id ORDER BY kafka_publish_timestamp) AS rn
    FROM
      `tvlk-realtime.nrtprod.bus`
    WHERE
      _PARTITIONTIME = CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS TIMESTAMP)
--       DATE(_PARTITIONTIME) BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -6 MONTH)
--       AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
      AND page_event = 'BOOKING')
  WHERE
    rn = 1),
    
  booking_base AS (
  SELECT
    * EXCEPT (rn)
  FROM (
    SELECT
      booking_id,
      primary_sales_product_type as product,
      locale,
      primary_sales_currency AS fare_currency,
      primary_sales_amount AS fare_amount,
      ROW_NUMBER() OVER(PARTITION BY booking_id ORDER BY invoice_update_timestamp) AS rn
    FROM
      `tvlk-data-mkt-prod.datamart.sales_table`
    WHERE
      DATE(_PARTITIONTIME) = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
--       DATE(_PARTITIONTIME) BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
--       AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
      AND booking_status = 'ISSUED')
  WHERE
    rn = 1
  )
    
    
SELECT
  partition_date,
  booking_id,
  session_id,
  CASE
    WHEN product = 'TRIP' THEN 'FLIGHT_HOTEL'
  ELSE
  product
END
  AS main_product,
  locale,
  fare_currency,
  fare_amount
FROM
  session_base AS S
INNER JOIN
  booking_base AS B
USING
  (booking_id,
    product)