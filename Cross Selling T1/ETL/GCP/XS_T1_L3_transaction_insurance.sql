#standardSQL
/* 
Creator : royan.aldian@traveloka.com
Destination table: `tvlk-data-user-dev.xs_playground.XS_T1_L3_transaction_insurance`
Output Field: 
    * transaction_dt 
    * profile_id  
    * booking_id  
    * country 
    * interface 
    * session_id  
    * main_product  
    * secondary_product
    * channel 
    * currency
    * fare_amount
    * gbv_lcy
    * net_rev_lcy
    * gbv_usd 
    * net_rev_usd
Data Processing:
    * ~30GB data processed per month
    * ~260K rows inserted per month
    * 2.6 sec for elapsed time per query hit (daily)
Est Cost Processing: 
    * Streaming Insert: $0.05/1GB * 30GB = $1.5
    * Queries: $0,005/1GB * 30GB = $0.015
*/

-- CREATE OR REPLACE TABLE `tvlk-data-user-dev.xs_playground.XS_T1_L3_transaction_insurance`
-- PARTITION BY (transaction_dt) AS 

WITH
  base_data AS(
  WITH
    conversion AS (
    SELECT
      TIMESTAMP_MILLIS(approved_timestamp) AS approved_timestamp,
      TIMESTAMP_MILLIS(lead_timestamp) AS lead_timestamp,
      source_currency,
      exchange_rates AS exchange_rates
    FROM
      `tvlk-data-user-dev.user_master_datamart.to_usd_conversion` ),
      
    conversion_avg_day_7 AS (
    SELECT
      source_currency AS source_currency_avg,
      AVG(exchange_rates) AS exchange_rates_avg,
    FROM
      `tvlk-data-user-dev.user_master_datamart.to_usd_conversion`
    WHERE
      DATE(TIMESTAMP_MILLIS(approved_timestamp)) BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -7 DAY)
      AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
    GROUP BY
      1 ),
      
    booking_session AS (
    SELECT
      booking_id,
      session_id
    FROM
      `tvlk-data-user-dev.xs_playground.XS_T1_L3_main_product_booking_session`
    WHERE
      partition_date BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -2 DAY)
      AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) )
--       partition_date >= '2019-08-01' )
      
  SELECT
    DATE( TIMESTAMP_TRUNC( TIMESTAMP_ADD(LSQ.issued_time, INTERVAL 7 HOUR ), DAY ) ) AS transaction_dt,
    LSQ.profile_id,
    LSQ.booking_id,
    LSQ.country_id AS country,
    CASE 
      WHEN LOWER(LSQ.interface) = 'ios' THEN 'mobile-iOS'
      WHEN LOWER(LSQ.interface) = 'android' THEN 'mobile-android'
      ELSE LSQ.interface
    END AS interface,
    BS.session_id,
    LSQ.product_type AS main_product,
    LSQ.product_name AS secondary_product,
    LSQ.total_fare_from_customer_currency_id AS currency,
    CASE
      WHEN LSQ.product_name = 'FlexFare' THEN 'PRE-BOOKING'
    ELSE
    'BOOKING FORM'
  END
    AS channel,
    ROUND(CASE WHEN total_fare_from_customer_currency_id NOT IN ('IDR', 'VND') THEN total_fare_from_customer*1.0/100 ELSE total_fare_from_customer END, 6) AS fare_amount,
    ROUND(CASE WHEN total_fare_from_customer_currency_id NOT IN ('IDR', 'VND') THEN total_fare_from_customer*1.0/100 ELSE total_fare_from_customer END, 6) AS gbv_lcy,
    ROUND(CASE WHEN total_insurance_commission_currency_id NOT IN ('IDR', 'VND') THEN total_insurance_commission*1.0/100 ELSE total_insurance_commission END, 6) AS net_rev_lcy,
    ROUND(CASE
      WHEN conv_1.exchange_rates IS NOT NULL THEN conv_1.exchange_rates
    ELSE
    conv_avg_1.exchange_rates_avg
  END
    * CASE WHEN total_fare_from_customer_currency_id NOT IN ('IDR', 'VND') THEN total_fare_from_customer*1.0/100 ELSE total_fare_from_customer END, 6) AS gbv_usd,
    ROUND(CASE
      WHEN conv_2.exchange_rates IS NOT NULL THEN conv_2.exchange_rates
    ELSE
    conv_avg_2.exchange_rates_avg
  END
    * CASE WHEN total_insurance_commission_currency_id NOT IN ('IDR', 'VND') THEN total_insurance_commission*1.0/100 ELSE total_insurance_commission END, 6) AS net_rev_usd
  FROM
    `tvlk-data-insurance-dev.datamart.fact_insurance_purchase_pg_complete` AS LSQ
  LEFT JOIN
    conversion AS conv_1
  ON
    LSQ.issued_time >= conv_1.approved_timestamp
    AND LSQ.issued_time < conv_1.lead_timestamp
    AND LSQ.total_fare_from_customer_currency_id = conv_1.source_currency
  LEFT JOIN
    conversion_avg_day_7 AS conv_avg_1
  ON
    LSQ.total_fare_from_customer_currency_id = conv_avg_1.source_currency_avg
  LEFT JOIN
    conversion AS conv_2
  ON
    LSQ.issued_time >= conv_2.approved_timestamp
    AND LSQ.issued_time < conv_2.lead_timestamp
    AND LSQ.total_insurance_commission_currency_id = conv_2.source_currency
  LEFT JOIN
    conversion_avg_day_7 AS conv_avg_2
  ON
    LSQ.total_insurance_commission_currency_id = conv_avg_2.source_currency_avg
  LEFT JOIN
    booking_session AS BS
  ON
    LSQ.booking_id = BS.booking_id
  WHERE
    DATE(issued_time) = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) 
--     DATE( issued_time) BETWEEN '2019-08-01' AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) 
    AND profile_id IS NOT NULL
    AND booking_status = 'ISSUED'
    AND booking_type IN ('ADDONS', 'CROSSSELL_ADDONS', 'CROSSSELL_BUNDLE') )

SELECT 
  transaction_dt,
  profile_id,
  booking_id,
  country,
  interface,
  session_id,
  main_product,
  secondary_product,
  channel,
  currency,
  fare_amount,
  gbv_lcy,
  net_rev_lcy,
  gbv_usd,
  net_rev_usd 
FROM 
  base_data