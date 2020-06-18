#standardSQL
/* 
Creator : royan.aldian@traveloka.com
Destination table: `tvlk-data-user-dev.xs_playground.XS_T1_L3_transaction_airport_transfer`
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
    * ~0.32GB data processed per month
    * ~2K rows inserted per month
    * 0.5 sec for elapsed time per query hit (daily)
Est Cost Processing: 
    * Streaming Insert: $0.05/1GB * 0.32GB = $0.0016
    * Queries: $0,005/1GB * 0.32GB = $0.00016
*/

-- CREATE OR REPLACE TABLE `tvlk-data-user-dev.xs_playground.XS_T1_L3_transaction_airport_transfer`
-- PARTITION BY (transaction_dt) AS 

WITH
  base_data AS(
  WITH
    conversion AS (
    SELECT
      *
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
      
    main_product_issuance AS (
    SELECT
      DISTINCT booking_id,
      'FLIGHT' AS main_product
    FROM
      `tvlk-realtime.nrtprod.flight_issued`
    WHERE
        _PARTITIONTIME = CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS TIMESTAMP)
--       DATE(_PARTITIONTIME) BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
--       AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
      
    UNION ALL
    
    SELECT
      DISTINCT booking_id,
      'FLIGHT_HOTEL' AS main_product
    FROM
      `tvlk-realtime.nrtprod.trip_product_issued`
    WHERE
        _PARTITIONTIME = CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS TIMESTAMP)
--       DATE(_PARTITIONTIME) BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
--       AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) 
      ),
      
    at_xs_booking AS(
    SELECT
      * EXCEPT(rn)
    FROM (
      SELECT
        DATE(DATETIME_ADD(kafka_publish_timestamp,
            INTERVAL 7 HOUR)) AS date,
        client_timestamp,
        booking_id,
        cookie_id,
        device_id,
        profile_id,
        session_id,
        product_type,
        country,
        intf AS interface,
        ROW_NUMBER() OVER(PARTITION BY booking_id ORDER BY kafka_publish_timestamp) AS rn
      FROM
        `tvlk-realtime.nrtprod.airport_transport_booking_submitted`
      WHERE
          _PARTITIONTIME = CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS TIMESTAMP)
--         DATE(_PARTITIONTIME) BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
--         AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
        AND product_type IN ('CROSSSELL_ADDON',
          'PREBOOKING'))
    WHERE
      rn = 1),
      
    at_issued AS(
    SELECT
      * EXCEPT(rn)
    FROM (
      SELECT
        booking_id,
        currency,
        normal_price_total,
        purchase_price_total,
        gross_sales_price_total,
        ROW_NUMBER() OVER(PARTITION BY booking_id ORDER BY kafka_publish_timestamp DESC) AS rn
      FROM
        `tvlk-realtime.nrtprod.airport_transport_issued`
      WHERE
          _PARTITIONTIME = CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS TIMESTAMP)
--         DATE(_PARTITIONTIME) BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
--         AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) 
        )
    WHERE
      rn = 1),
      
    coupon AS (
    SELECT
      * EXCEPT(rn)
    FROM (
      SELECT
        booking_id,
        voucher.amount AS voucher_amount,
        ROW_NUMBER() OVER(PARTITION BY booking_id ORDER BY invoice_update_timestamp) AS rn
      FROM
        `tvlk-data-mkt-prod.datamart.sales_invoice`
      CROSS JOIN
        UNNEST(voucher) AS voucher
      WHERE
        DATE(_PARTITIONTIME) BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -7 DAY)
        AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
        )
    WHERE
      rn = 1 )
      
  SELECT
    date AS transaction_dt,
    booking_id,
    session_id,
    cookie_id,
    device_id,
    profile_id,
    country,
    interface,
    main_product,
    secondary_product,
    channel,
    currency,
    ROUND(SUM(
      CASE
        WHEN currency NOT IN ('IDR', 'VND') THEN normal_price_total/100
      ELSE
      normal_price_total
    END
      ), 6) AS fare_amount,
    ROUND(SUM(
      CASE
        WHEN currency NOT IN ('IDR', 'VND') THEN normal_price_total/100
      ELSE
      normal_price_total
    END
      ), 6) AS gbv_lcy,
    ROUND(SUM(
      CASE
        WHEN currency NOT IN ('IDR', 'VND') THEN gr_rev/100
      ELSE
      gr_rev
    END
      ), 6) AS net_rev_lcy,
    ROUND(SUM(
      CASE
        WHEN exchange_rates IS NOT NULL THEN exchange_rates
      ELSE
      exchange_rates_avg
    END
      *
      CASE
        WHEN currency NOT IN ('IDR', 'VND') THEN normal_price_total/100
      ELSE
      normal_price_total
    END
      ), 6) AS gbv_usd,
    ROUND(SUM(
      CASE
        WHEN exchange_rates IS NOT NULL THEN exchange_rates
      ELSE
      exchange_rates_avg
    END
      *
      CASE
        WHEN currency NOT IN ('IDR', 'VND') THEN gr_rev/100
      ELSE
      gr_rev
    END
      ), 6) AS net_rev_usd
  FROM (
    SELECT
      *,
      CASE
        WHEN product_type = 'CROSSSELL_ADDON' THEN 'AIRPORT_TRANSFER_CROSSSELL_ADDON'
        WHEN product_type = 'PREBOOKING' THEN 'AIRPORT_TRANSFER_PREBOOKING'
      ELSE
      product_type
    END
      AS secondary_product,
      CASE
        WHEN product_type = 'CROSSSELL_ADDON' THEN 'BOOKING FORM'
        WHEN product_type = 'PREBOOKING' THEN 'PRE-BOOKING'
      ELSE
      product_type
    END
      AS channel,
      gross_sales_price_total-purchase_price_total+coupon_amount AS gr_rev
    FROM (
      SELECT
        *,
        CASE
          WHEN voucher_amount IS NULL THEN 0
        ELSE
        voucher_amount
      END
        AS coupon_amount
      FROM
        at_xs_booking
      INNER JOIN
        at_issued
      USING
        (booking_id)
      LEFT JOIN
        coupon
      USING
        (booking_id)
      LEFT JOIN
        main_product_issuance
      USING
        (booking_id)
      LEFT JOIN
        conversion
      ON
        client_timestamp >= approved_timestamp
        AND client_timestamp < lead_timestamp
        AND currency = source_currency
      LEFT JOIN
        conversion_avg_day_7
      ON
        currency = source_currency_avg ))
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12)


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