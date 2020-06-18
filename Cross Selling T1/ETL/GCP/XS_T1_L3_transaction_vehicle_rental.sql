#standardSQL
/* 
Creator : royan.aldian@traveloka.com
Destination table: `tvlk-data-user-dev.xs_playground.XS_T1_L3_transaction_vehicle_rental`
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
    * ~0.74GB data processed per month
    * ~150 rows inserted per month
    * 0.5 sec for elapsed time per query hit (daily)
Est Cost Processing: 
    * Streaming Insert: $0.05/1GB * 0.74GB = $0.0037
    * Queries: $0,005/1GB * 0.74GB = $0.00037
*/

-- CREATE OR REPLACE TABLE `tvlk-data-user-dev.xs_playground.XS_T1_L3_transaction_vehicle_rental`
-- PARTITION BY (transaction_dt) AS 

WITH
  base_data AS(
  WITH
    conversion AS (
    SELECT
      TIMESTAMP_MILLIS(approved_timestamp) AS approved_timestamp,
      TIMESTAMP_MILLIS(lead_timestamp) AS lead_timestamp,
      source_currency,
      exchange_rates,
      conversion_id
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
      
    rental_xs_booking AS(
    SELECT
      * EXCEPT(rn)
    FROM (
      SELECT
        booking_id,
        visit_id,
        CAST(conversion_rate_id AS INT64) AS conversion_rate_id,
        ROW_NUMBER() OVER (PARTITION BY booking_id ORDER BY publish_timestamp DESC, event_id DESC) AS rn
      FROM
        `tvlk-realtime.nrtprod.rental_backend`
      WHERE
        _PARTITIONTIME = CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS TIMESTAMP)
--         DATE(_PARTITIONTIME) BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
--         AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
        AND event_tracking_name IN ('BOOKING') )
    WHERE
      rn = 1 ),
      
    rental_issued AS(
    SELECT
      * EXCEPT(rn)
    FROM (
      SELECT
        DATE(TIMESTAMP_ADD(publish_timestamp, INTERVAL 7 HOUR)) AS date,
        client_timestamp,
        booking_id,
        cookie_id,
        device_id,
        profile_id,
        session_id,
        country,
        CASE
          WHEN LOWER(intf) = 'mobile-android' THEN 'mobile-android'
          WHEN LOWER(intf) = 'mobile-ios' THEN 'mobile-iOS'
          WHEN LOWER(intf) = 'desktop' THEN 'desktop'
          WHEN LOWER(intf) = 'mobile-web'
        OR LOWER(intf) = 'mobile' THEN 'mobile'
        ELSE
        'Other'
      END
        AS interface,
        currency,
        COALESCE(coupon_value,
          0) coupon_value,
        ROW_NUMBER() OVER (PARTITION BY booking_id ORDER BY publish_timestamp DESC, event_id DESC) AS rn
      FROM
        `tvlk-realtime.nrtprod.rental_backend`
      WHERE
        _PARTITIONTIME = CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS TIMESTAMP)
--         DATE(_PARTITIONTIME) BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
--         AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
        AND event_tracking_name IN ('ISSUED') )
    WHERE
      rn = 1 ),
      
    ingestion_booking_addon_rate AS (
    SELECT
      booking_id,
      conversion_table_id,
      SUM(total_addon_published_rate) total_addon_published_rate,
      SUM(total_addon_selling_rate) total_addon_selling_rate
    FROM (
      SELECT
        DISTINCT ba.addon_booking_id,
        booking_id,
        conversion_table_id,
        CASE
          WHEN rate_detail.rate_type = 'TOTAL_PUBLISHED_RATE' THEN CASE
          WHEN currency NOT IN ('IDR',
          'VND') THEN rate_value*1.0/100
        ELSE
        rate_value
      END
        ELSE
        0
      END
        total_addon_published_rate,
        CASE
          WHEN rate_detail.rate_type = 'TOTAL_SELLING_RATE' THEN CASE
          WHEN currency NOT IN ('IDR',
          'VND') THEN rate_value*1.0/100
        ELSE
        rate_value
      END
        ELSE
        0
      END
        total_addon_selling_rate
      FROM
        `tvlk-realtime.pg.ppr_booking_ppr_booking_rental_booking_addon` ba
      LEFT JOIN (
        SELECT
          addon_booking_id,
          rate_value,
          currency,
          rate_type,
          MAX(conversion_table_id) conversion_table_id
        FROM
          `tvlk-realtime.pg.ppr_booking_ppr_booking_rental_addon_booking_rate`
        WHERE
          _PARTITIONTIME = CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS TIMESTAMP)
--           DATE(_PARTITIONTIME) BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
--           AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
          AND rate_type IN ('TOTAL_PUBLISHED_RATE',
            'TOTAL_PURCHASE_RATE',
            'TOTAL_SELLING_RATE')
        GROUP BY
          1,
          2,
          3,
          4 ) rate_detail
      ON
        rate_detail.addon_booking_id = ba.addon_booking_id )
    GROUP BY
      1,
      2 ),
      
    ingestion_booking_data AS (
    SELECT
      booking_id,
      trip_itinerary_id AS traveloka_booking_id,
    FROM
      `tvlk-realtime.pg.ppr_booking_ppr_booking_rental_booking`
    WHERE
      _PARTITIONTIME = CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS TIMESTAMP)
--       DATE(_PARTITIONTIME) BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
--       AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
      AND usage_charging_type IS NOT NULL ),
      
    ingestion_booking_rate AS (
    SELECT
      booking_id,
      rate_value AS fare_amount,
      CASE
        WHEN currency NOT IN ('IDR', 'VND') THEN rate_value*1.0/100
      ELSE
      rate_value
    END
      rate_value,
      rate_type,
      --       currency,
      MAX(conversion_table_id) conversion_table_id
    FROM
      `tvlk-realtime.pg.ppr_booking_ppr_booking_rental_booking_rate`
    WHERE
      _PARTITIONTIME = CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS TIMESTAMP)
--       DATE(_PARTITIONTIME) BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
--       AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
      AND rate_type IN ('TOTAL_PUBLISHED_RATE',
        'TOTAL_PURCHASE_RATE',
        'TOTAL_SELLING_RATE')
    GROUP BY
      1,
      2,
      3,
      4 ),
      
    entry_point AS (
    SELECT
      (CASE
          WHEN SPLIT(REPLACE(event_trigger,'-','.'),'.')[ OFFSET (1)] = 'PREBOOKING' THEN 'VEHICLE_RENTAL_PREBOOKING'
        ELSE
        event_trigger
      END
        ) product_type,
      COALESCE(SPLIT(event_trigger,'.')[SAFE_OFFSET(2)],
        'UNKNOWN') AS main_product,
      TIMESTAMP(DATETIME_ADD(kafka_publish_timestamp,
          INTERVAL 7 HOUR)) dt_tm,
      visit_id
    FROM
      `tvlk-realtime-masked.nrtprod.rental_frontend`
    WHERE
      PARTITIONTIME = CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS TIMESTAMP)
--       DATE(PARTITIONTIME) BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
--       AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
      AND event_trigger LIKE 'CROSS%'
      AND SPLIT(REPLACE(event_trigger,'-','.'),'.')[
    OFFSET
      (1)] = 'PREBOOKING' )
      
  SELECT
    transaction_dt,
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
    exchange_rates,
    exchange_rates_avg,
    ROUND(SUM(total_fare_before_pricing_rule), 2) AS fare_amount,
    ROUND(SUM(total_fare_before_pricing_rule ), 2) AS gbv_lcy,
    ROUND(SUM(gr_rev ), 2) AS net_rev_lcy,
    ROUND(SUM(
        CASE
          WHEN exchange_rates IS NOT NULL THEN exchange_rates
        ELSE
        exchange_rates_avg
      END
        * total_fare_before_pricing_rule ), 2) AS gbv_usd,
    ROUND(SUM(
        CASE
          WHEN exchange_rates IS NOT NULL THEN exchange_rates
        ELSE
        exchange_rates_avg
      END
        * gr_rev ), 2) AS net_rev_usd
  FROM (
    SELECT
      *,
      product_type AS secondary_product,
      CASE
        WHEN product_type LIKE '%PREBOOKING%' THEN 'PRE-BOOKING'
      ELSE
      product_type
    END
      AS channel,
      (0.9*(`total_publish_price_of_base_product`*0.2+`total_publish_price_of_add_on`*0.1)+(`total_coupon_value`+(`total_selling_price_of_base_product`-`total_publish_price_of_base_product`))) AS gr_rev
    FROM (
      SELECT
        date AS transaction_dt,
        rental_issued.booking_id,
        session_id,
        cookie_id,
        device_id,
        profile_id,
        country,
        interface,
        main_product,
        product_type,
        currency,
        exchange_rates,
        exchange_rates_avg,
        published_fare.fare_amount,
        published_fare.rate_value AS total_publish_price_of_base_product,
        coalesce(ingestion_booking_addon_rate.total_addon_published_rate,
          0) AS total_publish_price_of_add_on,
        coupon_value AS total_coupon_value,
        selling_fare.rate_value AS total_selling_price_of_base_product,
        published_fare.rate_value + coalesce(ingestion_booking_addon_rate.total_addon_published_rate,
          0) AS total_fare_before_pricing_rule
      FROM
        rental_xs_booking
      INNER JOIN
        rental_issued
      USING
        (booking_id)
      LEFT JOIN
        ingestion_booking_data
      ON
        rental_issued.booking_id = ingestion_booking_data.traveloka_booking_id
      LEFT JOIN
        ingestion_booking_addon_rate
      ON
        ingestion_booking_addon_rate.booking_id = ingestion_booking_data.booking_id
      LEFT JOIN
        ingestion_booking_rate published_fare
      ON
        published_fare.booking_id = ingestion_booking_data.booking_id
        AND published_fare.rate_type = 'TOTAL_PUBLISHED_RATE'
      LEFT JOIN
        ingestion_booking_rate selling_fare
      ON
        selling_fare.booking_id = ingestion_booking_data.booking_id
        AND selling_fare.rate_type = 'TOTAL_SELLING_RATE'
      INNER JOIN
        entry_point
      USING
        (visit_id)
      LEFT JOIN
        conversion
      ON
        --         client_timestamp >= approved_timestamp
        --         AND client_timestamp < lead_timestamp
        conversion_rate_id = conversion_id
        AND currency = source_currency
      LEFT JOIN
        conversion_avg_day_7
      ON
        currency = source_currency_avg))
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
    12,
    13,
    14)


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