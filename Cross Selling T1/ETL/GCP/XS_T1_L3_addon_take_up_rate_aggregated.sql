#standardSQL
/* 
Creator : royan.aldian@traveloka.com
Destination table: tvlk-data-user-dev.xs_playground.XS_T1_L3_addon_take_up_rate_aggregated
Output Field: 
    * date
    * country 
    * interface 
    * channel 
    * event_action  
    * section 
    * main_product  
    * secondary_product
    * business_unit 
    * session_seen
    * session_transaction
    * booking_primary
    * booking_secondary
Data Processing:
    * ~0.099GB data processed per month
    * ~n rows inserted per month
    * 1.4 sec for elapsed time per query hit (daily)
Est Cost Processing: 
    * Streaming Insert: $0.05/1GB * 0.0998GB = $0.005
    * Queries: $0,005/1GB * 0.0998GB = $0.0005
*/

-- CREATE OR REPLACE TABLE tvlk-data-user-dev.xs_playground.XS_T1_L3_addon_take_up_rate_aggregated
-- PARTITION BY (date) AS

WITH
  addon_catalogue AS(
  SELECT
    secondary_product,
    business_unit
  FROM
    `tvlk-data-user-dev.xs_playground.XS_T1_L3_product_category_catalogue`),
    
  transaction_addon AS(
  SELECT
    transaction_dt,
    session_id,
    booking_id,
    country,
    interface,
    main_product,
    secondary_product.product_category AS secondary_product,
    channel
  FROM
    tvlk-data-user-dev.xs_playground.XS_T1_L3_transaction_all_product
  WHERE
    transaction_dt = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) ),
--     transaction_dt BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -6 MONTH)
--     AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) ),

  transaction_main AS(
  SELECT
    booking_id,
    session_id
  FROM
    `tvlk-data-user-dev.xs_playground.XS_T1_L3_main_product_booking_session`
  WHERE
    partition_date = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) ),
--     partition_date BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -6 MONTH)
--     AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) ),
  
  base AS(
  SELECT
    * EXCEPT (business_unit),
    CASE 
      WHEN section = 'ANCILLARY' THEN main_product
      ELSE C.business_unit
  END
    AS business_unit
  FROM(
    SELECT
      S.date,
      S.session_id AS session_seen,
      T.session_id AS session_transaction,
      M.booking_id AS booking_primary,
      T.booking_id AS booking_secondary,
      S.country,
      S.interface,
      S.channel,
      S.event_action,
      S.section,
      S.main_product,
      product.secondary_product AS secondary_product
    FROM
      tvlk-data-user-dev.xs_playground.XS_T1_L3_addon_activity AS S
    LEFT JOIN
      UNNEST(product) AS product
    LEFT JOIN
      transaction_addon AS T
    ON
      S.date = T.transaction_dt
      AND S.country = T.country
      AND S.interface = T.interface
      AND S.main_product = T.main_product
      AND product.secondary_product = T.secondary_product
      AND S.channel = T.channel
    FULL OUTER JOIN
      transaction_main AS M
    ON
      S.session_id = M.session_id
    WHERE
      date = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
--       date BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -6 MONTH)
--       AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) 
      AND S.event_action = 'SEEN'
      AND S.channel = 'BOOKING FORM') 
  LEFT JOIN
    addon_catalogue C
  USING
    (secondary_product) ) 


SELECT
  date,
  country,
  interface,
  channel,
  event_action,
  section,
  main_product,
  secondary_product,
  business_unit,
  COUNT(DISTINCT session_seen) AS session_seen,
  COUNT(DISTINCT session_transaction) AS session_transaction,
  COUNT(DISTINCT booking_primary) AS booking_primary,
  COUNT(DISTINCT booking_secondary) AS booking_secondary
FROM
  base
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9