#standardSQL
/* 
Creator : royan.aldian@traveloka.com
Destination table: tvlk-data-user-dev.xs_playground.XS_T1_L3_addon_take_up_rate
Output Field: 
    * date
    * session_seen  
    * session_transaction 
    * country 
    * interface 
    * channel 
    * event_action  
    * section
    * main_product  
    * secondary_product
Data Processing:
    * ~0.099GB data processed per month
    * ~n rows inserted per month
    * 1.4 sec for elapsed time per query hit (daily)
Est Cost Processing: 
    * Streaming Insert: $0.05/1GB * 0.0998GB = $0.005
    * Queries: $0,005/1GB * 0.0998GB = $0.0005
*/

-- CREATE OR REPLACE TABLE tvlk-data-user-dev.xs_playground.XS_T1_L3_addon_take_up_rate
-- PARTITION BY (date) AS

WITH
  transaction_base AS(
  SELECT
    transaction_dt,
    session_id,
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
  base AS(
  SELECT
    S.date,
    S.session_id AS session_seen,
    T.session_id AS session_transaction,
    S.country,
    S.interface,
    S.channel,
    S.event_action,
    S.section,
    S.main_product,
    product.secondary_product AS secondary_product,
  FROM
    tvlk-data-user-dev.xs_playground.XS_T1_L3_addon_activity AS S
  LEFT JOIN
    UNNEST(product) AS product
  LEFT JOIN
    transaction_base AS T
  ON
    S.date = T.transaction_dt
    AND S.country = T.country
    AND S.interface = T.interface
    AND S.main_product = T.main_product
    AND product.secondary_product = T.secondary_product
    AND S.channel = T.channel
  WHERE
    date = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
--     date BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -6 MONTH)
--     AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) 
    AND S.event_action = 'SEEN'
    AND S.channel = 'BOOKING FORM'
    )

SELECT
  date,
  country,
  interface,
  channel,
  event_action,
  section,
  STRUCT(main_product,
    secondary_product) AS product,
  STRUCT(session_seen,
    session_transaction) AS session
FROM (
  SELECT
    date,
    country,
    interface,
    channel,
    event_action,
    section,
    main_product,
    ARRAY_AGG(DISTINCT secondary_product IGNORE NULLS) AS secondary_product,
    ARRAY_AGG(DISTINCT session_seen IGNORE NULLS) AS session_seen,
    ARRAY_AGG(DISTINCT session_transaction IGNORE NULLS) AS session_transaction
  FROM
    base
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7)