#standardSQL
/* 
Creator : royan.aldian@traveloka.com
Destination table: `tvlk-data-user-dev.xs_playground.XS_T1_L4_main_metrics_with_customer_lifecycle_dimension
Output Field: 
    * event_month,
    * event,
    * period,
    * metric,
    * country,
    * interface,
    * main_product,
    * secondary_product,
    * channel,
    * value
Data Processing:
    * ~1.9GB data processed per month
    * ~n rows inserted per month
    * n sec for elapsed time per query hit (daily)
Est Cost Processing: 
    * Streaming Insert: $0.05/1GB * 1.9GB = $0.095
    * Queries: $0,005/1GB * 1.9GB = $0.0095
*/

-- CREATE OR REPLACE TABLE `tvlk-data-user-dev.xs_playground.XS_T1_L4_main_metrics_with_customer_lifecycle_dimension`
-- PARTITION BY (event_month) AS

WITH customer_lifecycle AS(
  SELECT
    profile_id,
    snapshot_month AS event_month,
    lifecycle
  FROM
    `tvlk-data-mkt-prod.ma_customer_analytics.l4_customer_lifecycle_profile_id_level`
  WHERE
    DATE_TRUNC(DATE(_PARTITIONTIME), MONTH) = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -1 MONTH) ),
--     DATE_TRUNC(DATE(_PARTITIONTIME), MONTH) BETWEEN DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -1 YEAR)
--     AND DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -1 MONTH) ),

business_base AS(
  SELECT
    DATE_TRUNC(transaction_dt, MONTH) AS event_month,
    country,
    interface,
    main_product,
    secondary_product.product_category AS secondary_product,
    secondary_product.business_unit AS business_unit,
    channel,
    section,
    booking_id,
    session_id,
    business.profile_id,
    gbv_usd,
    net_rev_usd,
    lifecycle
  FROM
    `tvlk-data-user-dev.xs_playground.XS_T1_L3_transaction_all_product` business
  LEFT JOIN
    customer_lifecycle
  ON
    business.profile_id = customer_lifecycle.profile_id
    AND DATE_TRUNC(business.transaction_dt, MONTH) = customer_lifecycle.event_month
  WHERE
    DATE_TRUNC(transaction_dt, MONTH) = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -1 MONTH) ),
--     DATE_TRUNC(transaction_dt, MONTH) BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
--     AND DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -1 MONTH) ),

usage_base AS(
  SELECT
    DATE_TRUNC(date, MONTH) AS event_month,
    COALESCE(CAST(activity.profile_id AS STRING),
      cookie_id) AS user_id,
    country,
    interface,
    event_action,
    main_product,
    product.secondary_product AS secondary_product,
    channel,
    session_id,
    section,
    lifecycle
  FROM
    `tvlk-data-user-dev.xs_playground.XS_T1_L3_addon_activity` AS activity
  LEFT JOIN
    UNNEST(product) AS product
  LEFT JOIN
    customer_lifecycle
  ON
    activity.profile_id = customer_lifecycle.profile_id
    AND DATE_TRUNC(activity.date, MONTH) = customer_lifecycle.event_month
  WHERE
    DATE_TRUNC(date, MONTH) = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -1 MONTH) 
--     DATE_TRUNC(date, MONTH) BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
--     AND DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -1 MONTH)
    AND event_action NOT IN ('SEEN') )

SELECT
  event_month,
  'TRANSACTION' AS event,
  'MONTHLY' AS period,
  'BY CUSTOMER LIFECYCLE' AS metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  section,
  lifecycle,
  COUNT(DISTINCT booking_id) AS value
FROM
  business_base
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
  11
  
UNION ALL

SELECT
  event_month,
  'GBV' AS event,
  'MONTHLY' AS period,
  'BY CUSTOMER LIFECYCLE' AS metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  section,
  lifecycle,
  SUM(gbv_usd) AS value
FROM
  business_base
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
  11
  
UNION ALL

SELECT
  event_month,
  'NET REVENUE' AS event,
  'MONTHLY' AS period,
  'BY CUSTOMER LIFECYCLE' AS metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  section,
  lifecycle,
  SUM(net_rev_usd) AS value
FROM
  business_base
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
  11
  
UNION ALL

SELECT
  event_month,
  'ATV' AS event,
  'MONTHLY' AS period,
  'BY CUSTOMER LIFECYCLE' AS metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  section,
  lifecycle,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT profile_id)), 4) AS value
FROM
  business_base
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
  11
  
UNION ALL

SELECT
  event_month,
  'ARPU' AS event,
  'MONTHLY' AS period,
  'BY CUSTOMER LIFECYCLE' AS metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  section,
  lifecycle,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT profile_id)), 4) AS value
FROM
  business_base
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
  11
  
UNION ALL

SELECT
  event_month,
  'XAU' AS event,
  'MONTHLY' AS period,
  'BY CUSTOMER LIFECYCLE' AS metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  section,
  lifecycle,
  COUNT(DISTINCT user_id) AS value
FROM
  usage_base
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
  11

UNION ALL

SELECT
  event_month,
  event,
  period,
  metric,
  country,
  interface,
  main_product,
  secondary_product,
  channel,
  section,
  lifecycle,
  ROUND(AVG(num_xs_prod), 6) AS value
FROM (
  SELECT
    event_month,
    user_id,
    'PRODUCT BREADTH - EXPLORED' AS event,
    'MONTHLY' AS period,
    'BY CUSTOMER LIFECYCLE' AS metric,
    'ALL' AS country,
    'ALL' AS interface,
    'ALL' AS main_product,
    'ALL' AS secondary_product,
    'ALL' AS channel,
    section,
    lifecycle,
    COUNT(DISTINCT secondary_product) AS num_xs_prod
  FROM
    usage_base
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
  11

UNION ALL

SELECT
  event_month,
  event,
  period,
  metric,
  country,
  interface,
  main_product,
  secondary_product,
  channel,
  section,
  lifecycle,
  ROUND(AVG(num_xs_prod), 6) AS value
FROM (
  SELECT
    event_month,
    profile_id,
    'PRODUCT BREADTH - ISSUED' AS event,
    'MONTHLY' AS period,
    'BY CUSTOMER LIFECYCLE' AS metric,
    'ALL' AS country,
    'ALL' AS interface,
    'ALL' AS main_product,
    'ALL' AS secondary_product,
    'ALL' AS channel,
    section,
    lifecycle,
    COUNT(DISTINCT secondary_product) AS num_xs_prod
  FROM
    business_base
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
  11