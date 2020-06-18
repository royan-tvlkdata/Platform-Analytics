#standardSQL
/* 
Creator : royan.aldian@traveloka.com
Destination table: `tvlk-data-user-dev.xs_playground.XS_T1_L4_new_user
Output Field: 
    * dt,
    * period,
    * metric,
    * secondary_product,
    * main_product,
    * interface,
    * country,
    * channel,
    * event_action,
    * vol_new_user
Data Processing:
    * ~10.41GB data processed per month
    * ~n rows inserted per month
    * 11.1 sec for elapsed time per query hit (daily)
Est Cost Processing: 
    * Streaming Insert: $0.05/1GB * 10.41GB = $0.52
    * Queries: $0,005/1GB * 10.41GB = $0.052
*/

-- CREATE OR REPLACE TABLE `tvlk-data-user-dev.xs_playground.XS_T1_L4_new_user`
-- PARTITION BY (dt) AS

-- DELETE FROM 
--   `tvlk-data-user-dev.xs_playground.XS_T1_L4_new_user`
-- WHERE 
--   ((EXTRACT(day
--       FROM
--         CURRENT_DATE()) = 1
--       AND DATE_TRUNC(dt, MONTH) = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -1 MONTH))
--     OR (EXTRACT(day
--       FROM
--         CURRENT_DATE()) > 1
--       AND DATE_TRUNC(dt, MONTH) = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 0 MONTH)))
--   AND period IN ('MONTHLY');

-- INSERT INTO `tvlk-data-user-dev.xs_playground.XS_T1_L4_new_user`

WITH
  base_data_daily AS(
  SELECT
    date AS dt,
    COALESCE(CAST(profile_id AS STRING),
      cookie_id) AS user_id,
    country,
    interface,
    event_action,
    main_product,
    product.secondary_product AS secondary_product,
    channel,
    session_id,
    section
  FROM
    `tvlk-data-user-dev.xs_playground.XS_T1_L3_addon_activity`
  LEFT JOIN
    UNNEST(product) AS product
--   WHERE
--     date = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) ),
--     date BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
--     AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) 
    ),

  base_data_monthly AS(
  SELECT
    date AS dt,
    COALESCE(CAST(profile_id AS STRING),
      cookie_id) AS user_id,
    country,
    interface,
    event_action,
    main_product,
    product.secondary_product AS secondary_product,
    channel,
    session_id,
    section
  FROM
    `tvlk-data-user-dev.xs_playground.XS_T1_L3_addon_activity`
  LEFT JOIN
    UNNEST(product) AS product
--   WHERE
--     ((EXTRACT(day
--         FROM
--           CURRENT_DATE()) = 1
--         AND DATE_TRUNC(date, MONTH) = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -1 MONTH))
--       OR (EXTRACT(day
--         FROM
--           CURRENT_DATE()) > 1
--         AND DATE_TRUNC(date, MONTH) = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 0 MONTH)))
--     date BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
--     AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) 
    )

    
SELECT
  dt,
  'MONTHLY' AS period,
  'OVERALL' AS metric,
  secondary_product,
  main_product,
  interface,
  country,
  channel,
  event_action,
  section,
  COUNT(DISTINCT user_id) AS vol_new_user
FROM (
  SELECT
    user_id,
    'ALL' AS secondary_product,
    'ALL' AS main_product,
    'ALL' AS interface,
    'ALL' AS country,
    'ALL' AS channel,
    'ALL' AS event_action,
    section,
    MIN(DATE_TRUNC(dt, MONTH)) AS dt
  FROM
    base_data_monthly
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8)
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
  10
  
UNION ALL

SELECT
  dt,
  'MONTHLY' AS period,
  'BY COUNTRY' AS metric,
  secondary_product,
  main_product,
  interface,
  country,
  channel,
  event_action,
  section,
  COUNT(DISTINCT user_id) AS vol_new_user
FROM (
  SELECT
    user_id,
    'ALL' AS secondary_product,
    'ALL' AS main_product,
    'ALL' AS interface,
    country,
    'ALL' AS channel,
    'ALL' AS event_action,
    section,
    MIN(DATE_TRUNC(dt, MONTH)) AS dt
  FROM
    base_data_monthly
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8)
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
  10
  
UNION ALL

SELECT
  dt,
  'MONTHLY' AS period,
  'BY INTERFACE' AS metric,
  secondary_product,
  main_product,
  interface,
  country,
  channel,
  event_action,
  section,
  COUNT(DISTINCT user_id) AS vol_new_user
FROM (
  SELECT
    user_id,
    'ALL' AS secondary_product,
    'ALL' AS main_product,
    interface,
    'ALL' AS country,
    'ALL' AS channel,
    'ALL' AS event_action,
    section,
    MIN(DATE_TRUNC(dt, MONTH)) AS dt
  FROM
    base_data_monthly
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8)
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
  10
  
UNION ALL

SELECT
  dt,
  'MONTHLY' AS period,
  'BY HOSTING PRODUCT' AS metric,
  secondary_product,
  main_product,
  interface,
  country,
  channel,
  event_action,
  section,
  COUNT(DISTINCT user_id) AS vol_new_user
FROM (
  SELECT
    user_id,
    'ALL' AS secondary_product,
    main_product,
    'ALL' AS interface,
    'ALL' AS country,
    'ALL' AS channel,
    'ALL' AS event_action,
    section,
    MIN(DATE_TRUNC(dt, MONTH)) AS dt
  FROM
    base_data_monthly
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8)
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
  10
  
UNION ALL

SELECT
  dt,
  'MONTHLY' AS period,
  'BY CROSS SELL PRODUCT' AS metric,
  secondary_product,
  main_product,
  interface,
  country,
  channel,
  event_action,
  section,
  COUNT(DISTINCT user_id) AS vol_new_user
FROM (
  SELECT
    user_id,
    secondary_product,
    'ALL' AS main_product,
    'ALL' AS interface,
    'ALL' AS country,
    'ALL' AS channel,
    'ALL' AS event_action,
    section,
    MIN(DATE_TRUNC(dt, MONTH)) AS dt
  FROM
    base_data_monthly
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8)
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
  10
  
UNION ALL

SELECT
  dt,
  'MONTHLY' AS period,
  'BY INTERACTION TYPE' AS metric,
  secondary_product,
  main_product,
  interface,
  country,
  channel,
  event_action,
  section,
  COUNT(DISTINCT user_id) AS vol_new_user
FROM (
  SELECT
    user_id,
    'ALL' AS secondary_product,
    'ALL' AS main_product,
    'ALL' AS interface,
    'ALL' AS country,
    'ALL' AS channel,
    event_action,
    section,
    MIN(DATE_TRUNC(dt, MONTH)) AS dt
  FROM
    base_data_monthly
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8)
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
  10
  
UNION ALL

SELECT
  dt,
  'MONTHLY' AS period,
  'BY HOSTING & CROSS SELL PRODUCT' AS metric,
  secondary_product,
  main_product,
  interface,
  country,
  channel,
  event_action,
  section,
  COUNT(DISTINCT user_id) AS vol_new_user
FROM (
  SELECT
    user_id,
    secondary_product,
    main_product,
    'ALL' AS interface,
    'ALL' AS country,
    'ALL' AS channel,
    'ALL' AS event_action,
    section,
    MIN(DATE_TRUNC(dt, MONTH)) AS dt
  FROM
    base_data_monthly
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8)
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
  10
  
UNION ALL

SELECT
  dt,
  'MONTHLY' AS period,
  'BY CHANNEL, HOSTING PRODUCT & CROSS SELL PRODUCT' AS metric,
  secondary_product,
  main_product,
  interface,
  country,
  channel,
  event_action,
  section,
  COUNT(DISTINCT user_id) AS vol_new_user
FROM (
  SELECT
    user_id,
    secondary_product,
    main_product,
    'ALL' AS interface,
    'ALL' AS country,
    channel,
    'ALL' AS event_action,
    section,
    MIN(DATE_TRUNC(dt, MONTH)) AS dt
  FROM
    base_data_monthly
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8)
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
  10
  
UNION ALL

SELECT
  dt,
  'MONTHLY' AS period,
  'BY CHANNEL' AS metric,
  secondary_product,
  main_product,
  interface,
  country,
  channel,
  event_action,
  section,
  COUNT(DISTINCT user_id) AS vol_new_user
FROM (
  SELECT
    user_id,
    'ALL' AS secondary_product,
    'ALL' AS main_product,
    'ALL' AS interface,
    'ALL' AS country,
    channel,
    'ALL' AS event_action,
    section,
    MIN(DATE_TRUNC(dt, MONTH)) AS dt
  FROM
    base_data_monthly
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8)
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
  10
  
UNION ALL

SELECT
  dt,
  'MONTHLY' AS period,
  'BY COUNTRY & INTERFACE' AS metric,
  secondary_product,
  main_product,
  interface,
  country,
  channel,
  event_action,
  section,
  COUNT(DISTINCT user_id) AS vol_new_user
FROM (
  SELECT
    user_id,
    'ALL' AS secondary_product,
    'ALL' AS main_product,
    interface,
    country,
    'ALL' AS channel,
    'ALL' AS event_action,
    section,
    MIN(DATE_TRUNC(dt, MONTH)) AS dt
  FROM
    base_data_monthly
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8)
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
  10
  
UNION ALL

SELECT
  dt,
  'MONTHLY' AS period,
  'BY ALL DIMENSIONS' AS metric,
  secondary_product,
  main_product,
  interface,
  country,
  channel,
  event_action,
  section,
  COUNT(DISTINCT user_id) AS vol_new_user
FROM (
  SELECT
    user_id,
    secondary_product,
    main_product,
    interface,
    country,
    channel,
    event_action,
    section,
    MIN(DATE_TRUNC(dt, MONTH)) AS dt
  FROM
    base_data_monthly
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8)
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
  10


UNION ALL


SELECT
  dt,
  'DAILY' AS period,
  'OVERALL' AS metric,
  secondary_product,
  main_product,
  interface,
  country,
  channel,
  event_action,
  section,
  COUNT(DISTINCT user_id) AS vol_new_user
FROM (
  SELECT
    user_id,
    'ALL' AS secondary_product,
    'ALL' AS main_product,
    'ALL' AS interface,
    'ALL' AS country,
    'ALL' AS channel,
    'ALL' AS event_action,
    section,
    MIN(dt) AS dt
  FROM
    base_data_daily
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8)
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
  10
  
UNION ALL

SELECT
  dt,
  'DAILY' AS period,
  'BY COUNTRY' AS metric,
  secondary_product,
  main_product,
  interface,
  country,
  channel,
  event_action,
  section,
  COUNT(DISTINCT user_id) AS vol_new_user
FROM (
  SELECT
    user_id,
    'ALL' AS secondary_product,
    'ALL' AS main_product,
    'ALL' AS interface,
    country,
    'ALL' AS channel,
    'ALL' AS event_action,
    section,
    MIN(dt) AS dt
  FROM
    base_data_daily
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8)
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
  10
  
UNION ALL

SELECT
  dt,
  'DAILY' AS period,
  'BY INTERFACE' AS metric,
  secondary_product,
  main_product,
  interface,
  country,
  channel,
  event_action,
  section,
  COUNT(DISTINCT user_id) AS vol_new_user
FROM (
  SELECT
    user_id,
    'ALL' AS secondary_product,
    'ALL' AS main_product,
    interface,
    'ALL' AS country,
    'ALL' AS channel,
    'ALL' AS event_action,
    section,
    MIN(dt) AS dt
  FROM
    base_data_daily
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8)
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
  10
  
UNION ALL

SELECT
  dt,
  'DAILY' AS period,
  'BY HOSTING PRODUCT' AS metric,
  secondary_product,
  main_product,
  interface,
  country,
  channel,
  event_action,
  section,
  COUNT(DISTINCT user_id) AS vol_new_user
FROM (
  SELECT
    user_id,
    'ALL' AS secondary_product,
    main_product,
    'ALL' AS interface,
    'ALL' AS country,
    'ALL' AS channel,
    'ALL' AS event_action,
    section,
    MIN(dt) AS dt
  FROM
    base_data_daily
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8)
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
  10
  
UNION ALL

SELECT
  dt,
  'DAILY' AS period,
  'BY CROSS SELL PRODUCT' AS metric,
  secondary_product,
  main_product,
  interface,
  country,
  channel,
  event_action,
  section,
  COUNT(DISTINCT user_id) AS vol_new_user
FROM (
  SELECT
    user_id,
    secondary_product,
    'ALL' AS main_product,
    'ALL' AS interface,
    'ALL' AS country,
    'ALL' AS channel,
    'ALL' AS event_action,
    section,
    MIN(dt) AS dt
  FROM
    base_data_daily
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8)
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
  10
  
UNION ALL

SELECT
  dt,
  'DAILY' AS period,
  'BY INTERACTION TYPE' AS metric,
  secondary_product,
  main_product,
  interface,
  country,
  channel,
  event_action,
  section,
  COUNT(DISTINCT user_id) AS vol_new_user
FROM (
  SELECT
    user_id,
    'ALL' AS secondary_product,
    'ALL' AS main_product,
    'ALL' AS interface,
    'ALL' AS country,
    'ALL' AS channel,
    event_action,
    section,
    MIN(dt) AS dt
  FROM
    base_data_daily
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8)
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
  10
  
UNION ALL

SELECT
  dt,
  'DAILY' AS period,
  'BY HOSTING & CROSS SELL PRODUCT' AS metric,
  secondary_product,
  main_product,
  interface,
  country,
  channel,
  event_action,
  section,
  COUNT(DISTINCT user_id) AS vol_new_user
FROM (
  SELECT
    user_id,
    secondary_product,
    main_product,
    'ALL' AS interface,
    'ALL' AS country,
    'ALL' AS channel,
    'ALL' AS event_action,
    section,
    MIN(dt) AS dt
  FROM
    base_data_daily
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8)
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
  10
  
UNION ALL

SELECT
  dt,
  'DAILY' AS period,
  'BY CHANNEL, HOSTING PRODUCT & CROSS SELL PRODUCT' AS metric,
  secondary_product,
  main_product,
  interface,
  country,
  channel,
  event_action,
  section,
  COUNT(DISTINCT user_id) AS vol_new_user
FROM (
  SELECT
    user_id,
    secondary_product,
    main_product,
    'ALL' AS interface,
    'ALL' AS country,
    channel,
    'ALL' AS event_action,
    section,
    MIN(dt) AS dt
  FROM
    base_data_daily
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8)
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
  10
  
UNION ALL

SELECT
  dt,
  'DAILY' AS period,
  'BY CHANNEL' AS metric,
  secondary_product,
  main_product,
  interface,
  country,
  channel,
  event_action,
  section,
  COUNT(DISTINCT user_id) AS vol_new_user
FROM (
  SELECT
    user_id,
    'ALL' AS secondary_product,
    'ALL' AS main_product,
    'ALL' AS interface,
    'ALL' AS country,
    channel,
    'ALL' AS event_action,
    section,
    MIN(dt) AS dt
  FROM
    base_data_daily
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8)
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
  10
  
UNION ALL

SELECT
  dt,
  'DAILY' AS period,
  'BY COUNTRY & INTERFACE' AS metric,
  secondary_product,
  main_product,
  interface,
  country,
  channel,
  event_action,
  section,
  COUNT(DISTINCT user_id) AS vol_new_user
FROM (
  SELECT
    user_id,
    'ALL' AS secondary_product,
    'ALL' AS main_product,
    interface,
    country,
    'ALL' AS channel,
    'ALL' AS event_action,
    section,
    MIN(dt) AS dt
  FROM
    base_data_daily
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8)
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
  10
  
UNION ALL

SELECT
  dt,
  'DAILY' AS period,
  'BY ALL DIMENSIONS' AS metric,
  secondary_product,
  main_product,
  interface,
  country,
  channel,
  event_action,
  section,
  COUNT(DISTINCT user_id) AS vol_new_user
FROM (
  SELECT
    user_id,
    secondary_product,
    main_product,
    interface,
    country,
    channel,
    event_action,
    section,
    MIN(dt) AS dt
  FROM
    base_data_daily
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8)
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
  10