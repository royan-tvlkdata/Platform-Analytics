#standardSQL
/* 
Creator : royan.aldian@traveloka.com
Destination table: `tvlk-data-user-dev.xs_playground.XS_T1_L4_xau
Output Field: 
    * dt,
    * period,
    * metric,
    * country,
    * interface,
    * event_action,
    * main_product,
    * secondary_product,
    * channel,
    * vol_user,
    * vol_session
Data Processing:
    * ~15.6GB data processed per month
    * ~n rows inserted per month
    * 4.6 sec for elapsed time per query hit (daily)
Est Cost Processing: 
    * Streaming Insert: $0.05/1GB * 15.6GB = $0.78
    * Queries: $0,005/1GB * 15.6GB = $0.078
*/

-- CREATE OR REPLACE TABLE `tvlk-data-user-dev.xs_playground.XS_T1_L4_xau`
-- PARTITION BY (dt) AS

DELETE FROM 
  `tvlk-data-user-dev.xs_playground.XS_T1_L4_xau`
WHERE 
  ((EXTRACT(day
      FROM
        CURRENT_DATE()) = 1
      AND DATE_TRUNC(dt, MONTH) = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -1 MONTH))
    OR (EXTRACT(day
      FROM
        CURRENT_DATE()) > 1
      AND DATE_TRUNC(dt, MONTH) = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 0 MONTH)))
  AND period IN ('MONTHLY');

INSERT INTO `tvlk-data-user-dev.xs_playground.XS_T1_L4_xau`


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
  WHERE
    date = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
--     date BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
--     AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
    AND event_action NOT IN ('SEEN') ),

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
  WHERE
    ((EXTRACT(day
        FROM
          CURRENT_DATE()) = 1
        AND DATE_TRUNC(date, MONTH) = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -1 MONTH))
      OR (EXTRACT(day
        FROM
          CURRENT_DATE()) > 1
        AND DATE_TRUNC(date, MONTH) = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 0 MONTH)))
--     date BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
--     AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
    AND event_action NOT IN ('SEEN') )

    
SELECT
  dt,
  'DAILY' AS period,
  'OVERALL' AS metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS event_action,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  section,
  COUNT(DISTINCT user_id) AS vol_user,
  COUNT(DISTINCT session_id) AS vol_session
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
  8,
  9,
  10
  
UNION ALL

SELECT
  dt,
  'DAILY' AS period,
  'BY COUNTRY' AS metric,
  country,
  'ALL' AS interface,
  'ALL' AS event_action,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  section,
  COUNT(DISTINCT user_id) AS vol_user,
  COUNT(DISTINCT session_id) AS vol_session
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
  8,
  9,
  10
  
UNION ALL

SELECT
  dt,
  'DAILY' AS period,
  'BY INTERFACE' AS metric,
  'ALL' AS country,
  interface,
  'ALL' AS event_action,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  section,
  COUNT(DISTINCT user_id) AS vol_user,
  COUNT(DISTINCT session_id) AS vol_session
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
  8,
  9,
  10
  
UNION ALL

SELECT
  dt,
  'DAILY' AS period,
  'BY HOSTING PRODUCT' AS metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS event_action,
  main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  section,
  COUNT(DISTINCT user_id) AS vol_user,
  COUNT(DISTINCT session_id) AS vol_session
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
  8,
  9,
  10
  
UNION ALL

SELECT
  dt,
  'DAILY' AS period,
  'BY CROSS SELL PRODUCT' AS metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS event_action,
  'ALL' AS main_product,
  secondary_product,
  'ALL' AS channel,
  section,
  COUNT(DISTINCT user_id) AS vol_user,
  COUNT(DISTINCT session_id) AS vol_session
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
  8,
  9,
  10
  
UNION ALL

SELECT
  dt,
  'DAILY' AS period,
  'BY INTERACTION TYPE' AS metric,
  'ALL' AS country,
  'ALL' AS interface,
  event_action,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  section,
  COUNT(DISTINCT user_id) AS vol_user,
  COUNT(DISTINCT session_id) AS vol_session
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
  8,
  9,
  10
  
UNION ALL

SELECT
  dt,
  'DAILY' AS period,
  'BY HOSTING PRODUCT & CROSS SELL PRODUCT' AS metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS event_action,
  main_product,
  secondary_product,
  'ALL' AS channel,
  section,
  COUNT(DISTINCT user_id) AS vol_user,
  COUNT(DISTINCT session_id) AS vol_session
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
  8,
  9,
  10
  
UNION ALL

SELECT
  dt,
  'DAILY' AS period,
  'BY CHANNEL, HOSTING PRODUCT & CROSS SELL PRODUCT' AS metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS event_action,
  main_product,
  secondary_product,
  channel,
  section,
  COUNT(DISTINCT user_id) AS vol_user,
  COUNT(DISTINCT session_id) AS vol_session
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
  8,
  9,
  10
  
UNION ALL

SELECT
  dt,
  'DAILY' AS period,
  'BY CHANNEL' AS metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS event_action,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  channel,
  section,
  COUNT(DISTINCT user_id) AS vol_user,
  COUNT(DISTINCT session_id) AS vol_session
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
  8,
  9,
  10
  
UNION ALL

SELECT
  dt,
  'DAILY' AS period,
  'BY COUNTRY & INTERFACE' AS metric,
  country,
  interface,
  'ALL' AS event_action,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  section,
  COUNT(DISTINCT user_id) AS vol_user,
  COUNT(DISTINCT session_id) AS vol_session
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
  8,
  9,
  10
  
UNION ALL

SELECT
  dt,
  'DAILY' AS period,
  'BY ALL DIMENSIONS' AS metric,
  country,
  interface,
  event_action,
  main_product,
  secondary_product,
  channel,
  section,
  COUNT(DISTINCT user_id) AS vol_user,
  COUNT(DISTINCT session_id) AS vol_session
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
  8,
  9,
  10


UNION ALL


SELECT
  DATE_TRUNC(dt, MONTH) AS dt,
  'MONTHLY' AS period,
  'OVERALL' AS metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS event_action,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  section,
  COUNT(DISTINCT user_id) AS vol_user,
  COUNT(DISTINCT session_id) AS vol_session
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
  8,
  9,
  10
  
UNION ALL

SELECT
  DATE_TRUNC(dt, MONTH) AS dt,
  'MONTHLY' AS period,
  'BY COUNTRY' AS metric,
  country,
  'ALL' AS interface,
  'ALL' AS event_action,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  section,
  COUNT(DISTINCT user_id) AS vol_user,
  COUNT(DISTINCT session_id) AS vol_session
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
  8,
  9,
  10
  
UNION ALL

SELECT
  DATE_TRUNC(dt, MONTH) AS dt,
  'MONTHLY' AS period,
  'BY INTERFACE' AS metric,
  'ALL' AS country,
  interface,
  'ALL' AS event_action,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  section,
  COUNT(DISTINCT user_id) AS vol_user,
  COUNT(DISTINCT session_id) AS vol_session
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
  8,
  9,
  10
  
UNION ALL

SELECT
  DATE_TRUNC(dt, MONTH) AS dt,
  'MONTHLY' AS period,
  'BY HOSTING PRODUCT' AS metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS event_action,
  main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  section,
  COUNT(DISTINCT user_id) AS vol_user,
  COUNT(DISTINCT session_id) AS vol_session
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
  8,
  9,
  10
  
UNION ALL

SELECT
  DATE_TRUNC(dt, MONTH) AS dt,
  'MONTHLY' AS period,
  'BY CROSS SELL PRODUCT' AS metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS event_action,
  'ALL' AS main_product,
  secondary_product,
  'ALL' AS channel,
  section,
  COUNT(DISTINCT user_id) AS vol_user,
  COUNT(DISTINCT session_id) AS vol_session
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
  8,
  9,
  10
  
UNION ALL

SELECT
  DATE_TRUNC(dt, MONTH) AS dt,
  'MONTHLY' AS period,
  'BY INTERACTION TYPE' AS metric,
  'ALL' AS country,
  'ALL' AS interface,
  event_action,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  section,
  COUNT(DISTINCT user_id) AS vol_user,
  COUNT(DISTINCT session_id) AS vol_session
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
  8,
  9,
  10
  
UNION ALL

SELECT
  DATE_TRUNC(dt, MONTH) AS dt,
  'MONTHLY' AS period,
  'BY HOSTING PRODUCT & CROSS SELL PRODUCT' AS metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS event_action,
  main_product,
  secondary_product,
  'ALL' AS channel,
  section,
  COUNT(DISTINCT user_id) AS vol_user,
  COUNT(DISTINCT session_id) AS vol_session
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
  8,
  9,
  10
  
UNION ALL

SELECT
  DATE_TRUNC(dt, MONTH) AS dt,
  'MONTHLY' AS period,
  'BY CHANNEL, HOSTING PRODUCT & CROSS SELL PRODUCT' AS metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS event_action,
  main_product,
  secondary_product,
  channel,
  section,
  COUNT(DISTINCT user_id) AS vol_user,
  COUNT(DISTINCT session_id) AS vol_session
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
  8,
  9,
  10
  
UNION ALL

SELECT
  DATE_TRUNC(dt, MONTH) AS dt,
  'MONTHLY' AS period,
  'BY CHANNEL' AS metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS event_action,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  channel,
  section,
  COUNT(DISTINCT user_id) AS vol_user,
  COUNT(DISTINCT session_id) AS vol_session
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
  8,
  9,
  10
  
UNION ALL

SELECT
  DATE_TRUNC(dt, MONTH) AS dt,
  'MONTHLY' AS period,
  'BY COUNTRY & INTERFACE' AS metric,
  country,
  interface,
  'ALL' AS event_action,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  section,
  COUNT(DISTINCT user_id) AS vol_user,
  COUNT(DISTINCT session_id) AS vol_session
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
  8,
  9,
  10
  
UNION ALL

SELECT
  DATE_TRUNC(dt, MONTH) AS dt,
  'MONTHLY' AS period,
  'BY ALL DIMENSIONS' AS metric,
  country,
  interface,
  event_action,
  main_product,
  secondary_product,
  channel,
  section,
  COUNT(DISTINCT user_id) AS vol_user,
  COUNT(DISTINCT session_id) AS vol_session
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
  8,
  9,
  10