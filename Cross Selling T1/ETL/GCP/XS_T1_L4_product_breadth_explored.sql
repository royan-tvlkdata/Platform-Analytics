#standardSQL
/* 
Creator : royan.aldian@traveloka.com
Destination table: `tvlk-data-user-dev.xs_playground.XS_T1_L4_product_breadth_explored
Output Field: 
    * dt,
    * period,
    * metric,
    * country,
    * interface,
    * main_product,
    * channel,
    * avg_xs_prod_search
Data Processing:
    * ~9.42GB data processed per month
    * ~n rows inserted per month
    * 6.9 sec for elapsed time per query hit (daily)
Est Cost Processing: 
    * Streaming Insert: $0.05/1GB * 9.42GB = $0.47
    * Queries: $0,005/1GB * 9.42GB = $0.047
*/

-- CREATE OR REPLACE TABLE `tvlk-data-user-dev.xs_playground.XS_T1_L4_product_breadth_explored`
-- PARTITION BY (dt) AS

DELETE FROM 
  `tvlk-data-user-dev.xs_playground.XS_T1_L4_product_breadth_explored`
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

INSERT INTO `tvlk-data-user-dev.xs_playground.XS_T1_L4_product_breadth_explored`

WITH
  base_data_daily AS(
  SELECT
    date AS dt,
    COALESCE(CAST(profile_id AS STRING),
      cookie_id) AS user_id,
    country,
    interface,
    main_product,
    product.secondary_product AS secondary_product,
    channel,
    section
  FROM
    `tvlk-data-user-dev.xs_playground.XS_T1_L3_addon_activity`
  LEFT JOIN
    UNNEST(product) AS product
  WHERE
    date = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
--     date BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -6 MONTH)
--     AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
    AND event_action NOT IN ('SEEN') ),

  base_data_monthly AS(
  SELECT
    date AS dt,
    COALESCE(CAST(profile_id AS STRING),
      cookie_id) AS user_id,
    country,
    interface,
    main_product,
    product.secondary_product AS secondary_product,
    channel,
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
--     date BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -6 MONTH)
--     AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
    AND event_action NOT IN ('SEEN') )

    
SELECT
  dt,
  period,
  metric,
  country,
  interface,
  main_product,
  channel,
  section,
  AVG(num_xs_prod) AS avg_xs_prod_search
FROM (
  SELECT
    DATE_TRUNC(dt, MONTH) AS dt,
    user_id,
    'MONTHLY' AS period,
    'OVERALL' AS metric,
    'ALL' AS country,
    'ALL' AS interface,
    'ALL' AS main_product,
    'ALL' AS channel,
    section,
    COUNT(DISTINCT secondary_product) AS num_xs_prod
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
    9)
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8
  
UNION ALL

SELECT
  dt,
  period,
  metric,
  country,
  interface,
  main_product,
  channel,
  section,
  AVG(num_xs_prod) AS avg_xs_prod_search
FROM (
  SELECT
    DATE_TRUNC(dt, MONTH) AS dt,
    user_id,
    'MONTHLY' AS period, 
    'BY COUNTRY' AS metric,
    country,
    'ALL' AS interface,
    'ALL' AS main_product,
    'ALL' AS channel,
    section,
    COUNT(DISTINCT secondary_product) AS num_xs_prod
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
    9)
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8
 
UNION ALL

SELECT
  dt,
  period,
  metric,
  country,
  interface,
  main_product,
  channel,
  section,
  AVG(num_xs_prod) AS avg_xs_prod_search
FROM (
  SELECT
    DATE_TRUNC(dt, MONTH) AS dt,
    user_id,
    'MONTHLY' AS period, 
    'BY INTERFACE' AS metric,
    'ALL' AS country,
    interface,
    'ALL' AS main_product,
    'ALL' AS channel,
    section,
    COUNT(DISTINCT secondary_product) AS num_xs_prod
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
    9)
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8
    
UNION ALL

SELECT
  dt,
  period,
  metric,
  country,
  interface,
  main_product,
  channel,
  section,
  AVG(num_xs_prod) AS avg_xs_prod_search
FROM (
  SELECT
    DATE_TRUNC(dt, MONTH) AS dt,
    user_id,
    'MONTHLY' AS period,
    'BY HOSTING PRODUCT' AS metric,
    'ALL' AS country,
    'ALL' AS interface,
    main_product,
    'ALL' AS channel,
    section,
    COUNT(DISTINCT secondary_product) AS num_xs_prod
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
    9)
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8
    
UNION ALL

SELECT
  dt,
  period,
  metric,
  country,
  interface,
  main_product,
  channel,
  section,
  AVG(num_xs_prod) AS avg_xs_prod_search
FROM (
  SELECT
    DATE_TRUNC(dt, MONTH) AS dt,
    user_id,
    'MONTHLY' AS period,
    'BY CHANNEL' AS metric,
    'ALL' AS country,
    'ALL' AS interface,
    'ALL' AS main_product,
    channel,
    section,
    COUNT(DISTINCT secondary_product) AS num_xs_prod
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
    9)
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8
    
UNION ALL

SELECT
  dt,
  period,
  metric,
  country,
  interface,
  main_product,
  channel,
  section,
  AVG(num_xs_prod) AS avg_xs_prod_search
FROM (
  SELECT
    DATE_TRUNC(dt, MONTH) AS dt,
    user_id,
    'MONTHLY' AS period,
    'BY COUNTRY & INTERFACE' AS metric,
    country,
    interface,
    'ALL' AS main_product,
    'ALL' AS channel,
    section,
    COUNT(DISTINCT secondary_product) AS num_xs_prod
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
    9)
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8
    
UNION ALL

SELECT
  dt,
  period,
  metric,
  country,
  interface,
  main_product,
  channel,
  section,
  AVG(num_xs_prod) AS avg_xs_prod_search
FROM (
  SELECT
    DATE_TRUNC(dt, MONTH) AS dt,
    user_id,
    'MONTHLY' AS period,
    'BY ALL DIMENSIONS' AS metric,
    country,
    interface,
    main_product,
    channel,
    section,
    COUNT(DISTINCT secondary_product) AS num_xs_prod
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
    9)
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8


UNION ALL


SELECT
  dt,
  period,
  metric,
  country,
  interface,
  main_product,
  channel,
  section,
  AVG(num_xs_prod) AS avg_xs_prod_search
FROM (
  SELECT
    dt,
    user_id,
    'DAILY' AS period,
    'OVERALL' AS metric,
    'ALL' AS country,
    'ALL' AS interface,
    'ALL' AS main_product,
    'ALL' AS channel,
    section,
    COUNT(DISTINCT secondary_product) AS num_xs_prod
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
    9)
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8
  
UNION ALL

SELECT
  dt,
  period,
  metric,
  country,
  interface,
  main_product,
  channel,
  section,
  AVG(num_xs_prod) AS avg_xs_prod_search
FROM (
  SELECT
    dt,
    user_id,
    'DAILY' AS period,
    'BY COUNTRY' AS metric,
    country,
    'ALL' AS interface,
    'ALL' AS main_product,
    'ALL' AS channel,
    section,
    COUNT(DISTINCT secondary_product) AS num_xs_prod
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
    9)
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8
 
UNION ALL

SELECT
  dt,
  period,
  metric,
  country,
  interface,
  main_product,
  channel,
  section,
  AVG(num_xs_prod) AS avg_xs_prod_search
FROM (
  SELECT
    dt,
    user_id,
    'DAILY' AS period,
    'BY INTERFACE' AS metric,
    'ALL' AS country,
    interface,
    'ALL' AS main_product,
    'ALL' AS channel,
    section,
    COUNT(DISTINCT secondary_product) AS num_xs_prod
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
    9)
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8
    
UNION ALL

SELECT
  dt,
  period,
  metric,
  country,
  interface,
  main_product,
  channel,
  section,
  AVG(num_xs_prod) AS avg_xs_prod_search
FROM (
  SELECT
    dt,
    user_id,
    'DAILY' AS period,
    'BY HOSTING PRODUCT' AS metric,
    'ALL' AS country,
    'ALL' AS interface,
    main_product,
    'ALL' AS channel,
    section,
    COUNT(DISTINCT secondary_product) AS num_xs_prod
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
    9)
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8
    
UNION ALL

SELECT
  dt,
  period,
  metric,
  country,
  interface,
  main_product,
  channel,
  section,
  AVG(num_xs_prod) AS avg_xs_prod_search
FROM (
  SELECT
    dt,
    user_id,
    'DAILY' AS period,
    'BY CHANNEL' AS metric,
    'ALL' AS country,
    'ALL' AS interface,
    'ALL' AS main_product,
    channel,
    section,
    COUNT(DISTINCT secondary_product) AS num_xs_prod
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
    9)
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8
    
UNION ALL

SELECT
  dt,
  period,
  metric,
  country,
  interface,
  main_product,
  channel,
  section,
  AVG(num_xs_prod) AS avg_xs_prod_search
FROM (
  SELECT
    dt,
    user_id,
    'DAILY' AS period,
    'BY COUNTRY & INTERFACE' AS metric,
    country,
    interface,
    'ALL' AS main_product,
    'ALL' AS channel,
    section,
    COUNT(DISTINCT secondary_product) AS num_xs_prod
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
    9)
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8
    
UNION ALL

SELECT
  dt,
  period,
  metric,
  country,
  interface,
  main_product,
  channel,
  section,
  AVG(num_xs_prod) AS avg_xs_prod_search
FROM (
  SELECT
    dt,
    user_id,
    'DAILY' AS period,
    'BY ALL DIMENSIONS' AS metric,
    country,
    interface,
    main_product,
    channel,
    section,
    COUNT(DISTINCT secondary_product) AS num_xs_prod
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
    9)
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8