#standardSQL
/* 
Creator : royan.aldian@traveloka.com
Destination table: `tvlk-data-user-dev.xs_playground.XS_T1_L4_product_breadth_issued_details
Output Field: 
    * dt,
    * period,
    * metric,
    * country,
    * interface,
    * main_product,
    * channel,
    * num_xs_prod 
    * items_xs_prod 
    * vol_user
Data Processing:
    * ~9.42GB data processed per month
    * ~n rows inserted per month
    * 6.9 sec for elapsed time per query hit (daily)
Est Cost Processing: 
    * Streaming Insert: $0.05/1GB * 9.42GB = $0.47
    * Queries: $0,005/1GB * 9.42GB = $0.047
*/
      
-- CREATE OR REPLACE TABLE `tvlk-data-user-dev.xs_playground.XS_T1_L4_product_breadth_issued_details`
-- PARTITION BY (dt) AS

DELETE FROM 
  `tvlk-data-user-dev.xs_playground.XS_T1_L4_product_breadth_issued_details`
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

INSERT INTO `tvlk-data-user-dev.xs_playground.XS_T1_L4_product_breadth_issued_details`


WITH
  base_data_daily AS(
  SELECT
    transaction_dt AS dt,
    country,
    interface,
    main_product,
    secondary_product.product_category AS secondary_product,
    channel,
    section,
    profile_id AS user_id
  FROM
    `tvlk-data-user-dev.xs_playground.XS_T1_L3_transaction_all_product`
  WHERE
    transaction_dt = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) ),
--     transaction_dt BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
--     AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) ),

  base_data_monthly AS(
  SELECT
    transaction_dt AS dt,
    country,
    interface,
    main_product,
    secondary_product.product_category AS secondary_product,
    channel,
    section,
    profile_id AS user_id
  FROM
    `tvlk-data-user-dev.xs_playground.XS_T1_L3_transaction_all_product`
  WHERE
    ((EXTRACT(day
        FROM
          CURRENT_DATE()) = 1
        AND DATE_TRUNC(transaction_dt, MONTH) = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -1 MONTH))
      OR (EXTRACT(day
        FROM
          CURRENT_DATE()) > 1
        AND DATE_TRUNC(transaction_dt, MONTH) = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 0 MONTH)))
--     transaction_dt BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
--     AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
    )
    
    
SELECT
  dt,
  period,
  metric,
  country,
  interface,
  main_product,
  channel,
  section,
  num_xs_prod,
  items_xs_prod,
  COUNT(DISTINCT user_id) AS vol_user
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
    COUNT(DISTINCT secondary_product) AS num_xs_prod,
    CONCAT('[', COUNT(DISTINCT secondary_product), '] ', STRING_AGG(DISTINCT secondary_product, ', '
      ORDER BY
        secondary_product)) items_xs_prod
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
  8,
  9,
  10
  
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
  num_xs_prod,
  items_xs_prod,
  COUNT(DISTINCT user_id) AS vol_user
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
    COUNT(DISTINCT secondary_product) AS num_xs_prod,
    CONCAT('[', COUNT(DISTINCT secondary_product), '] ', STRING_AGG(DISTINCT secondary_product, ', '
      ORDER BY
        secondary_product)) items_xs_prod
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
  8,
  9,
  10
 
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
  num_xs_prod,
  items_xs_prod,
  COUNT(DISTINCT user_id) AS vol_user
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
    COUNT(DISTINCT secondary_product) AS num_xs_prod,
    CONCAT('[', COUNT(DISTINCT secondary_product), '] ', STRING_AGG(DISTINCT secondary_product, ', '
      ORDER BY
        secondary_product)) items_xs_prod
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
  8,
  9,
  10
    
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
  num_xs_prod,
  items_xs_prod,
  COUNT(DISTINCT user_id) AS vol_user
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
    COUNT(DISTINCT secondary_product) AS num_xs_prod,
    CONCAT('[', COUNT(DISTINCT secondary_product), '] ', STRING_AGG(DISTINCT secondary_product, ', '
      ORDER BY
        secondary_product)) items_xs_prod
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
  8,
  9,
  10
    
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
  num_xs_prod,
  items_xs_prod,
  COUNT(DISTINCT user_id) AS vol_user
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
    COUNT(DISTINCT secondary_product) AS num_xs_prod,
    CONCAT('[', COUNT(DISTINCT secondary_product), '] ', STRING_AGG(DISTINCT secondary_product, ', '
      ORDER BY
        secondary_product)) items_xs_prod
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
  8,
  9,
  10
    
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
  num_xs_prod,
  items_xs_prod,
  COUNT(DISTINCT user_id) AS vol_user
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
    COUNT(DISTINCT secondary_product) AS num_xs_prod,
    CONCAT('[', COUNT(DISTINCT secondary_product), '] ', STRING_AGG(DISTINCT secondary_product, ', '
      ORDER BY
        secondary_product)) items_xs_prod
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
  8,
  9,
  10
    
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
  num_xs_prod,
  items_xs_prod,
  COUNT(DISTINCT user_id) AS vol_user
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
    COUNT(DISTINCT secondary_product) AS num_xs_prod,
    CONCAT('[', COUNT(DISTINCT secondary_product), '] ', STRING_AGG(DISTINCT secondary_product, ', '
      ORDER BY
        secondary_product)) items_xs_prod
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
  8,
  9,
  10


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
  num_xs_prod,
  items_xs_prod,
  COUNT(DISTINCT user_id) AS vol_user
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
    COUNT(DISTINCT secondary_product) AS num_xs_prod,
    CONCAT('[', COUNT(DISTINCT secondary_product), '] ', STRING_AGG(DISTINCT secondary_product, ', '
      ORDER BY
        secondary_product)) items_xs_prod
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
    9)
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
  period,
  metric,
  country,
  interface,
  main_product,
  channel,
  section,
  num_xs_prod,
  items_xs_prod,
  COUNT(DISTINCT user_id) AS vol_user
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
    COUNT(DISTINCT secondary_product) AS num_xs_prod,
    CONCAT('[', COUNT(DISTINCT secondary_product), '] ', STRING_AGG(DISTINCT secondary_product, ', '
      ORDER BY
        secondary_product)) items_xs_prod
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
  8,
  9,
  10
 
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
  num_xs_prod,
  items_xs_prod,
  COUNT(DISTINCT user_id) AS vol_user
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
    COUNT(DISTINCT secondary_product) AS num_xs_prod,
    CONCAT('[', COUNT(DISTINCT secondary_product), '] ', STRING_AGG(DISTINCT secondary_product, ', '
      ORDER BY
        secondary_product)) items_xs_prod
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
  8,
  9,
  10
    
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
  num_xs_prod,
  items_xs_prod,
  COUNT(DISTINCT user_id) AS vol_user
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
    COUNT(DISTINCT secondary_product) AS num_xs_prod,
    CONCAT('[', COUNT(DISTINCT secondary_product), '] ', STRING_AGG(DISTINCT secondary_product, ', '
      ORDER BY
        secondary_product)) items_xs_prod
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
  8,
  9,
  10
    
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
  num_xs_prod,
  items_xs_prod,
  COUNT(DISTINCT user_id) AS vol_user
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
    COUNT(DISTINCT secondary_product) AS num_xs_prod,
    CONCAT('[', COUNT(DISTINCT secondary_product), '] ', STRING_AGG(DISTINCT secondary_product, ', '
      ORDER BY
        secondary_product)) items_xs_prod
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
  8,
  9,
  10
    
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
  num_xs_prod,
  items_xs_prod,
  COUNT(DISTINCT user_id) AS vol_user
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
    COUNT(DISTINCT secondary_product) AS num_xs_prod,
    CONCAT('[', COUNT(DISTINCT secondary_product), '] ', STRING_AGG(DISTINCT secondary_product, ', '
      ORDER BY
        secondary_product)) items_xs_prod
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
  8,
  9,
  10
    
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
  num_xs_prod,
  items_xs_prod,
  COUNT(DISTINCT user_id) AS vol_user
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
    COUNT(DISTINCT secondary_product) AS num_xs_prod,
    CONCAT('[', COUNT(DISTINCT secondary_product), '] ', STRING_AGG(DISTINCT secondary_product, ', '
      ORDER BY
        secondary_product)) items_xs_prod
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
  8,
  9,
  10


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
  num_xs_prod,
  items_xs_prod,
  COUNT(DISTINCT user_id) AS vol_user
FROM (
  SELECT
    DATE_TRUNC(dt, MONTH) AS dt,
    user_id,
    'MONTHLY' AS period,
    'OVERALL - ALL SECTIONS' AS metric,
    'ALL' AS country,
    'ALL' AS interface,
    'ALL' AS main_product,
    'ALL' AS channel,
    'ALL' AS section,
    COUNT(DISTINCT secondary_product) AS num_xs_prod,
    CONCAT('[', COUNT(DISTINCT secondary_product), '] ', STRING_AGG(DISTINCT secondary_product, ', '
      ORDER BY
        secondary_product)) items_xs_prod
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
  8,
  9,
  10
  
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
  num_xs_prod,
  items_xs_prod,
  COUNT(DISTINCT user_id) AS vol_user
FROM (
  SELECT
    DATE_TRUNC(dt, MONTH) AS dt,
    user_id,
    'MONTHLY' AS period, 
    'BY COUNTRY - ALL SECTIONS' AS metric,
    country,
    'ALL' AS interface,
    'ALL' AS main_product,
    'ALL' AS channel,
    'ALL' AS section,
    COUNT(DISTINCT secondary_product) AS num_xs_prod,
    CONCAT('[', COUNT(DISTINCT secondary_product), '] ', STRING_AGG(DISTINCT secondary_product, ', '
      ORDER BY
        secondary_product)) items_xs_prod
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
  8,
  9,
  10
 
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
  num_xs_prod,
  items_xs_prod,
  COUNT(DISTINCT user_id) AS vol_user
FROM (
  SELECT
    DATE_TRUNC(dt, MONTH) AS dt,
    user_id,
    'MONTHLY' AS period, 
    'BY INTERFACE - ALL SECTIONS' AS metric,
    'ALL' AS country,
    interface,
    'ALL' AS main_product,
    'ALL' AS channel,
    'ALL' AS section,
    COUNT(DISTINCT secondary_product) AS num_xs_prod,
    CONCAT('[', COUNT(DISTINCT secondary_product), '] ', STRING_AGG(DISTINCT secondary_product, ', '
      ORDER BY
        secondary_product)) items_xs_prod
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
  8,
  9,
  10
    
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
  num_xs_prod,
  items_xs_prod,
  COUNT(DISTINCT user_id) AS vol_user
FROM (
  SELECT
    DATE_TRUNC(dt, MONTH) AS dt,
    user_id,
    'MONTHLY' AS period,
    'BY HOSTING PRODUCT - ALL SECTIONS' AS metric,
    'ALL' AS country,
    'ALL' AS interface,
    main_product,
    'ALL' AS channel,
    'ALL' AS section,
    COUNT(DISTINCT secondary_product) AS num_xs_prod,
    CONCAT('[', COUNT(DISTINCT secondary_product), '] ', STRING_AGG(DISTINCT secondary_product, ', '
      ORDER BY
        secondary_product)) items_xs_prod
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
  8,
  9,
  10
    
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
  num_xs_prod,
  items_xs_prod,
  COUNT(DISTINCT user_id) AS vol_user
FROM (
  SELECT
    DATE_TRUNC(dt, MONTH) AS dt,
    user_id,
    'MONTHLY' AS period,
    'BY CHANNEL - ALL SECTIONS' AS metric,
    'ALL' AS country,
    'ALL' AS interface,
    'ALL' AS main_product,
    channel,
    'ALL' AS section,
    COUNT(DISTINCT secondary_product) AS num_xs_prod,
    CONCAT('[', COUNT(DISTINCT secondary_product), '] ', STRING_AGG(DISTINCT secondary_product, ', '
      ORDER BY
        secondary_product)) items_xs_prod
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
  8,
  9,
  10
    
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
  num_xs_prod,
  items_xs_prod,
  COUNT(DISTINCT user_id) AS vol_user
FROM (
  SELECT
    DATE_TRUNC(dt, MONTH) AS dt,
    user_id,
    'MONTHLY' AS period,
    'BY COUNTRY & INTERFACE - ALL SECTIONS' AS metric,
    country,
    interface,
    'ALL' AS main_product,
    'ALL' AS channel,
    'ALL' AS section,
    COUNT(DISTINCT secondary_product) AS num_xs_prod,
    CONCAT('[', COUNT(DISTINCT secondary_product), '] ', STRING_AGG(DISTINCT secondary_product, ', '
      ORDER BY
        secondary_product)) items_xs_prod
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
  8,
  9,
  10
    
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
  num_xs_prod,
  items_xs_prod,
  COUNT(DISTINCT user_id) AS vol_user
FROM (
  SELECT
    DATE_TRUNC(dt, MONTH) AS dt,
    user_id,
    'MONTHLY' AS period,
    'BY ALL DIMENSIONS - ALL SECTIONS' AS metric,
    country,
    interface,
    main_product,
    channel,
    'ALL' AS section,
    COUNT(DISTINCT secondary_product) AS num_xs_prod,
    CONCAT('[', COUNT(DISTINCT secondary_product), '] ', STRING_AGG(DISTINCT secondary_product, ', '
      ORDER BY
        secondary_product)) items_xs_prod
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
  8,
  9,
  10