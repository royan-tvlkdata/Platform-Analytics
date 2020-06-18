#standardSQL
/* 
Creator : royan.aldian@traveloka.com
Destination table: `tvlk-data-user-dev.xs_playground.XS_T1_L4_all_addons_revenue`
Output Field: 
    * transaction_dt,
    * period,
    * metric,
    * country,
    * interface,
    * main_product,
    * secondary_product,
    * channel,
    * business_unit
    * section
    * num_transaction,
    * num_session,
    * gbv_usd,
    * net_rev_usd,
    * avg_gbv_usd,
    * avg_net_rev_usd,
    * avg_gbv_usd_per_bid,
    * avg_net_rev_usd_per_bid
Data Processing:
    * ~0.3GB data processed per month
    * ~n rows inserted per month
    * n sec for elapsed time per query hit (daily)
Est Cost Processing: 
    * Streaming Insert: $0.05/1GB * 0.01GB = $0.015
    * Queries: $0,005/1GB * 165GB = $0.0015
*/

-- CREATE OR REPLACE TABLE `tvlk-data-user-dev.xs_playground.XS_T1_L4_all_addons_revenue`
-- PARTITION BY (transaction_dt) AS 

-- DELETE FROM 
--   `tvlk-data-user-dev.xs_playground.XS_T1_L4_all_addons_revenue`
-- WHERE
--   ((EXTRACT(day
--       FROM
--         CURRENT_DATE()) = 1
--       AND DATE_TRUNC(transaction_dt, MONTH) = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -1 MONTH))
--     OR (EXTRACT(day
--       FROM
--         CURRENT_DATE()) > 1
--       AND DATE_TRUNC(transaction_dt, MONTH) = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 0 MONTH)))
--   AND period IN ('MONTHLY');

-- INSERT INTO `tvlk-data-user-dev.xs_playground.XS_T1_L4_all_addons_revenue`


WITH
  base_data_daily AS(
  SELECT
    transaction_dt,
    country,
    interface,
    main_product,
    secondary_product.product_category AS secondary_product,
    secondary_product.business_unit AS business_unit,
    channel,
    section,
    booking_id,
    session_id,
    profile_id,
    gbv_usd,
    net_rev_usd
  FROM
    `tvlk-data-user-dev.xs_playground.XS_T1_L3_transaction_all_product`
  WHERE
--     transaction_dt = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) ),
    transaction_dt BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
    AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) ),

  base_data_monthly AS(
  SELECT
    transaction_dt,
    country,
    interface,
    main_product,
    secondary_product.product_category AS secondary_product,
    secondary_product.business_unit AS business_unit,
    channel,
    section,
    booking_id,
    session_id,
    profile_id,
    gbv_usd,
    net_rev_usd
  FROM
    `tvlk-data-user-dev.xs_playground.XS_T1_L3_transaction_all_product`
  WHERE
--     ((EXTRACT(day
--         FROM
--           CURRENT_DATE()) = 1
--         AND DATE_TRUNC(transaction_dt, MONTH) = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -1 MONTH))
--       OR (EXTRACT(day
--         FROM
--           CURRENT_DATE()) > 1
--         AND DATE_TRUNC(transaction_dt, MONTH) = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 0 MONTH)))
    transaction_dt BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
    AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
    )
    
SELECT
  DATE_TRUNC(transaction_dt, MONTH) AS transaction_dt,
  'MONTHLY' AS period,
  'OVERALL' as metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  COUNT(DISTINCT booking_id) AS num_transaction,
  COUNT(DISTINCT session_id) AS num_session,
  SUM(gbv_usd) AS gbv_usd,
  SUM(net_rev_usd) AS net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT profile_id)), 4) AS avg_gbv_usd,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT profile_id)), 4) AS avg_net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT booking_id)), 4) AS avg_gbv_usd_per_bid,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT booking_id)), 4) AS avg_net_rev_usd_per_bid
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
  DATE_TRUNC(transaction_dt, MONTH) AS transaction_dt,
  'MONTHLY' AS period,
  'BY ALL DIMENSIONS' as metric,
  country,
  interface,
  main_product,
  secondary_product,
  channel,
  business_unit,
  section,
  COUNT(DISTINCT booking_id) AS num_transaction,
  COUNT(DISTINCT session_id) AS num_session,
  SUM(gbv_usd) AS gbv_usd,
  SUM(net_rev_usd) AS net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT profile_id)), 4) AS avg_gbv_usd,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT profile_id)), 4) AS avg_net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT booking_id)), 4) AS avg_gbv_usd_per_bid,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT booking_id)), 4) AS avg_net_rev_usd_per_bid
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
  DATE_TRUNC(transaction_dt, MONTH) AS transaction_dt,
  'MONTHLY' AS period,
  'BY CROSS SELL PRODUCT' as metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS main_product,
  secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  COUNT(DISTINCT booking_id) AS num_transaction,
  COUNT(DISTINCT session_id) AS num_session,
  SUM(gbv_usd) AS gbv_usd,
  SUM(net_rev_usd) AS net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT profile_id)), 4) AS avg_gbv_usd,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT profile_id)), 4) AS avg_net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT booking_id)), 4) AS avg_gbv_usd_per_bid,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT booking_id)), 4) AS avg_net_rev_usd_per_bid
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
  DATE_TRUNC(transaction_dt, MONTH) AS transaction_dt,
  'MONTHLY' AS period,
  'BY HOSTING PRODUCT' as metric,
  'ALL' AS country,
  'ALL' AS interface,
  main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  COUNT(DISTINCT booking_id) AS num_transaction,
  COUNT(DISTINCT session_id) AS num_session,
  SUM(gbv_usd) AS gbv_usd,
  SUM(net_rev_usd) AS net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT profile_id)), 4) AS avg_gbv_usd,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT profile_id)), 4) AS avg_net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT booking_id)), 4) AS avg_gbv_usd_per_bid,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT booking_id)), 4) AS avg_net_rev_usd_per_bid
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
  DATE_TRUNC(transaction_dt, MONTH) AS transaction_dt,
  'MONTHLY' AS period,
  'BY HOSTING PRODUCT & CROSS SELL PRODUCT' as metric,
  'ALL' AS country,
  'ALL' AS interface,
  main_product,
  secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  COUNT(DISTINCT booking_id) AS num_transaction,
  COUNT(DISTINCT session_id) AS num_session,
  SUM(gbv_usd) AS gbv_usd,
  SUM(net_rev_usd) AS net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT profile_id)), 4) AS avg_gbv_usd,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT profile_id)), 4) AS avg_net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT booking_id)), 4) AS avg_gbv_usd_per_bid,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT booking_id)), 4) AS avg_net_rev_usd_per_bid
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
  DATE_TRUNC(transaction_dt, MONTH) AS transaction_dt,
  'MONTHLY' AS period,
  'BY INTERFACE' as metric,
  'ALL' AS country,
  interface,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  COUNT(DISTINCT booking_id) AS num_transaction,
  COUNT(DISTINCT session_id) AS num_session,
  SUM(gbv_usd) AS gbv_usd,
  SUM(net_rev_usd) AS net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT profile_id)), 4) AS avg_gbv_usd,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT profile_id)), 4) AS avg_net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT booking_id)), 4) AS avg_gbv_usd_per_bid,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT booking_id)), 4) AS avg_net_rev_usd_per_bid
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
  DATE_TRUNC(transaction_dt, MONTH) AS transaction_dt,
  'MONTHLY' AS period,
  'BY COUNTRY' as metric,
  country,
  'ALL' AS interface,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  COUNT(DISTINCT booking_id) AS num_transaction,
  COUNT(DISTINCT session_id) AS num_session,
  SUM(gbv_usd) AS gbv_usd,
  SUM(net_rev_usd) AS net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT profile_id)), 4) AS avg_gbv_usd,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT profile_id)), 4) AS avg_net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT booking_id)), 4) AS avg_gbv_usd_per_bid,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT booking_id)), 4) AS avg_net_rev_usd_per_bid
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
  DATE_TRUNC(transaction_dt, MONTH) AS transaction_dt,
  'MONTHLY' AS period,
  'BY CHANNEL' as metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  channel,
  'ALL' AS business_unit,
  section,
  COUNT(DISTINCT booking_id) AS num_transaction,
  COUNT(DISTINCT session_id) AS num_session,
  SUM(gbv_usd) AS gbv_usd,
  SUM(net_rev_usd) AS net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT profile_id)), 4) AS avg_gbv_usd,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT profile_id)), 4) AS avg_net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT booking_id)), 4) AS avg_gbv_usd_per_bid,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT booking_id)), 4) AS avg_net_rev_usd_per_bid
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
  DATE_TRUNC(transaction_dt, MONTH) AS transaction_dt,
  'MONTHLY' AS period,
  'BY BUSINESS UNIT' as metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  business_unit,
  section,
  COUNT(DISTINCT booking_id) AS num_transaction,
  COUNT(DISTINCT session_id) AS num_session,
  SUM(gbv_usd) AS gbv_usd,
  SUM(net_rev_usd) AS net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT profile_id)), 4) AS avg_gbv_usd,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT profile_id)), 4) AS avg_net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT booking_id)), 4) AS avg_gbv_usd_per_bid,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT booking_id)), 4) AS avg_net_rev_usd_per_bid
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
  DATE_TRUNC(transaction_dt, MONTH) AS transaction_dt,
  'MONTHLY' AS period,
  'BY CROSS SELL PRODUCT & COUNTRY' as metric,
  country,
  'ALL' AS interface,
  'ALL' AS main_product,
  secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  COUNT(DISTINCT booking_id) AS num_transaction,
  COUNT(DISTINCT session_id) AS num_session,
  SUM(gbv_usd) AS gbv_usd,
  SUM(net_rev_usd) AS net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT profile_id)), 4) AS avg_gbv_usd,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT profile_id)), 4) AS avg_net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT booking_id)), 4) AS avg_gbv_usd_per_bid,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT booking_id)), 4) AS avg_net_rev_usd_per_bid
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
  DATE_TRUNC(transaction_dt, MONTH) AS transaction_dt,
  'MONTHLY' AS period,
  'BY CROSS SELL PRODUCT & INTERFACE' as metric,
  'ALL' AS country,
  interface,
  'ALL' AS main_product,
  secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  COUNT(DISTINCT booking_id) AS num_transaction,
  COUNT(DISTINCT session_id) AS num_session,
  SUM(gbv_usd) AS gbv_usd,
  SUM(net_rev_usd) AS net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT profile_id)), 4) AS avg_gbv_usd,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT profile_id)), 4) AS avg_net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT booking_id)), 4) AS avg_gbv_usd_per_bid,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT booking_id)), 4) AS avg_net_rev_usd_per_bid
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
  DATE_TRUNC(transaction_dt, MONTH) AS transaction_dt,
  'MONTHLY' AS period,
  'BY CROSS SELL PRODUCT, COUNTRY & INTERFACE' as metric,
  country,
  interface,
  'ALL' AS main_product,
  secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  COUNT(DISTINCT booking_id) AS num_transaction,
  COUNT(DISTINCT session_id) AS num_session,
  SUM(gbv_usd) AS gbv_usd,
  SUM(net_rev_usd) AS net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT profile_id)), 4) AS avg_gbv_usd,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT profile_id)), 4) AS avg_net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT booking_id)), 4) AS avg_gbv_usd_per_bid,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT booking_id)), 4) AS avg_net_rev_usd_per_bid
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
  transaction_dt,
  'DAILY' AS period,
  'OVERALL' as metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  COUNT(DISTINCT booking_id) AS num_transaction,
  COUNT(DISTINCT session_id) AS num_session,
  SUM(gbv_usd) AS gbv_usd,
  SUM(net_rev_usd) AS net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT profile_id)), 4) AS avg_gbv_usd,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT profile_id)), 4) AS avg_net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT booking_id)), 4) AS avg_gbv_usd_per_bid,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT booking_id)), 4) AS avg_net_rev_usd_per_bid
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
  transaction_dt,
  'DAILY' AS period,
  'BY ALL DIMENSIONS' as metric,
  country,
  interface,
  main_product,
  secondary_product,
  channel,
  business_unit,
  section,
  COUNT(DISTINCT booking_id) AS num_transaction,
  COUNT(DISTINCT session_id) AS num_session,
  SUM(gbv_usd) AS gbv_usd,
  SUM(net_rev_usd) AS net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT profile_id)), 4) AS avg_gbv_usd,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT profile_id)), 4) AS avg_net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT booking_id)), 4) AS avg_gbv_usd_per_bid,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT booking_id)), 4) AS avg_net_rev_usd_per_bid
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
  transaction_dt,
  'DAILY' AS period,
  'BY CROSS SELL PRODUCT' as metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS main_product,
  secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  COUNT(DISTINCT booking_id) AS num_transaction,
  COUNT(DISTINCT session_id) AS num_session,
  SUM(gbv_usd) AS gbv_usd,
  SUM(net_rev_usd) AS net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT profile_id)), 4) AS avg_gbv_usd,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT profile_id)), 4) AS avg_net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT booking_id)), 4) AS avg_gbv_usd_per_bid,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT booking_id)), 4) AS avg_net_rev_usd_per_bid
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
  transaction_dt,
  'DAILY' AS period,
  'BY HOSTING PRODUCT' as metric,
  'ALL' AS country,
  'ALL' AS interface,
  main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  COUNT(DISTINCT booking_id) AS num_transaction,
  COUNT(DISTINCT session_id) AS num_session,
  SUM(gbv_usd) AS gbv_usd,
  SUM(net_rev_usd) AS net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT profile_id)), 4) AS avg_gbv_usd,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT profile_id)), 4) AS avg_net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT booking_id)), 4) AS avg_gbv_usd_per_bid,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT booking_id)), 4) AS avg_net_rev_usd_per_bid
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
  transaction_dt,
  'DAILY' AS period,
  'BY HOSTING PRODUCT & CROSS SELL PRODUCT' as metric,
  'ALL' AS country,
  'ALL' AS interface,
  main_product,
  secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  COUNT(DISTINCT booking_id) AS num_transaction,
  COUNT(DISTINCT session_id) AS num_session,
  SUM(gbv_usd) AS gbv_usd,
  SUM(net_rev_usd) AS net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT profile_id)), 4) AS avg_gbv_usd,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT profile_id)), 4) AS avg_net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT booking_id)), 4) AS avg_gbv_usd_per_bid,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT booking_id)), 4) AS avg_net_rev_usd_per_bid
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
  transaction_dt,
  'DAILY' AS period,
  'BY INTERFACE' as metric,
  'ALL' AS country,
  interface,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  COUNT(DISTINCT booking_id) AS num_transaction,
  COUNT(DISTINCT session_id) AS num_session,
  SUM(gbv_usd) AS gbv_usd,
  SUM(net_rev_usd) AS net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT profile_id)), 4) AS avg_gbv_usd,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT profile_id)), 4) AS avg_net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT booking_id)), 4) AS avg_gbv_usd_per_bid,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT booking_id)), 4) AS avg_net_rev_usd_per_bid
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
  transaction_dt,
  'DAILY' AS period,
  'BY COUNTRY' as metric,
  country,
  'ALL' AS interface,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  COUNT(DISTINCT booking_id) AS num_transaction,
  COUNT(DISTINCT session_id) AS num_session,
  SUM(gbv_usd) AS gbv_usd,
  SUM(net_rev_usd) AS net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT profile_id)), 4) AS avg_gbv_usd,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT profile_id)), 4) AS avg_net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT booking_id)), 4) AS avg_gbv_usd_per_bid,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT booking_id)), 4) AS avg_net_rev_usd_per_bid
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
  transaction_dt,
  'DAILY' AS period,
  'BY CHANNEL' as metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  channel,
  'ALL' AS business_unit,
  section,
  COUNT(DISTINCT booking_id) AS num_transaction,
  COUNT(DISTINCT session_id) AS num_session,
  SUM(gbv_usd) AS gbv_usd,
  SUM(net_rev_usd) AS net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT profile_id)), 4) AS avg_gbv_usd,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT profile_id)), 4) AS avg_net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT booking_id)), 4) AS avg_gbv_usd_per_bid,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT booking_id)), 4) AS avg_net_rev_usd_per_bid
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
  transaction_dt,
  'DAILY' AS period,
  'BY BUSINESS UNIT' as metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  business_unit,
  section,
  COUNT(DISTINCT booking_id) AS num_transaction,
  COUNT(DISTINCT session_id) AS num_session,
  SUM(gbv_usd) AS gbv_usd,
  SUM(net_rev_usd) AS net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT profile_id)), 4) AS avg_gbv_usd,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT profile_id)), 4) AS avg_net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT booking_id)), 4) AS avg_gbv_usd_per_bid,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT booking_id)), 4) AS avg_net_rev_usd_per_bid
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
  transaction_dt,
  'DAILY' AS period,
  'BY CROSS SELL PRODUCT & COUNTRY' as metric,
  country,
  'ALL' AS interface,
  'ALL' AS main_product,
  secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  COUNT(DISTINCT booking_id) AS num_transaction,
  COUNT(DISTINCT session_id) AS num_session,
  SUM(gbv_usd) AS gbv_usd,
  SUM(net_rev_usd) AS net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT profile_id)), 4) AS avg_gbv_usd,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT profile_id)), 4) AS avg_net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT booking_id)), 4) AS avg_gbv_usd_per_bid,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT booking_id)), 4) AS avg_net_rev_usd_per_bid
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
  transaction_dt,
  'DAILY' AS period,
  'BY CROSS SELL PRODUCT & INTERFACE' as metric,
  'ALL' AS country,
  interface,
  'ALL' AS main_product,
  secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  COUNT(DISTINCT booking_id) AS num_transaction,
  COUNT(DISTINCT session_id) AS num_session,
  SUM(gbv_usd) AS gbv_usd,
  SUM(net_rev_usd) AS net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT profile_id)), 4) AS avg_gbv_usd,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT profile_id)), 4) AS avg_net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT booking_id)), 4) AS avg_gbv_usd_per_bid,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT booking_id)), 4) AS avg_net_rev_usd_per_bid
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
  transaction_dt,
  'DAILY' AS period,
  'BY CROSS SELL PRODUCT, COUNTRY & INTERFACE' as metric,
  country,
  interface,
  'ALL' AS main_product,
  secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  COUNT(DISTINCT booking_id) AS num_transaction,
  COUNT(DISTINCT session_id) AS num_session,
  SUM(gbv_usd) AS gbv_usd,
  SUM(net_rev_usd) AS net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT profile_id)), 4) AS avg_gbv_usd,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT profile_id)), 4) AS avg_net_rev_usd,
  ROUND(SAFE_DIVIDE(SUM(gbv_usd), COUNT(DISTINCT booking_id)), 4) AS avg_gbv_usd_per_bid,
  ROUND(SAFE_DIVIDE(SUM(net_rev_usd), COUNT(DISTINCT booking_id)), 4) AS avg_net_rev_usd_per_bid
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