#standardSQL
/* 
Creator : royan.aldian@traveloka.com
Destination table: `tvlk-data-user-dev.xs_playground.XS_T1_L4_addon_take_up_rate`
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
    * vol_session_seen,
    * vol_session_trx,
    * take_up_rate
Data Processing:
    * ~nGB data processed per month
    * ~n rows inserted per month
    * n sec for elapsed time per query hit (daily)
Est Cost Processing: 
    * Streaming Insert: $0.05/1GB * nGB = $n
    * Queries: $0,005/1GB * nGB = $n
*/

-- CREATE OR REPLACE TABLE `tvlk-data-user-dev.xs_playground.XS_T1_L4_addon_take_up_rate`
-- PARTITION BY (date) AS 

-- DELETE FROM 
--   `tvlk-data-user-dev.xs_playground.XS_T1_L4_addon_take_up_rate`
-- WHERE 
--   ((EXTRACT(day
--       FROM
--         CURRENT_DATE()) = 1
--       AND DATE_TRUNC(date, MONTH) = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -1 MONTH))
--     OR (EXTRACT(day
--       FROM
--         CURRENT_DATE()) > 1
--       AND DATE_TRUNC(date, MONTH) = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 0 MONTH)))
--   AND period IN ('MONTHLY');

-- INSERT INTO `tvlk-data-user-dev.xs_playground.XS_T1_L4_addon_take_up_rate`

WITH
  base_data_daily AS(
  SELECT
    date,
    country,
    interface,
    channel,
    event_action,
    main_product,
    secondary_product,
    business_unit,
    section,
    session_seen,
    session_transaction,
    booking_primary,
    booking_secondary
  FROM
    `tvlk-data-user-dev.xs_playground.XS_T1_L3_addon_take_up_rate_aggregated`
  WHERE
--     date = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) ),
    date BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
    AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) ),

  base_data_monthly AS(
  SELECT
    date,
    country,
    interface,
    channel,
    event_action,
    main_product,
    secondary_product,
    business_unit,
    section,
    session_seen,
    session_transaction,
    booking_primary,
    booking_secondary
  FROM
    `tvlk-data-user-dev.xs_playground.XS_T1_L3_addon_take_up_rate_aggregated`
  WHERE
--     ((EXTRACT(day
--         FROM
--           CURRENT_DATE()) = 1
--         AND DATE_TRUNC(date, MONTH) = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -1 MONTH))
--       OR (EXTRACT(day
--         FROM
--           CURRENT_DATE()) > 1
--         AND DATE_TRUNC(date, MONTH) = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 0 MONTH)))
    date BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
    AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) 
    )

SELECT
  date,
  'DAILY' AS period,
  'OVERALL' as metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  SUM(session_seen) AS vol_session_seen,
  SUM(session_transaction) AS vol_session_transaction,
  ROUND(SAFE_DIVIDE(SUM(session_transaction), SUM(session_seen)), 6) AS take_up_rate,
  ROUND(SAFE_DIVIDE(SUM(booking_secondary), SUM(booking_primary)), 6) AS to_eligible_main_rate
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
  date,
  'DAILY' AS period,
  'BY ALL DIMENSIONS' as metric,
  country,
  interface,
  main_product,
  secondary_product,
  channel,
  business_unit,
  section,
  SUM(session_seen) AS vol_session_seen,
  SUM(session_transaction) AS vol_session_transaction,
  ROUND(SAFE_DIVIDE(SUM(session_transaction), SUM(session_seen)), 6) AS take_up_rate,
  ROUND(SAFE_DIVIDE(SUM(booking_secondary), SUM(booking_primary)), 6) AS to_eligible_main_rate
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
  date,
  'DAILY' AS period,
  'BY CROSS SELL PRODUCT' as metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS main_product,
  secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  SUM(session_seen) AS vol_session_seen,
  SUM(session_transaction) AS vol_session_transaction,
  ROUND(SAFE_DIVIDE(SUM(session_transaction), SUM(session_seen)), 6) AS take_up_rate,
  ROUND(SAFE_DIVIDE(SUM(booking_secondary), SUM(booking_primary)), 6) AS to_eligible_main_rate
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
  date,
  'DAILY' AS period,
  'BY HOSTING PRODUCT' as metric,
  'ALL' AS country,
  'ALL' AS interface,
  main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  SUM(session_seen) AS vol_session_seen,
  SUM(session_transaction) AS vol_session_transaction,
  ROUND(SAFE_DIVIDE(SUM(session_transaction), SUM(session_seen)), 6) AS take_up_rate,
  ROUND(SAFE_DIVIDE(SUM(booking_secondary), SUM(booking_primary)), 6) AS to_eligible_main_rate
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
  date,
  'DAILY' AS period,
  'BY HOSTING PRODUCT & CROSS SELL PRODUCT' as metric,
  'ALL' AS country,
  'ALL' AS interface,
  main_product,
  secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  SUM(session_seen) AS vol_session_seen,
  SUM(session_transaction) AS vol_session_transaction,
  ROUND(SAFE_DIVIDE(SUM(session_transaction), SUM(session_seen)), 6) AS take_up_rate,
  ROUND(SAFE_DIVIDE(SUM(booking_secondary), SUM(booking_primary)), 6) AS to_eligible_main_rate
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
  date,
  'DAILY' AS period,
  'BY INTERFACE' as metric,
  'ALL' AS country,
  interface,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  SUM(session_seen) AS vol_session_seen,
  SUM(session_transaction) AS vol_session_transaction,
  ROUND(SAFE_DIVIDE(SUM(session_transaction), SUM(session_seen)), 6) AS take_up_rate,
  ROUND(SAFE_DIVIDE(SUM(booking_secondary), SUM(booking_primary)), 6) AS to_eligible_main_rate
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
  date,
  'DAILY' AS period,
  'BY COUNTRY' as metric,
  country,
  'ALL' AS interface,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  SUM(session_seen) AS vol_session_seen,
  SUM(session_transaction) AS vol_session_transaction,
  ROUND(SAFE_DIVIDE(SUM(session_transaction), SUM(session_seen)), 6) AS take_up_rate,
  ROUND(SAFE_DIVIDE(SUM(booking_secondary), SUM(booking_primary)), 6) AS to_eligible_main_rate
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
  date,
  'DAILY' AS period,
  'BY CHANNEL' as metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  channel,
  'ALL' AS business_unit,
  section,
  SUM(session_seen) AS vol_session_seen,
  SUM(session_transaction) AS vol_session_transaction,
  ROUND(SAFE_DIVIDE(SUM(session_transaction), SUM(session_seen)), 6) AS take_up_rate,
  ROUND(SAFE_DIVIDE(SUM(booking_secondary), SUM(booking_primary)), 6) AS to_eligible_main_rate
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
  date,
  'DAILY' AS period,
  'BY BUSINESS UNIT' as metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  business_unit,
  section,
  SUM(session_seen) AS vol_session_seen,
  SUM(session_transaction) AS vol_session_transaction,
  ROUND(SAFE_DIVIDE(SUM(session_transaction), SUM(session_seen)), 6) AS take_up_rate,
  ROUND(SAFE_DIVIDE(SUM(booking_secondary), SUM(booking_primary)), 6) AS to_eligible_main_rate
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
  date,
  'DAILY' AS period,
  'BY CROSS SELL PRODUCT & COUNTRY' as metric,
  country,
  'ALL' AS interface,
  'ALL' AS main_product,
  secondary_product,
  'ALL' AS channel,
  channel,
  'ALL' AS business_unit,
  SUM(session_seen) AS vol_session_seen,
  SUM(session_transaction) AS vol_session_transaction,
  ROUND(SAFE_DIVIDE(SUM(session_transaction), SUM(session_seen)), 6) AS take_up_rate,
  ROUND(SAFE_DIVIDE(SUM(booking_secondary), SUM(booking_primary)), 6) AS to_eligible_main_rate
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
  date,
  'DAILY' AS period,
  'BY CROSS SELL PRODUCT & INTERFACE' as metric,
  'ALL' AS country,
  interface,
  'ALL' AS main_product,
  secondary_product,
  'ALL' AS channel,
  channel,
  'ALL' AS business_unit,
  SUM(session_seen) AS vol_session_seen,
  SUM(session_transaction) AS vol_session_transaction,
  ROUND(SAFE_DIVIDE(SUM(session_transaction), SUM(session_seen)), 6) AS take_up_rate,
  ROUND(SAFE_DIVIDE(SUM(booking_secondary), SUM(booking_primary)), 6) AS to_eligible_main_rate
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
  date,
  'DAILY' AS period,
  'BY CROSS SELL PRODUCT, COUNTRY & INTERFACE' as metric,
  country,
  interface,
  'ALL' AS main_product,
  secondary_product,
  'ALL' AS channel,
  channel,
  'ALL' AS business_unit,
  SUM(session_seen) AS vol_session_seen,
  SUM(session_transaction) AS vol_session_transaction,
  ROUND(SAFE_DIVIDE(SUM(session_transaction), SUM(session_seen)), 6) AS take_up_rate,
  ROUND(SAFE_DIVIDE(SUM(booking_secondary), SUM(booking_primary)), 6) AS to_eligible_main_rate
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
  DATE_TRUNC(date, MONTH) AS date,
  'MONTHLY' AS period,
  'OVERALL' as metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  SUM(session_seen) AS vol_session_seen,
  SUM(session_transaction) AS vol_session_transaction,
  ROUND(SAFE_DIVIDE(SUM(session_transaction), SUM(session_seen)), 6) AS take_up_rate,
  ROUND(SAFE_DIVIDE(SUM(booking_secondary), SUM(booking_primary)), 6) AS to_eligible_main_rate
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
  DATE_TRUNC(date, MONTH) AS date,
  'MONTHLY' AS period,
  'BY ALL DIMENSIONS' as metric,
  country,
  interface,
  main_product,
  secondary_product,
  channel,
  business_unit,
  section,
  SUM(session_seen) AS vol_session_seen,
  SUM(session_transaction) AS vol_session_transaction,
  ROUND(SAFE_DIVIDE(SUM(session_transaction), SUM(session_seen)), 6) AS take_up_rate,
  ROUND(SAFE_DIVIDE(SUM(booking_secondary), SUM(booking_primary)), 6) AS to_eligible_main_rate
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
  DATE_TRUNC(date, MONTH) AS date,
  'MONTHLY' AS period,
  'BY CROSS SELL PRODUCT' as metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS main_product,
  secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  SUM(session_seen) AS vol_session_seen,
  SUM(session_transaction) AS vol_session_transaction,
  ROUND(SAFE_DIVIDE(SUM(session_transaction), SUM(session_seen)), 6) AS take_up_rate,
  ROUND(SAFE_DIVIDE(SUM(booking_secondary), SUM(booking_primary)), 6) AS to_eligible_main_rate
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
  DATE_TRUNC(date, MONTH) AS date,
  'MONTHLY' AS period,
  'BY HOSTING PRODUCT' as metric,
  'ALL' AS country,
  'ALL' AS interface,
  main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  SUM(session_seen) AS vol_session_seen,
  SUM(session_transaction) AS vol_session_transaction,
  ROUND(SAFE_DIVIDE(SUM(session_transaction), SUM(session_seen)), 6) AS take_up_rate,
  ROUND(SAFE_DIVIDE(SUM(booking_secondary), SUM(booking_primary)), 6) AS to_eligible_main_rate
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
  DATE_TRUNC(date, MONTH) AS date,
  'MONTHLY' AS period,
  'BY HOSTING PRODUCT & CROSS SELL PRODUCT' as metric,
  'ALL' AS country,
  'ALL' AS interface,
  main_product,
  secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  SUM(session_seen) AS vol_session_seen,
  SUM(session_transaction) AS vol_session_transaction,
  ROUND(SAFE_DIVIDE(SUM(session_transaction), SUM(session_seen)), 6) AS take_up_rate,
  ROUND(SAFE_DIVIDE(SUM(booking_secondary), SUM(booking_primary)), 6) AS to_eligible_main_rate
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
  DATE_TRUNC(date, MONTH) AS date,
  'MONTHLY' AS period,
  'BY INTERFACE' as metric,
  'ALL' AS country,
  interface,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  SUM(session_seen) AS vol_session_seen,
  SUM(session_transaction) AS vol_session_transaction,
  ROUND(SAFE_DIVIDE(SUM(session_transaction), SUM(session_seen)), 6) AS take_up_rate,
  ROUND(SAFE_DIVIDE(SUM(booking_secondary), SUM(booking_primary)), 6) AS to_eligible_main_rate
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
  DATE_TRUNC(date, MONTH) AS date,
  'MONTHLY' AS period,
  'BY COUNTRY' as metric,
  country,
  'ALL' AS interface,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  'ALL' AS business_unit,
  section,
  SUM(session_seen) AS vol_session_seen,
  SUM(session_transaction) AS vol_session_transaction,
  ROUND(SAFE_DIVIDE(SUM(session_transaction), SUM(session_seen)), 6) AS take_up_rate,
  ROUND(SAFE_DIVIDE(SUM(booking_secondary), SUM(booking_primary)), 6) AS to_eligible_main_rate
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
  DATE_TRUNC(date, MONTH) AS date,
  'MONTHLY' AS period,
  'BY CHANNEL' as metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  channel,
  'ALL' AS business_unit,
  section,
  SUM(session_seen) AS vol_session_seen,
  SUM(session_transaction) AS vol_session_transaction,
  ROUND(SAFE_DIVIDE(SUM(session_transaction), SUM(session_seen)), 6) AS take_up_rate,
  ROUND(SAFE_DIVIDE(SUM(booking_secondary), SUM(booking_primary)), 6) AS to_eligible_main_rate
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
  DATE_TRUNC(date, MONTH) AS date,
  'MONTHLY' AS period,
  'BY BUSINESS UNIT' as metric,
  'ALL' AS country,
  'ALL' AS interface,
  'ALL' AS main_product,
  'ALL' AS secondary_product,
  'ALL' AS channel,
  business_unit,
  section,
  SUM(session_seen) AS vol_session_seen,
  SUM(session_transaction) AS vol_session_transaction,
  ROUND(SAFE_DIVIDE(SUM(session_transaction), SUM(session_seen)), 6) AS take_up_rate,
  ROUND(SAFE_DIVIDE(SUM(booking_secondary), SUM(booking_primary)), 6) AS to_eligible_main_rate
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
  DATE_TRUNC(date, MONTH) AS date,
  'MONTHLY' AS period,
  'BY CROSS SELL PRODUCT & COUNTRY' as metric,
  country,
  'ALL' AS interface,
  'ALL' AS main_product,
  secondary_product,
  'ALL' AS channel,
  channel,
  'ALL' AS business_unit,
  SUM(session_seen) AS vol_session_seen,
  SUM(session_transaction) AS vol_session_transaction,
  ROUND(SAFE_DIVIDE(SUM(session_transaction), SUM(session_seen)), 6) AS take_up_rate,
  ROUND(SAFE_DIVIDE(SUM(booking_secondary), SUM(booking_primary)), 6) AS to_eligible_main_rate
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
  DATE_TRUNC(date, MONTH) AS date,
  'MONTHLY' AS period,
  'BY CROSS SELL PRODUCT & INTERFACE' as metric,
  'ALL' AS country,
  interface,
  'ALL' AS main_product,
  secondary_product,
  'ALL' AS channel,
  channel,
  'ALL' AS business_unit,
  SUM(session_seen) AS vol_session_seen,
  SUM(session_transaction) AS vol_session_transaction,
  ROUND(SAFE_DIVIDE(SUM(session_transaction), SUM(session_seen)), 6) AS take_up_rate,
  ROUND(SAFE_DIVIDE(SUM(booking_secondary), SUM(booking_primary)), 6) AS to_eligible_main_rate
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
  DATE_TRUNC(date, MONTH) AS date,
  'MONTHLY' AS period,
  'BY CROSS SELL PRODUCT, COUNTRY & INTERFACE' as metric,
  country,
  interface,
  'ALL' AS main_product,
  secondary_product,
  'ALL' AS channel,
  channel,
  'ALL' AS business_unit,
  SUM(session_seen) AS vol_session_seen,
  SUM(session_transaction) AS vol_session_transaction,
  ROUND(SAFE_DIVIDE(SUM(session_transaction), SUM(session_seen)), 6) AS take_up_rate,
  ROUND(SAFE_DIVIDE(SUM(booking_secondary), SUM(booking_primary)), 6) AS to_eligible_main_rate
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