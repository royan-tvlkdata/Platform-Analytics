#standardSQL
/* 
Creator : royan.aldian@traveloka.com
Destination table: `tvlk-data-user-dev.xs_playground.XS_T1_L3_explored_cross_products_per_user`
Output Field: 
    * event_month 
    * profile_id  
    * base_secondary_product  
    * lead_secondary_product
Data Processing:
    * ~n GB data processed per month
    * ~n K rows inserted per month
    * n sec for elapsed time per query hit (daily)
Est Cost Processing: 
    * Streaming Insert: $0.05/1GB * n GB = $n
    * Queries: $0,005/1GB * n GB = $n
*/

-- CREATE OR REPLACE TABLE `tvlk-data-user-dev.xs_playground.XS_T1_L3_explored_cross_products_per_user`
-- PARTITION BY (event_month) AS 

WITH
  base AS (
  SELECT
    DISTINCT DATE_TRUNC(date, month) AS event_month,
    profile_id,
    product.secondary_product AS secondary_product
  FROM
    `tvlk-data-user-dev.xs_playground.XS_T1_L3_addon_activity`
  LEFT JOIN
    UNNEST(product) AS product
  WHERE
    DATE_TRUNC(date, MONTH) = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -1 MONTH) )
--     DATE_TRUNC(date, MONTH) BETWEEN DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -2 YEAR)
--     AND DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -1 MONTH) )

SELECT
  a.event_month,
  a.profile_id,
  a.secondary_product AS base_secondary_product,
  b.secondary_product AS lead_secondary_product,
FROM
  base a
LEFT JOIN
  base b
ON
  a.event_month = b.event_month
  AND a.profile_id = b.profile_id
--   AND a.secondary_product <= b.secondary_product