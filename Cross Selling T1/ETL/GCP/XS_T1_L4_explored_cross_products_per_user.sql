#standardSQL
  /* 
Creator : royan.aldian@traveloka.com
Destination table: `tvlk-data-user-dev.xs_playground.XS_T1_L4_explored_cross_products_per_user`
Output Field: 
    * event_month 
    * base_secondary_product  
    * lead_secondary_product
    * num_users
Data Processing:
    * ~n GB data processed per month
    * ~n K rows inserted per month
    * n sec for elapsed time per query hit (daily)
Est Cost Processing: 
    * Streaming Insert: $0.05/1GB * n GB = $n
    * Queries: $0,005/1GB * n GB = $n
*/

-- CREATE OR REPLACE TABLE `tvlk-data-user-dev.xs_playground.XS_T1_L4_explored_cross_products_per_user`
-- PARTITION BY (event_month) AS
  
SELECT
  event_month,
  base_secondary_product,
  lead_secondary_product,
  COUNT(DISTINCT profile_id) AS num_users
FROM
  `tvlk-data-user-dev.xs_playground.XS_T1_L3_explored_cross_products_per_user`
WHERE
  event_month = DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -1 MONTH)
--   event_month BETWEEN DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -2 YEAR)
--   AND DATE_ADD(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL -1 MONTH)
  AND profile_id IS NOT NULL
GROUP BY
  1,
  2,
  3