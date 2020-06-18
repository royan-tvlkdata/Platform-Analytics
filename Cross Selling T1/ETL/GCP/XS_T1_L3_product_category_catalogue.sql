#standardSQL
/* 
Creator : royan.aldian@traveloka.com
Destination table: `tvlk-data-user-dev.xs_playground.XS_T1_L3_product_category_catalogue`
Output Field: 
    * secondary_product,
    * business_unit,
Data Processing:
    * ~nGB data processed per month
    * ~n rows inserted per month
    * n sec for elapsed time per query hit (daily)
Est Cost Processing: 
    * Streaming Insert: $0.05/1GB * nGB = $n
    * Queries: $0,005/1GB * nGB = $n
*/

-- CREATE OR REPLACE TABLE `tvlk-data-user-dev.xs_playground.XS_T1_L3_product_category_catalogue` AS 

SELECT
  distinct
  secondary_product.product_category AS secondary_product,
  secondary_product.business_unit AS business_unit
FROM
  `tvlk-data-user-dev.xs_playground.XS_T1_L3_transaction_all_product`
ORDER BY
  2,
  1