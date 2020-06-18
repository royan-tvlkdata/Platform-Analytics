#standardSQL
/* 
Creator : royan.aldian@traveloka.com
Destination table: `tvlk-data-user-dev.xs_playground.XS_T1_L3_transaction_all_product`
Output Field: 
    * transaction_dt 
    * profile_id  
    * booking_id  
    * country 
    * interface 
    * session_id
    * channel 
    * section 
    * main_product  
    * secondary_product.product_category
    * secondary_product.business_unit
    * currency,
    * fare_amount,
    * gbv_lcy,
    * net_rev_lcy,
    * gbv_usd,
    * net_rev_usd
Data Processing:
    * ~n GB data processed per month
    * ~n K rows inserted per month
    * n sec for elapsed time per query hit (daily)
Est Cost Processing: 
    * Streaming Insert: $0.05/1GB * n GB = $n
    * Queries: $0,005/1GB * n GB = $n
*/

CREATE TEMP FUNCTION
  PROPER(str STRING) AS ((
    SELECT
      REPLACE(STRING_AGG(CONCAT(UPPER(SUBSTR(w,1,1)), LOWER(SUBSTR(w,2))), ' '
        ORDER BY
          pos), ' ','')
    FROM
      UNNEST(SPLIT(str, ' ')) w
    WITH
    OFFSET
      pos ));
      
-- CREATE OR REPLACE TABLE `tvlk-data-user-dev.xs_playground.XS_T1_L3_transaction_all_product`
-- PARTITION BY (transaction_dt) AS 

WITH
  transaction_base AS(
  SELECT
    'AIRPORT TRANSFER' AS business_unit,
    'CROSS SELL' AS section,
    transaction_dt,
    profile_id,
    booking_id,
    country,
    interface,
    session_id,
    main_product,
  IF
    (REGEXP_CONTAINS(secondary_product, r'_'),
    PROPER(REPLACE(secondary_product, '_', ' ')),
    secondary_product) AS secondary_product,
    channel,
    currency,
    fare_amount,
    gbv_lcy,
    net_rev_lcy,
    gbv_usd,
    net_rev_usd
  FROM
    `tvlk-data-user-dev.xs_playground.XS_T1_L3_transaction_airport_transfer`
  WHERE
    transaction_dt = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
--     transaction_dt BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -2 YEAR)
--     AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
    
  UNION ALL
  
  SELECT
    'VEHICLE RENTAL' AS business_unit,
    'CROSS SELL' AS section,
    transaction_dt,
    profile_id,
    booking_id,
    country,
    interface,
    session_id,
    main_product,
  IF
    (REGEXP_CONTAINS(secondary_product, r'_'),
    PROPER(REPLACE(secondary_product, '_', ' ')),
    secondary_product) AS secondary_product,
    channel,
    currency,
    fare_amount,
    gbv_lcy,
    net_rev_lcy,
    gbv_usd,
    net_rev_usd 
  FROM
    `tvlk-data-user-dev.xs_playground.XS_T1_L3_transaction_vehicle_rental`
  WHERE
    transaction_dt = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
--     transaction_dt BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -2 YEAR)
--     AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
    
  UNION ALL
  
  SELECT
    'INSURANCE' AS business_unit,
    'CROSS SELL' AS section,
    transaction_dt,
    profile_id,
    booking_id,
    country,
    interface,
    session_id,
    main_product,
  IF
    (REGEXP_CONTAINS(secondary_product, r'_'),
    PROPER(REPLACE(secondary_product, '_', ' ')),
    secondary_product) AS secondary_product,
    channel,
    currency,
    fare_amount,
    gbv_lcy,
    net_rev_lcy,
    gbv_usd,
    net_rev_usd
  FROM
    `tvlk-data-user-dev.xs_playground.XS_T1_L3_transaction_insurance`
  WHERE
    transaction_dt = DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) )
--     transaction_dt BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -2 YEAR)
--     AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) )

SELECT
  transaction_dt,
  profile_id,
  booking_id,
  country,
  interface,
  session_id,
  channel,
  section,
  main_product,
  STRUCT(secondary_product AS product_category,
    business_unit) AS secondary_product,
  currency,
  fare_amount,
  gbv_lcy,
  net_rev_lcy,
  gbv_usd,
  net_rev_usd
FROM
  transaction_base