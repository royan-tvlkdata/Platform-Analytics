#standardSQL
/* 
Creator : royan.aldian@traveloka.com
Destination table: `tvlk-data-user-dev.xs_playground.XS_T1_L3_addon_activity`
Output Field: 
    * date                   
    * profile_id  
    * cookie_id 
    * session_id  
    * country 
    * interface 
    * channel 
    * section 
    * main_product
    * event_action
    * product.secondary_product
Data Processing:
    * ~3.48GB data processed per month
    * ~60K rows inserted per month
    * 4.9 sec for elapsed time per query hit (daily)
Est Cost Processing: 
    * Streaming Insert: $0.05/1GB * 3.48GB = $0.17
    * Queries: $0,005/1GB * 3.48GB = $0.017
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

-- CREATE OR REPLACE TABLE `tvlk-data-user-dev.xs_playground.XS_T1_L3_addon_activity`
-- PARTITION BY (date) AS

WITH
  addon_sbf AS(
  SELECT
    kafka_publish_timestamp,
    profile_id,
    session_id,
    cookie_id,
    country,
    login_id,
    intf AS interface,
    event_action,
    UPPER(prod.main_product) AS main_product,
    ps.name AS section,
    psi.id AS items_id,
    CAST(REGEXP_EXTRACT(psi.id, r'([\d]+)') AS INT64) AS product_category_id
  FROM
    `tvlk-realtime.nrtprod.user_booking_form`
  LEFT JOIN
    UNNEST(page) AS p
  LEFT JOIN
    UNNEST(p.sections) AS ps
  LEFT JOIN
    UNNEST(ps.items) AS psi
  LEFT JOIN
    UNNEST(products) AS prod
  WHERE
    _PARTITIONTIME = CAST(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) AS TIMESTAMP)
--     DATE(_PARTITIONTIME) BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -1 YEAR)
--     AND DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
    AND ps.name IN ('CROSS_SELL_ADD_ON',
      'SIMPLE_ADD_ON',
      'PRODUCT_ADD_ON')
    AND event_action IN ('EXPAND',
      'SELECT_ITEM',
      'TAP_ITEM_INFO',
      'TAP_EXTENSION',
      'COLLAPSE',
      'UNSELECT_ITEM',
      'TAP_ITEM',
      'SCROLL',
      'SEEN')
    AND UPPER(psi.id) NOT LIKE '%TITLE%'
    AND prod.secondary_product IS NULL ),
  INS AS (
  SELECT
    DISTINCT insurance_plan_id AS product_category_id,
    product_name AS product_category
  FROM
    `tvlk-realtime.pg.insurance_product_insurance_booking_item`
--   WHERE
--     DATE(_PARTITIONTIME) BETWEEN DATE_ADD(CURRENT_DATE(), INTERVAL -2 DAY) /* change interval when needed */
--     AND CURRENT_DATE() 
    )

SELECT
  date,
  profile_id,
  cookie_id,
  session_id,
  country,
  interface,
  channel,
  section,
  main_product,
  event_action,
  ARRAY_AGG(STRUCT(
    IF
      (REGEXP_CONTAINS(secondary_product, r'_'),
        PROPER(REPLACE(secondary_product, '_', ' ')),
        secondary_product) AS secondary_product)) AS product
FROM (
  SELECT
    DATE(DATETIME_ADD(kafka_publish_timestamp,
        INTERVAL 7 HOUR)) AS date,
    profile_id,
    cookie_id,
    session_id,
    country,
    interface,
    'BOOKING FORM' AS channel,
    CASE
      WHEN section = 'PRODUCT_ADD_ON' THEN 'ANCILLARY'
    ELSE
    'CROSS SELL'
  END
    AS section,
    main_product,
    event_action,
    CASE
      WHEN items_id = "AIRPORT_TRANSFER_CROSSSELL_ADDON" THEN "AIRPORT_TRANSFER_CROSSSELL_ADDON"
    ELSE
    COALESCE(INS.product_category,
      items_id)
  END
    AS secondary_product,
    ROW_NUMBER() OVER(PARTITION BY session_id, event_action, main_product, items_id ORDER BY kafka_publish_timestamp) AS rn
  FROM
    addon_sbf
  LEFT JOIN
    INS
  USING
    (product_category_id))
WHERE
  rn = 1
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