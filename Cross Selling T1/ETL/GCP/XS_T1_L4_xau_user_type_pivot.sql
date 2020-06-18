#standardSQL
/* 
Creator : royan.aldian@traveloka.com
Destination table: `tvlk-data-user-dev.xs_playground.XS_T1_L4_xau_user_type_pivot
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
    * section
    * user_type
    * vol_user,
Data Processing:
    * ~0.63GB data processed per month
    * ~n rows inserted per month
    * 9.4 sec for elapsed time per query hit (daily)
Est Cost Processing: 
    * Streaming Insert: $0.05/1GB * 0.63GB = $0.031
    * Queries: $0,005/1GB * 0.63GB = $0.0031
*/

-- CREATE OR REPLACE TABLE `tvlk-data-user-dev.xs_playground.XS_T1_L4_xau_user_type_pivot`
-- PARTITION BY (dt) AS

SELECT
  dt,
  period,
  metric,
  country,
  interface,
  event_action,
  main_product,
  secondary_product,
  channel,
  section,
  'All User' AS user_type,
  vol_user AS vol_user
FROM
  `tvlk-data-user-dev.xs_playground.XS_T1_L4_xau_user_type`

UNION ALL

SELECT
  dt,
  period,
  metric,
  country,
  interface,
  event_action,
  main_product,
  secondary_product,
  channel,
  section,
  'New User' AS user_type,
  vol_new_user AS vol_user
FROM
  `tvlk-data-user-dev.xs_playground.XS_T1_L4_xau_user_type`

UNION ALL

SELECT
  dt,
  period,
  metric,
  country,
  interface,
  event_action,
  main_product,
  secondary_product,
  channel,
  section,
  'Retained User' AS user_type,
  vol_retained_user AS vol_user
FROM
  `tvlk-data-user-dev.xs_playground.XS_T1_L4_xau_user_type`