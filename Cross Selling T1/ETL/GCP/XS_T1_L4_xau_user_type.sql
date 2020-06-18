#standardSQL
/* 
Creator : royan.aldian@traveloka.com
Destination table: `tvlk-data-user-dev.xs_playground.XS_T1_L4_xau_user_type
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
    * vol_user,
    * vol_new_user,
    * vol_retained_user (NA),
    * vol_active_user (NA),
    * vol_churned_user (NA),
    * vol_resurrected_user (NA),
Data Processing:
    * ~0.63GB data processed per month
    * ~n rows inserted per month
    * 9.4 sec for elapsed time per query hit (daily)
Est Cost Processing: 
    * Streaming Insert: $0.05/1GB * 0.63GB = $0.031
    * Queries: $0,005/1GB * 0.63GB = $0.0031
*/

-- CREATE OR REPLACE TABLE `tvlk-data-user-dev.xs_playground.XS_T1_L4_xau_user_type`
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
  vol_user,
  vol_new_user,
  vol_user - vol_new_user AS vol_retained_user
FROM
  `tvlk-data-user-dev.xs_playground.XS_T1_L4_xau`
INNER JOIN (
  SELECT
    *
  FROM
    `tvlk-data-user-dev.xs_playground.XS_T1_L4_new_user`)
USING
  (dt,
    period,
    metric,
    country,
    interface,
    event_action,
    main_product,
    secondary_product,
    channel,
    section)