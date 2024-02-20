-- #1 totals eCommerce action type per bounce value(1 = bounced session, null = non-bounced session)
SELECT totals.bounces, COUNT(*) AS total, eCommerceAction.action_type
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20161101`,
UNNEST(hits)
GROUP BY 
  totals.bounces,
  eCommerceAction.action_type


-- #2 selecting features
SELECT DISTINCT
  CONCAT(fullVisitorId, visitId) AS session_id,
  SUBSTR(date, 7, 2) AS day,
  SUBSTR(date, 5, 2) AS month,
  SUBSTR(date, 1, 4) AS year,
  totals.hits,
  totals.pageviews AS page_views,
  totals.bounces,
  totals.timeOnSite AS time_on_site,
  hour,
  minute,
  device.deviceCategory AS device,
  geoNetwork.subContinent AS sub_continent,
  geoNetwork.country,
  product_category,
  product_name,
  product_price,
  add_to_cart,
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
LEFT JOIN (
  SELECT
    CONCAT(fullVisitorId, visitId) AS session_id,
    MAX(CASE hit.eCommerceAction.action_type = '3' WHEN TRUE THEN 1 ELSE 0 END) AS add_to_cart,
    hit.hour AS hour,
    hit.minute AS minute,
    product.v2ProductName AS product_name,
    product.v2ProductCategory AS product_category,
    CAST(AVG(product.localProductPrice) AS INT64) AS product_price
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    UNNEST(hits) AS hit
    LEFT JOIN UNNEST(hit.product) AS product
  WHERE
    _TABLE_SUFFIX BETWEEN '20161101' AND '20161130'
  GROUP BY
    session_id,
    hit.hour,
    hit.minute,
    product_name,
    product_category
) AS target_table ON CONCAT(fullVisitorId, visitId) = target_table.session_id
WHERE
  (_TABLE_SUFFIX BETWEEN '20161101' AND '20161130')
  AND totals.bounces IS NULL
ORDER BY session_id
