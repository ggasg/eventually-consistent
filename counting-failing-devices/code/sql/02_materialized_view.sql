-- HLL_COUNT.INIT is on BigQuery's list of aggregations an incremental
-- materialized view may use. HLL_COUNT.MERGE and HLL_COUNT.MERGE_PARTIAL are
-- not, which is why the pipeline ships arrays of device IDs and lets the view
-- create the sketches rather than creating them upstream in Beam.
-- https://cloud.google.com/bigquery/docs/materialized-views-create
CREATE MATERIALIZED VIEW IF NOT EXISTS `iot_analytics.hourly_anomaly_rollup` AS
SELECT
  TIMESTAMP_TRUNC(window_timestamp, HOUR) AS hourly_bucket,
  sensor_type,
  HLL_COUNT.INIT(device_id) AS intermediate_sketch
FROM `iot_analytics.raw_anomaly_device_ids`, UNNEST(device_ids) AS device_id
GROUP BY 1, 2;
