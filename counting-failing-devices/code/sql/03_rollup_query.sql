-- Read time. Merging is associative and commutative, so any range that is a
-- whole number of hours can be answered from the stored sketches without
-- touching the landing table, and a device in alarm during several hours is
-- still counted once.
SELECT
  sensor_type,
  HLL_COUNT.MERGE(intermediate_sketch) AS distinct_failing_devices
FROM `iot_analytics.hourly_anomaly_rollup`
WHERE hourly_bucket >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY sensor_type;
