-- What the Beam pipeline writes: one row per one-minute window per sensor
-- type, holding the distinct device IDs that breached the alarm threshold in
-- that window. The identities are still here, which is what lets the
-- materialized view below build a sketch in a single pass.
CREATE TABLE IF NOT EXISTS `iot_analytics.raw_anomaly_device_ids` (
  window_timestamp TIMESTAMP
    OPTIONS(description = "Start boundary of the 1-minute streaming pane"),
  sensor_type STRING
    OPTIONS(description = "Sensor metric type: temperature or pressure"),
  device_ids ARRAY<STRING>
    OPTIONS(description = "Distinct device IDs seen for this sensor_type within the pane")
)
PARTITION BY DATE(window_timestamp)
CLUSTER BY sensor_type;
