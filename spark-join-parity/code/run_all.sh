#!/usr/bin/env bash
# Reproduces the benchmark end to end. Requires Java 11+, Python 3.9+, and
# `pip install pyspark==3.5.1` (this pulls in spark-shell / spark-submit too,
# no separate Spark download needed).
set -euo pipefail

export SPARK_HOME="$(python3 -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')"
export SPARK_LOCAL_HOSTNAME=127.0.0.1
export SPARK_LOCAL_IP=127.0.0.1

DATA_DIR="${1:-/tmp/bench}"
mkdir -p "$DATA_DIR/data" "$DATA_DIR/events/pyspark" "$DATA_DIR/events/scala" "$DATA_DIR/results"

echo "== 1/4 generating data =="
python3 code/gen_data.py

echo "== 2/4 running PySpark DataFrame join (50 runs + 20 warmup) =="
python3 code/join_bench.py

echo "== 3/4 running Scala Dataset joinWith (50 runs + 20 warmup) =="
"$SPARK_HOME/bin/spark-shell" \
  --master "local[4]" \
  --conf spark.driver.host=127.0.0.1 \
  --conf spark.driver.bindAddress=127.0.0.1 \
  --conf spark.driver.memory=2g \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.sql.autoBroadcastJoinThreshold=-1 \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file://$DATA_DIR/events/scala \
  -i code/JoinBench.scala

echo "== 4/4 parsing event logs and writing results/summary.json =="
python3 code/parse_events.py
