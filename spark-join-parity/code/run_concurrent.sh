#!/usr/bin/env bash
# Runs both benchmarks CONCURRENTLY, in the same wall-clock window,
# instead of one after another in two separate sessions.
#
# results/session_2 and session_3 showed that running PySpark's whole
# sample, then Scala's whole sample minutes later (or in an entirely
# separate invocation), lets ambient load on shared/sandboxed hardware
# drift between the two samples. That drift was bigger than the effect
# being measured and flipped its sign across sessions. Running both
# engines at the same time means both see the same ambient conditions
# at the same moment, so a paired-by-timestamp analysis (see
# parse_events.py's pair_by_time / paired_t_test) can difference the
# shared noise out instead of being confounded by it.
#
# Each engine gets local[2] instead of local[4] so the two together
# don't oversubscribe a 4-core machine.
set -euo pipefail

export SPARK_HOME="$(python3 -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')"
export SPARK_LOCAL_HOSTNAME=127.0.0.1
export SPARK_LOCAL_IP=127.0.0.1

DATA_DIR="${1:-/tmp/bench_concurrent}"
mkdir -p "$DATA_DIR/events/pyspark" "$DATA_DIR/events/scala" "$DATA_DIR/results"

echo "== launching both engines concurrently, local[2] each =="

BENCH_MASTER="local[2]" \
BENCH_EVENT_LOG_DIR="file://$DATA_DIR/events/pyspark" \
BENCH_APP_NAME="pyspark-concurrent" \
BENCH_DRIVER_MEMORY="1g" \
python3 code/join_bench.py > "$DATA_DIR/pyspark.log" 2>&1 &
PYSPARK_PID=$!

"$SPARK_HOME/bin/spark-shell" \
  --master "local[2]" \
  --conf spark.driver.host=127.0.0.1 \
  --conf spark.driver.bindAddress=127.0.0.1 \
  --conf spark.driver.memory=1g \
  --conf spark.ui.enabled=false \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.sql.autoBroadcastJoinThreshold=-1 \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir="file://$DATA_DIR/events/scala" \
  -i code/JoinBench.scala > "$DATA_DIR/scala.log" 2>&1 &
SCALA_PID=$!

wait "$PYSPARK_PID" "$SCALA_PID"
echo "== both finished =="
