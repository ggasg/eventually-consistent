# A Databricks job cluster, one Spark property at a time

Companion code for [the article of the same name](https://ggasg.github.io/blog/local-job-cluster/):
a Databricks-like job cluster on a laptop. Spark runs on a local Kubernetes cluster
([kind](https://kind.sigs.k8s.io/)), jobs are submitted through the
[Spark Operator](https://github.com/kubeflow/spark-operator), and Delta Lake tables
are stored in [MinIO](https://min.io/) through Hadoop's S3A connector.

Every run gets its own driver pod on a driver node and executor pods on two worker
nodes, which are deleted when the run ends. Executors autoscale between 1 and 4, and
the Spark History Server keeps the Spark UI of every run.

## Layout

```
code/
  infra/
    Dockerfile                  the "runtime": Spark 4.1.3, Delta 4.1.0, S3A, Python 3.12
    kind-config.yml             one control-plane node, one driver node, two worker nodes
    spark-jobs.yml              namespace, driver service account, Spark UI, History Server
    spark-operator-values.yaml  Helm values for the Spark Operator
  jobs/
    generate-touch-events.yaml  writes a synthetic Delta table to MinIO
    dedup-analysis.yaml         deduplicates it with one of three strategies
  app/touch_events/             the job code, mounted into the cluster at /opt/app
tests/
  test_touch_events.py          the table has ~10% redeliveries; dedup strategies behave
  test_job_files.py             job files agree with the cluster on paths, labels, accounts
spark-events/                   event logs of past runs, read by the History Server
```

## Prerequisites

Docker Desktop, kind, kubectl (1.31 or later, for `wait --for=create`), [Helm](https://helm.sh/), and a MinIO server with a
bucket named `spark-experiments`:

```bash
minio server ~/minio-data    # default user and password: minioadmin / minioadmin
```

The S3A endpoint in the job files is `host.docker.internal:9000`, which is how pods
reach the host under Docker Desktop. Without Docker Desktop, replace it with the host's address.

## Create the cluster

Run everything from this folder: `kind-config.yml` mounts `code/app` and
`spark-events` by relative path.

```bash
kind create cluster --name spark-cluster --config code/infra/kind-config.yml
docker build -t spark-with-delta:4.1.3 code/infra
kind load docker-image spark-with-delta:4.1.3 --name spark-cluster
kubectl apply -f code/infra/spark-jobs.yml
helm install spark-operator spark-operator --repo https://kubeflow.github.io/spark-operator \
  --version 2.5.2 -n spark-operator --create-namespace \
  -f code/infra/spark-operator-values.yaml --wait
kubectl config set-context kind-spark-cluster --namespace spark-jobs
```

When you are done: `kind delete cluster --name spark-cluster`. Tables in MinIO and
event logs in `spark-events/` are kept.

## Run a job

```bash
kubectl replace --force -f code/jobs/generate-touch-events.yaml
kubectl wait --for=create pod/generate-touch-events-driver
kubectl logs -f generate-touch-events-driver
```

`replace --force` also reruns a job. Arguments are in the job file (`arguments:`).
Code changes under `code/app` apply to the next run without rebuilding the image.

- Spark UI of the running job: http://localhost:4040
- Spark UI of every run, through the History Server: http://localhost:18080
- Where the pods run: `kubectl get pods -o wide -w`

## Running the tests

```bash
pip install pyspark==4.1.3 pytest pyyaml
python3 -m pytest tests -q
```

The tests run Spark in local mode and need neither the cluster nor MinIO.

## Feedback

If you run this on another platform, or emulate a job cluster feature differently,
open an issue or a PR.
