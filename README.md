# Lakehouse on minikube

An open-source lakehouse that runs on a small laptop (minikube, about 6 GB RAM).

```
PostgreSQL (shop) -> Debezium (CDC) -> Kafka -> Spark Streaming -> Iceberg Bronze
   -> Spark batch -> Silver (MERGE + quality rules, ops.rejects) -> Gold marts
   -> Trino -> Grafana
Airflow (optional) runs Silver and Gold hourly. Storage: SeaweedFS (S3 API).
Iceberg catalog: JDBC catalog in the platform PostgreSQL (catalog name "lh").
```

| Namespace | What runs there |
|---|---|
| `source` | `shop-db` (the demo source PostgreSQL) |
| `ingestion` | `kafka`, `debezium` |
| `processing` | `streaming-bronze` (always on), batch Spark jobs |
| `lakehouse` | `platform-db` (Iceberg catalog + Airflow metadata), `seaweedfs` |
| `orchestration` | Airflow (Helm) |
| `serving` | Trino (Helm), Grafana |

All commands below are PowerShell, run from the repository root.

## Why "phases"?

Everything at once needs more than 6 GB and crashes minikube. So the heavy parts
are switched on and off by phase. `k8s\phase.ps1` scales deployments up or down:

| Phase | Running | Switched off |
|---|---|---|
| `ingest` | shop-db, Kafka, Debezium, streaming Spark, platform-db, SeaweedFS | Trino, Grafana |
| `transform` | platform-db, SeaweedFS (+ one Spark batch job at a time) | everything else |
| `query` | platform-db, SeaweedFS, Trino, Grafana | Kafka, Debezium, streaming, shop-db |

Airflow is not part of the phase script. Leave it off unless you need it (see the end).

## 0. One-time setup

Start minikube with a memory cap (more than this will starve Windows on a 10 GB PC):

```powershell
minikube start --memory=6144 --cpus=4
minikube addons enable metrics-server
```

Create the namespaces and the two secrets the workloads read. The values below are
**throwaway lab values**. Change them for anything real (a safer workflow is planned
in a later phase).

```powershell
kubectl apply -f .\k8s\00-namespaces.yaml

# S3 credentials used by SeaweedFS, Spark and Trino
foreach ($ns in 'lakehouse','processing','serving') {
  kubectl create secret generic s3-credentials -n $ns `
    --from-literal=AWS_ACCESS_KEY_ID=<access-key> `
    --from-literal=AWS_SECRET_ACCESS_KEY=<secret-key>
}

# Platform PostgreSQL credentials (Iceberg catalog)
foreach ($ns in 'lakehouse','processing','serving') {
  kubectl create secret generic platform-db -n $ns `
    --from-literal=PG_USER=<user> `
    --from-literal=PG_PASSWORD=<password>
}
```

If you already have these secrets in your cluster, skip this step.

Deploy the base services and the Spark job code:

```powershell
kubectl apply -f .\k8s\10-source-postgres.yaml
kubectl apply -f .\k8s\11-platform-postgres.yaml
kubectl apply -f .\k8s\12-seaweedfs.yaml
kubectl apply -f .\k8s\20-kafka.yaml
kubectl apply -f .\k8s\21-debezium.yaml

# Spark jobs are mounted from a ConfigMap. Re-run this whenever you edit spark\*.py
kubectl create configmap spark-jobs -n processing --from-file=.\spark --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f .\k8s\30-spark-streaming.yaml
```

Wait until everything is ready:

```powershell
kubectl get pods -A
```

Register the Debezium connector once Debezium is `Ready`:

```powershell
kubectl port-forward -n ingestion svc/debezium 8083:8083   # leave this running in another window
curl.exe -X POST -H "Content-Type: application/json" --data "@k8s/debezium-shop.json" http://localhost:8083/connectors
```

## Phase 1: ingest (add rows)

```powershell
.\k8s\phase.ps1 ingest

# Terminal 2: expose the source database for the generator
kubectl port-forward -n source svc/shop-db 5433:5432

# Terminal 3 (activate your virtualenv first, needs psycopg2: pip install psycopg2-binary)
python data-generator\generate.py seed          # first time only
python data-generator\generate.py stream 120    # 120 seconds of changes
```

Add rows in small rounds. Between rounds, check the load:

```powershell
kubectl top nodes
kubectl logs deploy/streaming-bronze -n processing --tail=20
```

Wait until streaming has caught up (no new batches, or `numInputRows: 0`) before
moving on. If node memory is above about 80%, stop adding rows.

## Phase 2: transform (Bronze to Silver to Gold)

```powershell
.\k8s\phase.ps1 transform
. .\k8s\run-spark.ps1
Run-Spark "bronze-to-silver" "bronze_to_silver.py"
kubectl wait --for=condition=complete job/bronze-to-silver -n processing --timeout=15m
Run-Spark "silver-to-gold" "silver_to_gold.py"
kubectl wait --for=condition=complete job/silver-to-gold -n processing --timeout=15m
```

Row counts for every layer (a quick health check):

```powershell
Run-Spark "check" "check.py"
kubectl wait --for=condition=complete job/check -n processing --timeout=10m
kubectl logs job/check -n processing | Select-String RESULT
```

If a job fails, read why: `kubectl logs job/bronze-to-silver -n processing`.

## Phase 3: query

```powershell
.\k8s\phase.ps1 query
kubectl port-forward -n serving svc/grafana 3000:3000
```

Open http://localhost:3000. Trino is installed by Helm (values in `helm_values\trino.yaml`):

```powershell
helm repo add trino https://trinodb.github.io/charts
helm upgrade --install trino trino/trino --version 1.45.0 -n serving -f helm_values\trino.yaml
```

## Optional: Airflow

Airflow runs the same two Spark jobs hourly, but its own pods need roughly 1.5 to 2 GB.
Use it only in the `transform` phase, with everything else scaled down.

```powershell
kubectl apply -f .\k8s\40-airflow-rbac.yaml
helm repo add apache-airflow https://airflow.apache.org
helm upgrade --install airflow apache-airflow/airflow --version 1.21.0 -n orchestration -f helm_values\airflow.yaml
```

Airflow reads DAGs from GitHub (git-sync, branch `master`), so push DAG changes first.
Scale it back to 0 when done:

```powershell
kubectl scale deploy,statefulset --all -n orchestration --replicas=0
```

## Troubleshooting

| Symptom | Check |
|---|---|
| Pod `OOMKilled` | `kubectl describe pod <name> -n <ns>`. Raise that pod's memory limit a little. |
| Spark job fails | `kubectl logs job/<name> -n processing` |
| Streaming does not progress | `kubectl logs deploy/streaming-bronze -n processing --tail=50` |
| Node memory high | `kubectl top pods -A`, then scale something down with `phase.ps1` |

## Pinned versions (verified on Docker Hub, 2026-11-25)

SeaweedFS `4.55`, Grafana `13.2.7`, aws-cli `2.42.3`. Kafka (`apache/kafka:4.2.0`),
Debezium (`3.5`) and Spark (`3.5.9-java17`) were already pinned. PostgreSQL is pinned to
major version `16`.

Spark must use a Java 17 image (Iceberg 1.11 needs it), and the Spark catalog name and
the Trino `iceberg.jdbc-catalog.catalog-name` must both be `lh`.
