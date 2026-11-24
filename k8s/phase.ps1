# Usage: .\k8s\phase.ps1 ingest | transform | query
param([Parameter(Mandatory)][ValidateSet('ingest','transform','query')]$Phase)

# namespace/deployment pairs
$all = @(
  @('source','shop-db'), @('ingestion','kafka'), @('ingestion','debezium'),
  @('processing','streaming-bronze'), @('lakehouse','platform-db'),
  @('lakehouse','seaweedfs'), @('serving','grafana')
)
$on = switch ($Phase) {
  'ingest'    { 'shop-db','kafka','debezium','streaming-bronze','platform-db','seaweedfs' }
  'transform' { 'platform-db','seaweedfs' }
  'query'     { 'platform-db','seaweedfs','grafana' }
}
foreach ($p in $all) {
  $n = if ($on -contains $p[1]) { 1 } else { 0 }
  kubectl scale deploy/$($p[1]) -n $p[0] --replicas=$n
}

# Trino (helm release) and Airflow: only in 'query' / never during ingest
$trino = kubectl get deploy -A -o jsonpath='{range .items[*]}{.metadata.namespace}/{.metadata.name}{"\n"}{end}' | Select-String -Pattern 'trino'
foreach ($t in $trino) {
  $ns,$name = $t.ToString().Split('/')
  $n = if ($Phase -eq 'query') { 1 } else { 0 }
  kubectl scale deploy/$name -n $ns --replicas=$n
}
Write-Host "Phase '$Phase' applied. Check: kubectl top nodes"
