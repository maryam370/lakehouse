function Run-Spark($name, $script) {
    kubectl delete job $name -n processing --ignore-not-found

    $yaml = Get-Content .\k8s\31-spark-job-template.yaml -Raw
    $yaml = $yaml.Replace('__NAME__', $name)
    $yaml = $yaml.Replace('__SCRIPT__', $script)

    $yaml | kubectl apply -f -
}