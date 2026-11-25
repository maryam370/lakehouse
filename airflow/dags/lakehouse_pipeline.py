from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

PACKAGES = ('org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,'
            'org.apache.iceberg:iceberg-aws-bundle:1.11.0,'
            'org.postgresql:postgresql:42.7.4')

def spark_task(task_id, script):
    return KubernetesPodOperator(
        task_id=task_id,
        name=task_id.replace('_', '-'),
        namespace='processing',
        image='apache/spark:3.5.9-java17-python3',
        cmds=['/bin/bash', '-c'],
        arguments=['/opt/spark/bin/spark-submit --master local[1] --driver-memory 768m '
                   '--conf spark.jars.ivy=/tmp/ivy --packages ' + PACKAGES + ' /opt/jobs/' + script],
        env_vars={'PYTHONPATH': '/opt/jobs', 'AWS_REGION': 'us-east-1'},
        env_from=[
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name='s3-credentials')),
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name='platform-db')),
        ],
        volumes=[k8s.V1Volume(name='jobs', config_map=k8s.V1ConfigMapVolumeSource(name='spark-jobs'))],
        volume_mounts=[k8s.V1VolumeMount(name='jobs', mount_path='/opt/jobs')],
        container_resources=k8s.V1ResourceRequirements(limits={'memory': '1280Mi'}),
        in_cluster=True,
        get_logs=True,
        on_finish_action='delete_succeeded_pod',
    )

with DAG(
    dag_id='lakehouse_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule='0 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['lakehouse'],
) as dag:
    silver = spark_task('bronze_to_silver', 'bronze_to_silver.py')
    gold = spark_task('silver_to_gold', 'silver_to_gold.py')
    silver >> gold