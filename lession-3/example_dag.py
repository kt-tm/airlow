from airflow.models import DAG
from util.settings import default_settings
from util.bash import bash
import util.task_body as t

with DAG(**default_settings()) as dag:
    first_task = bash('first_task', 'echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"')
    last_task = bash('last_task', 'echo "Pipeline finished! Execution date is {{ ds }}"')

    (
            first_task >>
            t.download_titanic_dataset() >>
            (t.pivot_dataset(), t.mean_fare_per_class()) >>
            last_task
    )