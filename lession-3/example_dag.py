import task.task_body as t
from airflow.models import DAG
from util.settings import default_settings
from util.dummy import dummy
from util.bash import bash
from util.deco import python_operator



# @python_operator()
# def decorated_python_operator_definition(**context):
#     print('Hey I\'m a decorated operator.')

with DAG(**default_settings()) as dag:
    bash('first_task', 'echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"') >> t.download_titanic_dataset() >> (t.pivot_dataset(), t.mean_fare_per_class()) >> bash('last_task', 'echo "Pipeline finished! Execution date is {{ ds }}"')

        # with DAG(**default_settings()) as dag:
#     decorated_python_operator_definition()
# with DAG(**default_settings()) as dag:
#     dummy('first_task') >> dummy('create_titanic_dataset') >> \
#         (dummy('pivot_titanic_dataset'), dummy('mean_fares_titanic_dataset')) >> dummy('last_task')
