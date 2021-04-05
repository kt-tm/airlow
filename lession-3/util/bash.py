from airflow.operators.bash_operator import BashOperator


def bash(task_id, bash_command, **context):
    return BashOperator(task_id=task_id, bash_command=bash_command, dag=dag)
#
# BashOperator(
#         task_id='first_task',
#         bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
#         dag=dag,
#     )