from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.edgemodifier import Label

from args import default_args

with DAG(
    dag_id="trigger_rules",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:
    run_this_first = BashOperator(
        task_id="run_this_first", bash_command="echo run this first"
    )

    branching = BashOperator(
        task_id="branching", bash_command="echo branching"          # Success branch
        # task_id="branching", bash_command="ecaho branching"       # Failed branch
    )

    branch_a = BashOperator(
        task_id="branch_a", bash_command="echo branch_a"
    )

    follow_branch_a = BashOperator(
        task_id="follow_branch_a", bash_command="echo follow_branch_a"
    )

    branch_false = BashOperator(
        task_id="branch_false", bash_command="echo branch_false", trigger_rule="all_failed"
    )

    join = BashOperator(
        task_id="join", bash_command="echo join", trigger_rule="one_success"
    )

    run_this_first >> branching
    branching >> Label("Success") >> branch_a >> follow_branch_a >> join
    branching >> Label("Get Error") >> branch_false >> join
