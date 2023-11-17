from airflow.operators.python import PythonOperator, get_current_context
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.dagrun import DagRun


def check_upstream_success(ti):
    dag_run: DagRun = get_current_context().get("dag_run")

    # get tasks is the direct relatives. True - for upstream
    upstream_task_ids = {t.task_id for t in ti.task.get_direct_relatives(True)}

    # grab all of the failed task instance in the current run
    # which will get us tasks that some of might be un-related to this one
    failed_task_instances = dag_run.get_task_instances(
        state=["failed", "upstream_failed"]
    )

    failed_upstream_task_ids = [
        failed_task.task_id
        for failed_task in failed_task_instances
        if failed_task.task_id in upstream_task_ids
    ]

    if len(failed_upstream_task_ids) == 0:
        return True

    return False


def create_email_report_task(upstream_task):
    check_upstream_success_task = PythonOperator(
        task_id="check_upstream_success",
        python_callable=check_upstream_success,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    send_email = EmailOperator(
        task_id="send_email_report",
        to="ngocthinh303@gmail.com",
        subject="""
            DAG {{dag_run.dag_id}} run {% if ti.xcom_pull(task_ids="check_upstream_success") == True %} successfully {% else %} failure {% endif %} on {{ ds }}
        """,
        html_content="""
        {% if ti.xcom_pull(task_ids="check_upstream_success") == True %}
            <p>The DAG run successfully</p>
        {% else %}
            <p>The DAG got error</p>
        {% endif %}
        """,
    )
    upstream_task >> check_upstream_success_task >> send_email
