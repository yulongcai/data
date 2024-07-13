from __future__ import annotations

from datetime import timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

from job.process import process
from job.publish import publish

with DAG(
        dag_id='practice2',
        default_args={
            "depends_on_past": False,
            "email": ["382366956@qq.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "execution_timeout": timedelta(hours=1)
        },
) as dag:

    sql = """
          SELECT
            YEAR(date) AS year,
            model,
            SUM(failure) AS total_drive_failures
          FROM
            data
          GROUP BY
            YEAR(date), model
          ORDER BY
            year, model
        """

    loadData = PythonOperator(
        task_id='loadData',
        python_callable=loadDataTest,
        execution_timeout=timedelta(minutes=30)
    )

    process = PythonOperator(
        task_id='process',
        python_callable=process,
        op_kwargs={
            'sql': sql,
            'output': "/Users/yulong.cai/data_develop/airflow/process_result/practice2/"
        }
    )

    publish = PythonOperator(
        task_id='publish',
        python_callable=publish,
        op_kwargs={
            'input_file': "/Users/yulong.cai/data_develop/airflow/process_result/practice2/",
            'output_file': "/Users/yulong.cai/data_develop/airflow/publish_result/practice2.xlsx"
        }
    )

    loadData >> process >> publish
