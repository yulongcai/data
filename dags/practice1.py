from __future__ import annotations

from datetime import timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

from job.process import process
from job.publish import publish
from job.downLoadData import loadData

with DAG(
        dag_id='practice1',
        default_args={
            "depends_on_past": False,
            "email": ["382366956@qq.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        }
) as dag:
    loadData = PythonOperator(
        task_id='loadData',
        python_callable=loadData
    )
    sql = """
            SELECT
              date,
              SUM(failure) AS total_drive_failures,
              COUNT(DISTINCT serial_number) AS unique_drive_count
            FROM
              data
            GROUP BY
              date
            ORDER BY
              date;
            """

    process = PythonOperator(
        task_id='process',
        python_callable=process,
        op_kwargs={
            'sql': sql,
            'output': "/Users/yulong.cai/data_develop/airflow/process_result/practice1/"
        }
    )

    publish = PythonOperator(
        task_id='publish',
        python_callable=publish,
        op_kwargs={
            'input_file': "/Users/yulong.cai/data_develop/airflow/process_result/practice1/",
            'output_file': "/Users/yulong.cai/data_develop/airflow/publish_result/practice1.xlsx"
        }
    )

    loadData >> process >> publish
