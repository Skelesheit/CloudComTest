import json
from datetime import datetime

from airflow.sdk import DAG, task
from airflow.sdk.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from db.models import T1
from db.session import get_session

# A Dag represents a workflow, a collection of tasks
with DAG(
        dag_id="2",
        start_date=datetime(2026, 1, 1),
        schedule="@daily"
) as dag:
    @task
    def t1_to_s3(data_interval_start=None, data_interval_end=None) -> None:
        rows = list()
        with get_session() as session:
            with session.begin():
                rows = session.query(T1).filter(
                    T1.created >= data_interval_start,
                    T1.created < data_interval_end
                ).all()
        if rows is None:
            raise AirflowSkipException()
        # Загрузим в S3
        key_path = f"{data_interval_start.date().isoformat()}/data.json"
        t1_json = [
            {
                "id": row.id,
                "name": row.name,
                "surname": row.surname,
                "patronymic": row.patronymic,
                "created": row.created,
            }
            for row in rows
        ]
        s3_hook = S3Hook(aws_conn_id="aws_default")
        s3_hook.load_string(
            json.dumps(t1_json, indent=4, ensure_ascii=False),
            key=key_path,
            bucket_name="default-bucket",
            replace=True,
        )
