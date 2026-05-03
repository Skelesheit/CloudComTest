import json
from datetime import datetime

from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import DAG, task, get_current_context

from db.models import T1
from db.session import get_session

# A Dag represents a workflow, a collection of tasks
with DAG(
        dag_id="2",
        start_date=datetime(2026, 1, 1),
        schedule="@daily"
) as dag:
    @task
    def t1_to_s3() -> None:
        context = get_current_context()
        start = context["data_interval_start"]
        end = context["data_interval_end"]
        rows = list()
        with get_session() as session:
            rows = session.query(T1).filter(
                T1.created >= start,
                T1.created < end
            ).all()
        if len(rows) == 0:
            raise AirflowSkipException()
        # Загрузим в S3
        key_path = f"{start.date().isoformat()}/data.json"
        t1_json = [
            {
                "id": row.id,
                "name": row.name,
                "surname": row.surname,
                "patronymic": row.patronymic,
                "gender": row.gender,
                "created": row.created.isoformat(),
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

    t1_to_s3()
