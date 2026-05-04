import json
from datetime import datetime, timedelta

from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import DAG, task, get_current_context
from airflow.utils.log.logging_mixin import LoggingMixin

from db.models import T1
from db.session import get_session

# A Dag represents a workflow, a collection of tasks
with DAG(
        dag_id="3",
        start_date=datetime(2026, 1, 1),
        schedule="@daily"
) as dag:

    @task
    def t1_to_s3() -> None:
        context = get_current_context()
        start = context["data_interval_start"]
        end = context["data_interval_end"]
        rows = list()
        t1_json = list()
        log = LoggingMixin().log
        log.info("START = %s", start)
        log.info("END = %s", end)
        if start == end: # костыль при одном и том же времени
            end = start + timedelta(days=1)
        with get_session() as session:
            with session.begin():
                rows = session.query(T1).filter(
                    T1.created >= start,
                    T1.created < end
                ).all()
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
        log.info("rows = %s", t1_json)
        if len(t1_json) == 0:
            raise AirflowSkipException()
        # Загрузим в S3
        key_path = f"{start.date().isoformat()}/data.json"
        s3_hook = S3Hook(aws_conn_id="aws_default")
        s3_hook.load_string(
            json.dumps(t1_json, indent=4, ensure_ascii=False),
            key=key_path,
            bucket_name="default-bucket",
            replace=True,
        )

    t1_to_s3()
