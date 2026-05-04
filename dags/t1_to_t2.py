from datetime import datetime

from airflow.sdk import DAG, task
from sqlalchemy import text as sa_text

from db.models import T1, T2
from db.session import get_session

# A Dag represents a workflow, a collection of tasks
with DAG(
        dag_id="2",
        start_date=datetime(2022, 1, 1),
        schedule="@daily"
) as dag:
    # Tasks are represented as operators
    @task
    def insert_from_t1_to2() -> None:
        with get_session() as session:
            with session.begin():
                session.execute(sa_text("TRUNCATE TABLE t2"))
                rows_t1: list[T1] = session.query(T1).all()
                session.bulk_save_objects(
                    [
                        T2(
                            id=row.id,
                            name=row.name,
                            surname=row.surname,
                            patronymic=row.patronymic,
                            created=row.created,
                            gender=row.gender,
                        ) for row in rows_t1
                    ]
                )


    insert_from_t1_to2()
