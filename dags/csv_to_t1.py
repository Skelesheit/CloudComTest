import csv
from datetime import datetime

from airflow.sdk import DAG, task

from db.models import T1
from db.session import get_session

# A Dag represents a workflow, a collection of tasks
with DAG(dag_id="demo", start_date=datetime(2022, 1, 1), schedule="0 0 * * *") as dag:
    # Tasks are represented as operators
    @task
    def insert_from_csv_to_t1(rows_count=100) -> None:
        models_to_insert = list()
        with open("/opt/airflow/dags/data/data.csv", newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            for i, row in enumerate(reader):
                if i >= rows_count:
                    break
                name, surname, patronymic = row
                models_to_insert.append(
                    T1(
                        name=name,
                        surname=surname,
                        patronymic=patronymic
                    )
                )
        with get_session() as session:
            with session.begin():
                session.bulk_save_objects(models_to_insert)
