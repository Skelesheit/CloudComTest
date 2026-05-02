from contextlib import contextmanager

from sqlalchemy import create_engine, URL
from sqlalchemy.orm import sessionmaker, Session

# чуть хардкода (пока не .env) - временно
url_object = URL.create(
    "postgresql+psycopg",
    username="airflow",
    password="airflow",
    host="postgres",
    database="airflow",
    port=5432,
)
engine = create_engine(url=url_object)

session_factory = sessionmaker(bind=engine)


def get_session() -> Session:
    return session_factory()


@contextmanager
def get_session():
    session = session_factory()
    try:
        yield session # питоновская сессия
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

@contextmanager
def get_transaction():
    session = session_factory()
    try:
        with session.begin():
            yield session # БД транзакция
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

