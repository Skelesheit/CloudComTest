from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# чуть хардкода без .env
engine = create_engine(
    url="postgresql+psycopg2://airflow:airflow@postgres/airflow",
    pool_pre_ping=True,
    echo=False,
)

session_factory = sessionmaker(bind=engine)


@contextmanager
def get_session():
    session = session_factory()
    try:
        yield session  # это питоновская сессия, не транзакция
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
