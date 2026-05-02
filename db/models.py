from sqlalchemy import (
    Column,
    Integer,
    String,
    TIMESTAMP,
    func,
    select,
    between
)
from datetime import date, timedelta

from .base import Base


class FIOMixin(Base):
    name = Column(String(255))
    surname = Column(String(700))
    patronymic = Column(String(255))


class T1(Base, FIOMixin):
    __tablename__ = 't1'
    id = Column(Integer, primary_key=True)
    created = Column(TIMESTAMP, server_default=func.now())


class T2(Base, FIOMixin):
    __tablename__ = 't2'
    id = Column(Integer, primary_key=True)
    created = Column(TIMESTAMP)
    load_time = Column(TIMESTAMP, server_default=func.now())
