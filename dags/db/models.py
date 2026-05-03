from sqlalchemy import (
    Column,
    String,
    TIMESTAMP,
    func,
    BigInteger
)

from .base import Base


class FIOMixin:
    name = Column(String(255))
    surname = Column(String(700))
    patronymic = Column(String(255))
    gender = Column(String(1))


class T1(Base, FIOMixin):
    __tablename__ = 't1'
    id = Column(BigInteger, primary_key=True)
    created = Column(TIMESTAMP, server_default=func.now())


class T2(Base, FIOMixin):
    __tablename__ = 't2'
    id = Column(BigInteger, primary_key=True)
    created = Column(TIMESTAMP)
    load_time = Column(TIMESTAMP, server_default=func.now())
