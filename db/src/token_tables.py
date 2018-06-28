#!/usr/bin/evn python3

from sqlalchemy import Column, Integer, String, Table
from sqlalchemy.ext.declarative import declarative_base

STRING_COLLATION = 'SQL_Latin1_General_CP1_CI_AS'

Base = declarative_base()
metadata = MetaData()


class TB1(Table):
    __tablename__ = 'tb1'
    id = Column(Integer, primary_key=True)
    name = Column(String(256))


class RecordTable(Base):
    __tablename__ = 'record'
    id = Column(Integer)
    natural_key = Column(String, primary_key=True)
    family_names = Column(String(256))
    first_name = Column(String(256))
    gender = Column(String(1))
    modified_date = Column()

    def __repr__(self):
        return "<Record(natural_key='%s', family_names='%s', first_name='%s')>" % (
            self.natural_key, self.family_names, self.first_name)


class RecordTokenTable(Base):
    __tablename__ = 'record_token'
    id = Column(Integer)
    natural_key = Column(String(256), primary_key=True)
    code = Column(String(256), primary_key=True)
    token = Column(String(64), unique=True)
    family_names = Column(String(256))


class TokenSpecification(Base):
    """
    Specification of a token record
    """
    __tablename__ = "tokenspecification"

    id = Column(
        Integer,
        primary_key=True
    )
    account_id = Column(
        String(0xFF),
        nullable=False,
        index=True
    )
    code = Column(
        String(0xFF),
        nullable=False,
        doc='Identifier of this token (used in output reports)'
    )
    name = Column(
        String(0xFF),
        nullable=False,
        doc='Display name of this token'
    )
