#!/usr/bin/evn python3
from sqlalchemy import Column, Integer, String, Table, MetaData, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.types import DateTime

STRING_COLLATION = 'SQL_Latin1_General_CP1_CI_AS'

Base = declarative_base()
metadata = MetaData()


class RecordTable(Table):
    __tablename__ = 'record'

    @classmethod
    def create(cls):
        """
        Generate record table for account.
        """
        columns = [
            Column('id', Integer),
            Column('natural_key', String(256), primary_key=True),
            Column('family_names', String(256)),
            Column('first_name', String(256)),
            Column('gender', String(1)),
            Column('modified_date', DateTime),
        ]

        return cls(cls.__tablename__, metadata, *columns)


class RecordTokenTable(Table):
    __tablename__ = 'record_token'

    @classmethod
    def create(cls):
        """
        Generate record table for account.
        """
        columns = [
            Column('id', Integer),
            Column('natural_key', String(256), ForeignKey('record.natural_key'), primary_key=True),
            Column('code', String(256), primary_key=True),
            Column('token', String(64)),
            Column('family_names', String(256))
        ]

        return cls(cls.__tablename__, metadata, *columns)


class TempRecordTokenTable(Table):
    __tablename__ = 'temp_record_token'

    @classmethod
    def create(cls):
        """
        Generate record table for account.
        """
        columns = [
            Column('id', Integer),
            Column('natural_key', String(256), ForeignKey('record.natural_key'), primary_key=True),
            Column('code', String(256), primary_key=True),
            Column('token', String(64)),
            Column('family_names', String(256))
        ]

        return cls(cls.__tablename__, metadata, *columns)


class TokenSpecification(Table):
    """
    Specification of a token record
    """
    __tablename__ = "tokenspecification"

    @classmethod
    def create(cls):
        """
        Generate record table for account.
        """
        columns = [
            Column('id', Integer, primary_key=True),
            Column('account_id', String(256)),
            Column('code', String(256)),
            Column('name', String(256))
        ]

        return cls(cls.__tablename__, metadata, *columns)
