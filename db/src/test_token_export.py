#!/usr/bin/env python3
import os
import csv
import time
import arrow
import pyodbc
from typing import Generator, Any, Sequence, Tuple

from sqlalchemy import Column, Integer, String, Table, and_, literal_column, union
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select

from tables import RecordTable, RecordTokenTable, TokenSpecification, TempRecordTokenTable

STRING_COLLATION = 'SQL_Latin1_General_CP1_CI_AS'

# an Engine, which the Session will use for connection
# resources
pg_engine = create_engine('postgresql://wangj@localhost/db1')

# create a configured "Session" class
Session = sessionmaker(bind=pg_engine)

# create a Session
session = Session()


def get_engine(database_name='db1'):
    # pg_engine = create_engine(os.path.join('postgresql://wangj@localhost', database_name))
    engine = create_engine('mssql+pyodbc://sa:yourStrong(!)Password@db')
    return engine


pg_engine = get_engine()

Base = declarative_base()


class DatabankTSV(csv.Dialect):
    """
    Databank Tab Separated Value file dialect.
    """
    delimiter = '\t'
    quotechar = '"'
    doublequote = True
    skipinitialspace = False
    lineterminator = '\r\n'
    quoting = csv.QUOTE_MINIMAL


class TokenExportor:
    def __init__(self, connection,
                 token_codes,
                 record_table,
                 token_table,
                 modified_date=None):
        self.record_table = record_table
        self.token_table = token_table
        self.token_codes = token_codes
        self.connection = connection
        self.modified_date = modified_date
        self.row_count = 0

    def create_query(self):
        """
        Create a SQL query is as following:

        SELECT record.natural_key, record_token.token, record_token.code
        FROM record JOIN record_token ON record.natural_key = record_token.natural_key
        ORDER BY record.natural_key, record_token.code;
        """

        j = self.record_table.join(self.token_table,
                                   and_(self.record_table.c.natural_key == self.token_table.c.natural_key))

        print(j)

        if self.modified_date:
            query = select([self.record_table.c.natural_key,
                            self.token_table.c.token,
                            self.token_table.c.code]).\
                select_from(j).\
                where(self.record_table.c.modified_date == self.modified_date).\
                order_by(self.record_table.c.natural_key, self.token_table.c.code)
        else:
            query = select([self.record_table.c.natural_key,
                            self.token_table.c.token,
                            self.token_table.c.code]).\
                select_from(j).\
                order_by(self.record_table.c.natural_key, self.token_table.c.code)

        return query

    def header(self) -> Sequence[str]:
        """
        Return a list contains all field name as following:
        ["natural_key","token0","token1"...,"tokenn"]
        """
        # hd = ["natural_key"]

        hd = [self.record_table.c.natural_key.name]

        # print(help(self.record_table.c.natural_key))

        [hd.append('token{}'.format(n)) for n in range(len(self.token_codes))]

        return hd

    def __iter__(self) ->Generator[list, any, None]:
        """
        Each return
        """
        token_record = []
        token_code_dict = {}
        pre_nk = None

        query = self.create_query()
        print(query)
        result = self.connection.execution_options(stream_results=True).execute(query)
        for tup in result:
            if not pre_nk:
                pre_nk = tup.natural_key
                token_code_dict = {}
                token_record.append(tup.natural_key)

            if pre_nk != tup.natural_key:
                for code in self.token_codes:
                    token_record.append(token_code_dict.get(code, ''))

                yield token_record
                pre_nk = tup.natural_key
                token_record = []
                token_code_dict = {tup.code: tup.token}
                token_record.append(tup.natural_key)
            else:
                token_code_dict[tup.code] = tup.token

        if token_record:
            for code in self.token_codes:
                token_record.append(token_code_dict.get(code, ''))

            yield token_record


def testTokenExportor():
    record = RecordTable.create()
    token = RecordTokenTable.create()
    token_codes = ['type-1', 'type-2', 'type-3']
    modified_date = arrow.utcnow()

    with pg_engine.connect() as conn:
        with open("token_export.csv", 'w') as result_file:
            writer = csv.writer(result_file, dialect=DatabankTSV)

            token_export = TokenExportor(conn, token_codes, record, token, modified_date.datetime)
            # token_export = TokenExportor(conn, token_codes, record, token)
            writer.writerow(token_export.header())
            [writer.writerow(x) for x in token_export]

            # print(token_export.header())

            # [print(x) for x in token_export]


def main():
    testTokenExportor()


if __name__ == '__main__':
    main()
