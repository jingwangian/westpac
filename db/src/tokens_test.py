#!/usr/bin/env python3
import os
import csv
import time
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
                 token_table):
        self.record_table = record_table
        self.token_table = token_table
        self.token_codes = token_codes
        self.connection = connection
        self.row_count = 0

    def create_query(self):
        j = self.record_table.join(self.token_table,
                                   and_(self.record_table.c.natural_key == self.token_table.c.natural_key))

        query = select([self.record_table.c.natural_key,
                        self.token_table.c.token,
                        self.token_table.c.code]).\
            select_from(j).order_by(self.record_table.c.natural_key, self.token_table.c.code)

        return query

    def header(self) -> Sequence[str]:
        """
        Return a list contains all field name as following:
        ["natural_key","token0","token1"...,"tokenn"]
        """
        hd = ["natural_key"]

        [hd.append('token{}'.format(n)) for n in range(len(self.token_codes))]

        return hd

    def __iter__(self):
        """
        Each return 
        """
        query = self.create_query()

        token_code_dict = {}
        pre_nk = None
        token_record = []

        result = self.connection.execution_options(stream_results=True).execute(query)
        for tup in result:
            if not pre_nk:
                pre_nk = tup.natural_key
                token_code_dict = {}

            if pre_nk == tup.natural_key:
                token_code_dict[tup.code] = tup.token
            else:
                token_record.append(pre_nk)
                for code in self.token_codes:
                    token_record.append(token_code_dict.get(code, ''))

                yield token_record
                pre_nk = tup.natural_key
                token_record = []
                token_code_dict = {}
                token_code_dict[tup.code] = tup.token

        if token_record:
            yield token_record


class TokenRecords:
    def __init__(self):
        self.token_table = RecordTokenTable
        self.record_table = RecordTable
        self.row_count = 0

        Session = sessionmaker(bind=pg_engine)

    # create a Session
        self.session = Session()

        self.code_list = ['type-1', 'type-2', 'type-3']

    def __iter__(self):
        for code in self.code_list:
            stmt = self.session.query(self.token_table.natural_key).filter(self.token_table.code == code)
            records = self.session.query(self.record_table.natural_key).filter(
                ~self.record_table.natural_key.in_(stmt))
            for record in records:
                print("self.row_count = {}".format(self.row_count))
                self.row_count += 1
                yield (record.natural_key, code)


def create_tables():
    # create a configured "Session" class
    Session = sessionmaker(bind=pg_engine)

    # create a Session
    session = Session()
    record = RecordTable()

    Base.metadata.create_all(pg_engine)


def query_table():
    Session = sessionmaker(bind=pg_engine)

    # create a Session
    session = Session()
    records = session.query(RecordTable.natural_key)

    for record in records:
        print(record)


def test_RecordTable(query_number, max_count):
    num = 1

    Session = sessionmaker(bind=pg_engine)

    # create a Session
    session = Session()

    t1 = time.time()
    records = session.query(RecordTable).filter(RecordTable.id <= query_number).all()
    print("query {} records from table record takes {} seconds".format(query_number, int(time.time() - t1)))

    t2 = time.time()
    with open('record_out.csv', 'w') as wf:
        writer = csv.writer(wf, delimiter='\t')
        for record in records:
            writer.writerow([record.id, record.natural_key, record.family_names, record.first_name, record.gender])
            num += 1
            if num % 1000 == 0:
                print("Fetched rows : {}".format(num))
            if num > max_count:
                break

    print("Write {} rows into file takes {} seconds".format(num - 1, int(time.time() - t2)))


def test_stream_RecordTable(n):
    """Load Core result rows using fetchmany/streaming."""

    num = 1
    with pg_engine.connect() as conn:
        # result = conn.execution_options(stream_results=True).\
        #     execute(RecordTable.__table__.select().limit(n))

        query_str = "select top {} * from record;".format(n)
        result = conn.execution_options(stream_results=True).\
            execute(query_str)

        with open('record_out.csv', 'w') as wf:
            writer = csv.writer(wf, delimiter='\t')
            while True:
                chunk = result.fetchmany(10000)
                if not chunk:
                    break
                for record in chunk:
                    # data = row['id'], row['natural_key'], row['family_names']
                    writer.writerow([record.id, record.natural_key, record.family_names,
                                     record.first_name, record.gender])
                    num += 1
                    if num % 1000 == 0:
                        print("Fetched rows : {}".format(num))


def test_untoken_records_sql(n):
    """Load Core result rows using fetchmany/streaming."""
    num = 1
    with pg_engine.connect() as conn:
        # query_str = "select top {} * from record;".format(n)

        #         query_str = """SELECT top {} R.natural_key, R.gender, R.family_names AS fname, T0.code as code1, T1.code as code2, T2.code as code3 FROM record R
        # LEFT OUTER JOIN record_token T0 ON R.natural_key = T0.natural_key AND T0.code = 'type-3'
        # LEFT OUTER JOIN record_token T1 ON R.natural_key = T1.natural_key AND T1.code = 'type-2'
        # LEFT OUTER JOIN record_token T2 ON R.natural_key = T2.natural_key AND T2.code = 'type-1'; """.format(n)

        query_str = """with T3 AS(SELECT R.natural_key, R.gender, R.family_names AS fname, T0.code as code FROM record R
LEFT OUTER JOIN record_token T0 ON R.natural_key = T0.natural_key AND T0.code = 'type-3'),
T2 AS (SELECT R.natural_key, R.gender, R.family_names AS fname, T0.code as code FROM record R
LEFT OUTER JOIN record_token T0 ON R.natural_key = T0.natural_key AND T0.code = 'type-2'),
T1 AS (SELECT R.natural_key, R.gender, R.family_names AS fname, T0.code as code FROM record R
LEFT OUTER JOIN record_token T0 ON R.natural_key = T0.natural_key AND T0.code = 'type-1')
SELECT natural_key,'type-3' as code , fname from T3 where code is null
UNION
SELECT natural_key,'type-2' as code, fname from T2 where code is null
UNION
SELECT natural_key,'type-1' as code, fname from T1 where code is null;"""

        result = conn.execution_options(stream_results=True).\
            execute(query_str)

        with open('record_out.csv', 'w') as wf:
            writer = csv.writer(wf, delimiter='\t')
            while True:
                chunk = result.fetchmany(10000)
                if not chunk:
                    break
                for record in chunk:
                    # data = row['id'], row['natural_key'], row['family_names']
                    writer.writerow([record.natural_key, record.code, record.fname])
                    num += 1
                    if num % 1000 == 0:
                        print("Fetched rows : {}".format(num))


def test_untoken_records(n):
    """Load Core result rows using fetchmany/streaming."""
    num = 0

    record = RecordTable.create()
    token = RecordTokenTable.create()

    # s = select([record])

    str_code = 'type-3'

    j = record.outerjoin(token, and_(record.c.natural_key == token.c.natural_key, token.c.code == str_code))

    sub1 = select([record.c.id, record.c.natural_key, token.c.code,
                   literal_column("'{}'".format(str_code)).label("tc")]).\
        select_from(j).where(token.c.code == None)

    j2 = record.outerjoin(token, and_(record.c.natural_key == token.c.natural_key, token.c.code == 'type-2'))

    sub2 = select([record.c.id, record.c.natural_key, token.c.code,
                   literal_column("'type-2'").label("tc")]).\
        select_from(j2).where(token.c.code == None)

    sub3 = select([record.c.id, record.c.natural_key, token.c.code, literal_column("'type-1'").label("tc")]).select_from(
        record.outerjoin(token, and_(record.c.natural_key == token.c.natural_key, token.c.code == 'type-1'))).where(token.c.code == None)

    sub_list = []

    sub_list.append(sub1)
    sub_list.append(sub2)
    sub_list.append(sub3)

    print("=======sub1=======")
    print(str(sub1))

    # s = select([sub1]).where(sub1.c.code == None)
    s = union(*sub_list)

    print("\n=======s=======")
    print(str(s))

    # return

    with pg_engine.connect() as conn:
        result = conn.execution_options(stream_results=True).execute(s)
        # print(help(result))
        # return

        with open('record_out_1.csv', 'w') as wf:
            writer = csv.writer(wf, delimiter='\t')
            for record in result:
                # print(record.id, record.natural_key, record.code, record.tc)
                writer.writerow([record.natural_key, record.tc])
                num += 1
                if num % 1000 == 0:
                    print("Fetched rows : {}".format(num))

    print("Fetched rows : {}".format(num))


def test_insert():
    record = RecordTable.create()
    token = RecordTokenTable.create()

    tmp_token = TempRecordTokenTable.create()

    # print(token.__dict__)

    # print(token.insert().__dict__)

    sel = select([tmp_token.c.id, tmp_token.c.natural_key, tmp_token.c.token])
    ins = token.insert()  # .values({token.c.id: tmp_token.c.id})

    # print(help(ins.from_select))
    print(help(record))
    ins2 = ins.from_select(['id', 'natural_key', 'code', 'token'], sel)
    print(str(ins2))

    # ins.select_names = sel
    # ins.include_insert_from_select_defaults = True

    print(str(sel))

    keys = [tmp_token.c.natural_key, tmp_token.c.code, tmp_token.c.token]
    select_query = select(keys)
    # insert_query = token.insert().select_from(select_query)
    insert_query = token.insert().from_select(keys, select_query)
    print(insert_query)


def test_untoken_records_using_sql_in(n):
    """Load Core result rows using fetchmany/streaming."""
    num = 1
    with pg_engine.connect() as conn:
        query_str = """with T3 AS (select natural_key from record_token where code='type-3'),
        T2 AS (select natural_key from record_token where code='type-2')
select natural_key,'type-3' as tc from record where natural_key not in (select natural_key from T3)
union
select natural_key,'type-2' as tc from record where natural_key not in (select natural_key from T2);"""

        t1 = time.time()
        result = conn.execution_options(stream_results=True).\
            execute(query_str)

        with open('record_out.csv', 'w') as wf:
            writer = csv.writer(wf, delimiter='\t')
            while True:
                chunk = result.fetchmany(10000)
                if not chunk:
                    break
                for record in chunk:
                    # data = row['id'], row['natural_key'], row['family_names']
                    writer.writerow([record.natural_key, record.tc])
                    num += 1
                    # if num % 1000 == 0:
                    #     print("Fetched rows : {}".format(num))

        print("Fetched rows : {}".format(num))

        print("Finished query with {}".format((time.time() - t1)))


def test_output_tokens():
    record = RecordTable.create()
    token = RecordTokenTable.create()
    j = record.join(token, and_(record.c.natural_key == token.c.natural_key))

    sel = select([record.c.id, record.c.natural_key, token.c.token, token.c.code]).\
        select_from(j).order_by(record.c.natural_key, token.c.code)

    print(sel)

    token_code_list = ['type-1', 'type-2', 'type-3', 'type-4']
    # return
    token_code_dict = {}
    pre_nk = None

    print("natural_key", end=',')
    [print('token{}'.format(n), end=',') for n in range(len(token_code_list))]
    print('')

    with pg_engine.connect() as conn:
        result = conn.execution_options(stream_results=True).execute(sel)

        for tup in result:
            if not pre_nk:
                pre_nk = tup.natural_key
                token_code_dict = {}

            # print(tup)
            # continue

            if pre_nk == tup.natural_key:
                token_code_dict[tup.code] = tup.token
            else:
                print(pre_nk, end=',')
                [print(token_code_dict.get(x, None), end=',') for x in token_code_list]
                print('')
                pre_nk = tup.natural_key
                token_code_dict = {}
                token_code_dict[tup.code] = tup.token

        print(pre_nk, end=',')
        [print(token_code_dict.get(x, None), end=',') for x in token_code_list]
        print('')

        # print(tup)


def testTokenExportor():
    record = RecordTable.create()
    token = RecordTokenTable.create()
    token_codes = ['type-1', 'type-2', 'type-3']
    with pg_engine.connect() as conn:
        with open("token_export.csv", 'w') as result_file:
            writer = csv.writer(result_file, dialect=DatabankTSV)

            token_export = TokenExportor(conn, token_codes, record, token)

            writer.writerow(token_export.header())
            [writer.writerow(x) for x in token_export]

            # print(token_export.header())

            # [print(x) for x in token_export]


def main():
    # test_RecordTable(1000000, 10000000)
    # test_stream_RecordTable(500000)
    # test_untoken_records(5000000)
    # test_untoken_records_using_sql_in(9000000)
    # test_insert()
    # test_connect_exec(100)
    # test_output_tokens()
    testTokenExportor()


if __name__ == '__main__':
    main()
