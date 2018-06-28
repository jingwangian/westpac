#!/usr/bin/env python3

"""
Create the dummy data csv file for different tables:

record_xxx.csv for record table
record_token_xxx.csv for record_token table
tokenspec.csv for tokenspecification table

"""

import os
import csv
import arrow
import random
import base62
import base64
import numpy as np

import argparse
from typing import Iterable, TypeVar, Hashable, Sequence, List
T = TypeVar('T')

token_code_list = [('type-1', 10), ('type-2', 9), ('type-3', 6)]

new_token_code_list = ['type-4', 'type-5']


def is_select(rate):
    if rate == 10:
        return True
    else:
        return random.randrange(1, 11) <= rate


def chunk(iterable: Iterable[T], n: int) -> List[Iterable[T]]:
    continue_ = True

    def inner():
        nonlocal continue_
        record_list = []
        for _ in range(n):
            try:
                record_list.append(next(iterable))
            except StopIteration:
                continue_ = False
                break

        return record_list

    while continue_:
        yield inner()


def get_a_prefix():
    b = os.urandom(8)
    return base62.encode(b)


def get_prefix_list(num):
    return [get_a_prefix() for _ in range(num)]


def genearete_a_token(prefix):
    return prefix + base62.encode(os.urandom(8))


def generate_natural_key(len_nk):
    nk = base64.b64encode(os.urandom(len_nk))
    return nk.decode()


def random_pick(range_, probability=None):
    picked_value = np.random.choice(range_, p=probability)
    return picked_value


def generate_record(filename='record.csv', start_number=1, record_count=1000, gender='M'):
    """
    Generate record data into the file for table record
    """
    num = 1
    with open(filename, 'w') as wf:
        writer_1 = csv.writer(wf, delimiter='\t')

        for _ in range(record_count):
            nk = generate_natural_key(49)
            fa_name = "family_name_{}".format(start_number)
            first_name = "first_name_{}".format(start_number)
            modified_date = arrow.utcnow().format('YYYY-MM-DD HH:mm:ss')
            writer_1.writerow([start_number, nk, fa_name, first_name, gender, modified_date])

            num += 1
            start_number += 1

            if num % 1000 == 0:
                print("Created records ", num)


def generate_record_token(record_file, filename, start_number=1, max_record_count=0):
    """
    Generate record token data into the file for table record_token
    """
    total_records = 0

    if not record_file:
        print("Error: record_file is None in generate_record_token")
        return 0

    if not filename:
        print("Error: filename is None in generate_record_token")
        return 0

    random.seed(2018)

    code_prefix_tups = [(code, get_a_prefix(), rate) for code, rate in token_code_list]

    with open(filename, 'w') as wf:
        writer_1 = csv.writer(wf, delimiter='\t')
        for tup in code_prefix_tups:
            rate = tup[2]
            print("rate = ", rate)
            with open(record_file) as rf:
                reader_1 = csv.reader(rf, delimiter='\t')
                for record_list in chunk(reader_1, 100000):
                    if not record_list:
                        break
                    for row in random_get(record_list, len(record_list), rate):
                        nk = row[1]
                        token = genearete_a_token(tup[1])
                        code = tup[0]
                        writer_1.writerow([start_number, nk, code, token, row[2]])
                        total_records += 1
                        start_number += 1
                        if total_records % 1000 == 0:
                            print("generated records : {}".format(total_records))
                        if max_record_count:
                            if max_record_count <= total_records:
                                print("generate_record_token: write {} rows into {}".format(total_records, filename))

                                return total_records

    print("generate_record_token: write {} rows into {}".format(total_records, filename))

    return total_records


def generate_tokenspec(filename='token_spec.csv', record_count=1000):
    pass


def random_get(record_list, count, rate=10):
    """
    Random select one item from the list and remove that item from list

    rate: is used to return the item or only delete it
    """
    while True:
        n = random.randrange(count)
        if is_select(rate):
            yield record_list.pop(n)
        else:
            record_list.pop(n)
        count -= 1
        if count == 0:
            break


def test_trunk(record_file):
    with open(record_file) as rf:
        reader_1 = csv.reader(rf, delimiter='\t')

        for record_list in chunk(reader_1, 30):
            # [print(x) for x in record_list]
            # print("===================")
            [print(x) for x in random_get(record_list, len(record_list), 9)]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("type", help="record, token, tokespec")
    parser.add_argument("count", help="The number of records to be created")
    parser.add_argument("--file", "-f", help="File name to be output")
    parser.add_argument("--record", "-r", help="record file name")
    parser.add_argument("--start_num", "-s", help="start number")
    parser.add_argument("--gender", "-g", help="gender")

    args = parser.parse_args()

    table_type = args.type
    record_count = args.count
    record_count = int(record_count)
    file_name = args.file
    record_file = args.record
    start_number = args.start_num
    gender = args.gender

    if start_number:
        start_number = int(start_number)
    else:
        start_number = 1

    # k1 = generate_natural_key(15)
    # k2 = generate_natural_key(49)

    # print(k1, len(k1))
    # print(k2, len(k2))

    if table_type == 'record':
        # Usage: src/dummy_data.py record 10 -f a1.tsv -s 200
        generate_record(file_name, start_number, record_count, gender)
    elif table_type == 'token':
        generate_record_token(record_file, file_name, start_number, record_count)
    elif table_type == 'tokenspec':
        generate_tokenspec(file_name, record_count)
    else:
        print("Unknown type : {}".format(table_type))


if __name__ == '__main__':
    main()
    # test_trunk('../data/a1.csv')
