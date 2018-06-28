#!/usr/bin/env python3

"""
Identify the untoken records

usage: python3 identifier filename.csv
"""

import os
import csv

import argparse


def find_untoken_record(in_file, out_file, max_count=0):
    """
    Find record which hasn't matched code with it

    Return List[Tup(natural_key, code)]
    """
    num = 1
    with open(in_file) as rf, open(out_file, 'w') as wf:
        reader_1 = csv.reader(rf, delimiter='\t')
        writer_1 = csv.writer(wf, delimiter='\t')

        for row in reader_1:
            if len(row[1]) == 0:
                print(row)

            num += 1

            if num > 200:
                break

            if num % 1000 == 0:
                print("Created records ", num)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("infile", help="in file")
    parser.add_argument("out", help="output file")

    args = parser.parse_args()

    in_file = args.infile
    out_file = args.out
    # record_count = int(record_count)
    # file_name = args.file
    # record_file = args.record

    find_untoken_record(in_file, out_file, 1000)


if __name__ == '__main__':
    main()
    # generate_record_token("../data/a1.csv")
    # prefix_list = get_prefix_list(3)
    # [print(x) for x in prefix_list]
