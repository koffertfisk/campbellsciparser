#!/usr/bin/env
# -*- coding: utf-8 -*-

import os

from collections import OrderedDict

from campbellsciparser import cr

TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')


def test_process_mixed_array_rows_generator_empty():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_empty.dat')
    assert tuple(cr._process_mixed_array_rows(infile_path=file)) == ()


def test_process_mixed_array_rows_generator_exceeding_line_num():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_empty.dat')
    assert tuple(cr._process_mixed_array_rows(infile_path=file, first_line_num=1)) == ()


def test_process_mixed_array_rows_generator_three_rows():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_3_rows.dat')
    row_1 = OrderedDict([(0, '1')])
    row_2 = OrderedDict([(0, '1'), (1, '2')])
    row_3 = OrderedDict([(0, '1'), (1, '2'), (2, '3')])

    assert tuple(cr._process_mixed_array_rows(infile_path=file)) == (row_1, row_2, row_3)


def test_process_mixed_array_rows_generator_three_rows_slice():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_3_rows.dat')
    row_2 = OrderedDict([(0, '1'), (1, '2')])
    row_3 = OrderedDict([(0, '1'), (1, '2'), (2, '3')])

    assert tuple(cr._process_mixed_array_rows(infile_path=file, first_line_num=1)) == (row_2, row_3, )
    assert tuple(cr._process_mixed_array_rows(
        infile_path=file, first_line_num=1, last_line_num=1)) == (row_2, )
