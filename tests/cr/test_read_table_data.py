#!/usr/bin/env
# -*- coding: utf-8 -*-

import os

from collections import OrderedDict
from datetime import datetime

import pytest
import pytz

from campbellsciparser import cr

TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')


def test_read_table_rows_generator_three_rows():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_3_rows.dat')

    row_1 = OrderedDict([(0, '1')])
    row_2 = OrderedDict([(0, '1'), (1, '2')])
    row_3 = OrderedDict([(0, '1'), (1, '2'), (2, '3')])

    assert tuple(cr._read_table_data(
        infile_path=file)) == (row_1, row_2, row_3)


def test_read_table_data_length_empty():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_empty.dat')
    data = cr.read_table_data(infile_path=file)

    assert len(data) == 0


def test_read_table_data_ten_rows_length():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_10_rows.dat')
    data = cr.read_table_data(infile_path=file)

    assert len(data) == 10


def test_read_table_data_line_num_five_rows_length():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_10_rows.dat')
    data = cr.read_table_data(infile_path=file, first_line_num=5)

    assert len(data) == 5


def test_read_table_data_ten_rows_indices():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_10_rows.dat')
    expected_header = [i for i in range(10)]
    data = cr.read_table_data(infile_path=file, first_line_num=0)

    for row in data:
        for name, expected_name in zip(list(row.keys()), expected_header):
            assert name == expected_name


def test_read_table_data_ten_rows_header():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_10_rows.dat')
    header = ['Label_' + str(i) for i in range(10)]
    data = cr.read_table_data(infile_path=file, header=header, first_line_num=0)

    for row in data:
        for name, expected_name in zip(list(row.keys()), header):
            assert name == expected_name


def test_read_table_data_three_rows_header_row():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_3_rows_header.dat')
    data = cr.read_table_data(infile_path=file, header_row=0, first_line_num=0)
    expected_header = ['Label_' + str(i) for i in range(3)]

    for row in data:
        for name, expected_name in zip(list(row.keys()), expected_header):
            assert name == expected_name


def test_read_table_data_parse_time_no_time_columns():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_empty.dat')
    with pytest.raises(cr.TimeColumnValueError):
        cr.read_table_data(infile_path=file, parse_time_values=True)


def test_read_table_data_parse_time():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_1_row_time_no_tz.dat')
    time_zone = 'Europe/Stockholm'
    pytz_time_zone = pytz.timezone(time_zone)
    time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S']
    time_columns = [i for i in range(6)]
    expected_datetime = datetime(2016, 1, 1, 22, 30, 15, tzinfo=pytz_time_zone)

    data = cr.read_table_data(
        infile_path=file,
        parse_time_values=True,
        time_zone=time_zone,
        time_format_args_library=time_format_args_library,
        time_columns=time_columns
    )

    data_first_row = data[0]
    dt_first_row = data_first_row.get(0)

    assert dt_first_row == expected_datetime


def test_read_table_data_convert_time_to_utc():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_1_row_time_no_tz.dat')
    time_zone = 'Europe/Stockholm'
    time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S']
    time_columns = [i for i in range(6)]
    expected_datetime = datetime(2016, 1, 1, 21, 30, 15, tzinfo=pytz.UTC)

    data = cr.read_table_data(
        infile_path=file,
        parse_time_values=True,
        time_zone=time_zone,
        time_format_args_library=time_format_args_library,
        time_columns=time_columns,
        to_utc=True
    )

    data_first_row = data[0]
    dt_first_row = data_first_row.get(0)

    assert dt_first_row == expected_datetime


def test_read_table_data_time_parsed_column_name():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_1_row_time_no_tz.dat')
    time_zone = 'Europe/Stockholm'
    time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S']
    time_columns = [i for i in range(6)]
    expected_time_parsed_column_name = 'TIMESTAMP'

    data = cr.read_table_data(
        infile_path=file,
        parse_time_values=True,
        time_zone=time_zone,
        time_format_args_library=time_format_args_library,
        time_parsed_column=expected_time_parsed_column_name,
        time_columns=time_columns
    )

    data_first_row = data[0]
    time_parsed_column_name = list(data_first_row.keys())[0]

    assert time_parsed_column_name == expected_time_parsed_column_name
