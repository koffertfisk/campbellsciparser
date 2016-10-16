#!/usr/bin/env
# -*- coding: utf-8 -*-

import os

from collections import OrderedDict
from datetime import datetime

import pytest
import pytz

from campbellsciparser.devices import CampbellSCIBaseParser
from campbellsciparser.devices import TimeColumnValueError

TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')


def test_data_generator():
    test_list = [1, 2, 3]
    baseparser = CampbellSCIBaseParser()

    assert tuple(baseparser._data_generator(test_list)) == (1, 2, 3)


def test_process_rows_generator_empty():
    file = os.path.join(TEST_DATA_DIR, 'csv_base_testdata_empty.dat')
    baseparser = CampbellSCIBaseParser()

    assert tuple(baseparser._process_rows(infile_path=file)) == ()


def test_process_rows_generator_exceeding_line_num():
    file = os.path.join(TEST_DATA_DIR, 'csv_base_testdata_empty.dat')
    baseparser = CampbellSCIBaseParser()

    assert tuple(baseparser._process_rows(infile_path=file, line_num=1)) == ()


def test_process_rows_generator_three_rows():
    file = os.path.join(TEST_DATA_DIR, 'csv_base_testdata_3_rows.dat')
    baseparser = CampbellSCIBaseParser()
    row_1 = OrderedDict([(0, '1')])
    row_2 = OrderedDict([(0, '1'), (1, '2')])
    row_3 = OrderedDict([(0, '1'), (1, '2'), (2, '3')])

    assert tuple(baseparser._process_rows(infile_path=file)) == (row_1, row_2, row_3)


def test_process_rows_generator_three_rows_slice():
    file = os.path.join(TEST_DATA_DIR, 'csv_base_testdata_3_rows.dat')
    baseparser = CampbellSCIBaseParser()
    row_2 = OrderedDict([(0, '1'), (1, '2')])
    row_3 = OrderedDict([(0, '1'), (1, '2'), (2, '3')])

    assert tuple(baseparser._process_rows(infile_path=file, line_num=1)) == (row_2, row_3)


def test_process_rows_generator_three_rows_headers():
    file = os.path.join(TEST_DATA_DIR, 'csv_base_testdata_3_rows.dat')
    baseparser = CampbellSCIBaseParser()
    headers = ['Label_' + str(i) for i in range(3)]

    row_1 = OrderedDict([('Label_0', '1')])
    row_2 = OrderedDict([('Label_0', '1'), ('Label_1', '2')])
    row_3 = OrderedDict([('Label_0', '1'), ('Label_1', '2'), ('Label_2', '3')])

    assert tuple(baseparser._process_rows(
        infile_path=file,
        headers=headers,
        line_num=0)
    ) == (row_1, row_2, row_3)


def test_process_rows_generator_three_rows_indices():
    file = os.path.join(TEST_DATA_DIR, 'csv_base_testdata_3_rows.dat')
    baseparser = CampbellSCIBaseParser()

    row_1 = OrderedDict([(0, '1')])
    row_2 = OrderedDict([(0, '1'), (1, '2')])
    row_3 = OrderedDict([(0, '1'), (1, '2'), (2, '3')])

    assert tuple(baseparser._process_rows(
        infile_path=file,
        line_num=0)
        ) == (row_1, row_2, row_3)


def test_process_rows_generator_three_rows_less_headers():
    file = os.path.join(TEST_DATA_DIR, 'csv_base_testdata_3_rows.dat')
    baseparser = CampbellSCIBaseParser()
    headers = ['Label_0']

    row_1 = OrderedDict([('Label_0', '1')])
    row_2 = row_1
    row_3 = row_1

    assert tuple(baseparser._process_rows(
        infile_path=file, headers=headers, line_num=0)) == (row_1, row_2, row_3)


def test_process_rows_generator_three_rows_headers_row():
    file = os.path.join(TEST_DATA_DIR, 'csv_base_testdata_3_rows_headers.dat')
    baseparser = CampbellSCIBaseParser()

    row_1 = OrderedDict([('Label_0', '1')])
    row_2 = OrderedDict([('Label_0', '1'), ('Label_1', '2')])
    row_3 = OrderedDict([('Label_0', '1'), ('Label_1', '2'), ('Label_2', '3')])

    assert tuple(baseparser._process_rows(
        infile_path=file, header_row=0, line_num=0)) == (row_1, row_2, row_3)


def test_read_rows_generator_three_rows():
    file = os.path.join(TEST_DATA_DIR, 'csv_base_testdata_3_rows.dat')
    baseparser = CampbellSCIBaseParser()

    row_1 = OrderedDict([(0, '1')])
    row_2 = OrderedDict([(0, '1'), (1, '2')])
    row_3 = OrderedDict([(0, '1'), (1, '2'), (2, '3')])

    assert tuple(baseparser._read_data(
        infile_path=file)) == (row_1, row_2, row_3)


def test_values_to_strings():
    baseparser = CampbellSCIBaseParser()
    row = OrderedDict([
        ('Label_0', 'string'),
        ('Label_1', datetime(2016, 1, 1, 22, 30, 0)),
        ('Label_2', 15.7)
    ])

    expected_row_values = ['string', '2016-01-01 22:30:00', '15.7']
    row_values_converted = baseparser._values_to_strings(row=row)

    assert list(row_values_converted) == expected_row_values


def test_read_data_length_empty():
    file = os.path.join(TEST_DATA_DIR, 'csv_base_testdata_empty.dat')
    baseparser = CampbellSCIBaseParser()
    data = baseparser.read_data(infile_path=file)

    assert len(data) == 0


def test_read_data_ten_rows_length():
    file = os.path.join(TEST_DATA_DIR, 'csv_base_testdata_10_rows.dat')
    baseparser = CampbellSCIBaseParser()
    data = baseparser.read_data(infile_path=file)

    assert len(data) == 10


def test_read_data_line_num_five_rows_length():
    file = os.path.join(TEST_DATA_DIR, 'csv_base_testdata_10_rows.dat')
    baseparser = CampbellSCIBaseParser()
    data = baseparser.read_data(infile_path=file, line_num=5)

    assert len(data) == 5

def test_read_data_ten_rows_indices():
    file = os.path.join(TEST_DATA_DIR, 'csv_base_testdata_10_rows.dat')
    expected_headers = [i for i in range(10)]
    baseparser = CampbellSCIBaseParser()
    data = baseparser.read_data(infile_path=file, line_num=0)

    for row in data:
        for name, expected_name in zip(list(row.keys()), expected_headers):
            assert name == expected_name


def test_read_data_ten_rows_headers():
    file = os.path.join(TEST_DATA_DIR, 'csv_base_testdata_10_rows.dat')
    headers = ['Label_' + str(i) for i in range(10)]
    baseparser = CampbellSCIBaseParser()
    data = baseparser.read_data(infile_path=file, headers=headers, line_num=0)

    for row in data:
        for name, expected_name in zip(list(row.keys()), headers):
            assert name == expected_name


def test_read_data_three_rows_header_row():
    file = os.path.join(TEST_DATA_DIR, 'csv_base_testdata_3_rows_headers.dat')
    baseparser = CampbellSCIBaseParser()
    data = baseparser.read_data(infile_path=file, header_row=0, line_num=0)
    expected_headers = ['Label_' + str(i) for i in range(3)]

    for row in data:
        for name, expected_name in zip(list(row.keys()), expected_headers):
            assert name == expected_name


def test_read_data_convert_time_no_time_columns():
    file = os.path.join(TEST_DATA_DIR, 'csv_base_testdata_empty.dat')
    baseparser = CampbellSCIBaseParser()

    with pytest.raises(TimeColumnValueError):
        baseparser.read_data(infile_path=file, convert_time=True)


def test_read_data_convert_time():
    file = os.path.join(TEST_DATA_DIR, 'csv_base_testdata_1_row_time_no_tz.dat')
    time_zone = 'Europe/Stockholm'
    pytz_time_zone = pytz.timezone(time_zone)
    time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S']
    time_columns = [i for i in range(6)]
    expected_datetime = datetime(2016, 1, 1, 22, 30, 15, tzinfo=pytz_time_zone)

    baseparser = CampbellSCIBaseParser(
        pytz_time_zone=time_zone, time_format_args_library=time_format_args_library)

    data = baseparser.read_data(infile_path=file, convert_time=True, time_columns=time_columns)
    data_first_row = data[0]
    dt_first_row = data_first_row.get(0)

    assert dt_first_row == expected_datetime


def test_read_data_convert_time_to_utc():
    file = os.path.join(TEST_DATA_DIR, 'csv_base_testdata_1_row_time_no_tz.dat')
    time_zone = 'Europe/Stockholm'
    time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S']
    time_columns = [i for i in range(6)]
    expected_datetime = datetime(2016, 1, 1, 21, 30, 15, tzinfo=pytz.UTC)

    baseparser = CampbellSCIBaseParser(
        pytz_time_zone=time_zone, time_format_args_library=time_format_args_library)

    data = baseparser.read_data(
        infile_path=file, convert_time=True, time_columns=time_columns, to_utc=True)

    data_first_row = data[0]
    dt_first_row = data_first_row.get(0)

    assert dt_first_row == expected_datetime


def test_read_data_time_parsed_column_name():
    file = os.path.join(TEST_DATA_DIR, 'csv_base_testdata_1_row_time_no_tz.dat')
    time_zone = 'Europe/Stockholm'
    time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S']
    time_columns = [i for i in range(6)]
    expected_time_parsed_column_name = 'TIMESTAMP'

    baseparser = CampbellSCIBaseParser(
        pytz_time_zone=time_zone, time_format_args_library=time_format_args_library)

    data = baseparser.read_data(
        infile_path=file,
        convert_time=True,
        time_parsed_column=expected_time_parsed_column_name,
        time_columns=time_columns
    )

    data_first_row = data[0]
    time_parsed_column_name = list(data_first_row.keys())[0]

    assert time_parsed_column_name == expected_time_parsed_column_name
