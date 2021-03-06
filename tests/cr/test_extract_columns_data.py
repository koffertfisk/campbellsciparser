# !/usr/bin/env
# -*- coding: utf-8 -*-

import os

from datetime import datetime

import pytz

from campbellsciparser import cr
from campbellsciparser.dataset import DataSet
from campbellsciparser.dataset import Row

TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')


def assert_two_data_sets_equal(dataset_1, dataset_2):
    assert len(dataset_1) == len(dataset_2)

    for row_1, row_2 in zip(dataset_1, dataset_2):
        for name_value_1, name_value_2 in zip(row_1.items(), row_2.items()):
            assert name_value_1 == name_value_2


def test_extract_columns_data_generator():
    time_zone = 'Etc/GMT-1'
    time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S']
    time_columns = ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Second']

    data = DataSet([Row([
        ('Id', '100'), ('Year', '2016'), ('Month', '1'), ('Day', '1'), ('Hour', '22'),
        ('Minute', '30'), ('Second', '15'), ('Value', '200')
    ])])

    assert tuple(cr._extract_columns_data_generator(
        data)) == (data[0],)

    expected_row = Row([('Year', '2016'), ('Month', '1'), ('Value', '200')])

    assert tuple(cr._extract_columns_data_generator(
        data, 'Year', 'Month', 'Value')) == (expected_row, )

    data_row_time_converted = cr.parse_time(
        data=data,
        time_zone=time_zone,
        time_format_args_library=time_format_args_library,
        time_parsed_column='Timestamp',
        time_columns=time_columns,
        to_utc=True
    )

    expected_datetime = datetime(2016, 1, 1, 21, 30, 15, tzinfo=pytz.UTC)
    expected_row = Row([('Timestamp', expected_datetime), ('Value', '200')])

    assert tuple(cr._extract_columns_data_generator(
        data_row_time_converted, 'Timestamp', 'Value')) == (expected_row, )


def test_extract_columns_data_generator_time_range():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_5_rows_time_and_values.dat')
    time_zone = 'Etc/GMT-1'
    time_format_args_library = ['%Y-%m-%d %H:%M:%S']
    time_columns = [1]

    data_three_rows = cr.read_table_data(infile_path=file)

    data_time_converted = cr.parse_time(
        data=data_three_rows,
        time_zone=time_zone,
        time_format_args_library=time_format_args_library,
        time_columns=time_columns,
        to_utc=True
    )

    expected_datetime_row_1 = datetime(2016, 1, 1, 18, 30, 15, tzinfo=pytz.UTC)
    expected_datetime_row_2 = datetime(2016, 2, 1, 19, 30, 15, tzinfo=pytz.UTC)
    expected_datetime_row_3 = datetime(2016, 3, 1, 20, 30, 15, tzinfo=pytz.UTC)
    expected_datetime_row_4 = datetime(2016, 4, 1, 21, 30, 15, tzinfo=pytz.UTC)
    expected_datetime_row_5 = datetime(2016, 5, 1, 22, 30, 15, tzinfo=pytz.UTC)

    expected_row_1 = Row([(1, expected_datetime_row_1), (2, '200')])
    expected_row_2 = Row([(1, expected_datetime_row_2), (2, '210')])
    expected_row_3 = Row([(1, expected_datetime_row_3), (2, '220')])
    expected_row_4 = Row([(1, expected_datetime_row_4), (2, '230')])
    expected_row_5 = Row([(1, expected_datetime_row_5), (2, '240')])

    assert tuple(cr._extract_columns_data_generator(
        data_time_converted, 1, 2, time_column=1)) == (
               expected_row_1, expected_row_2, expected_row_3, expected_row_4, expected_row_5)

    from_timestamp = datetime(2016, 3, 1, 20, 0, 0, tzinfo=pytz.UTC)

    assert tuple(
        cr._extract_columns_data_generator(
            data_time_converted, 1, 2, time_column=1, from_timestamp=from_timestamp
        )
    ) == (expected_row_3, expected_row_4, expected_row_5)

    to_timestamp = datetime(2016, 4, 1, 22, 0, 0, tzinfo=pytz.UTC)

    assert tuple(
        cr._extract_columns_data_generator(
            data_time_converted, 1, 2, time_column=1,
            to_timestamp=to_timestamp
        )
    ) == (expected_row_1, expected_row_2, expected_row_3, expected_row_4)

    assert tuple(
        cr._extract_columns_data_generator(
            data_time_converted, 1, 2, time_column=1,
            from_timestamp=from_timestamp, to_timestamp=to_timestamp
        )
    ) == (expected_row_3, expected_row_4)

    expected_row_3 = Row([(0, '100'), (1, expected_datetime_row_3), (2, '220')])
    expected_row_4 = Row([(0, '100'), (1, expected_datetime_row_4), (2, '230')])

    assert tuple(
        cr._extract_columns_data_generator(
            data_time_converted, time_column=1,
            from_timestamp=from_timestamp, to_timestamp=to_timestamp
        )
    ) == (expected_row_3, expected_row_4)


def test_extract_columns_data():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_5_rows_time_and_values.dat')
    time_zone = 'Etc/GMT-1'
    time_format_args_library = ['%Y-%m-%d %H:%M:%S']
    time_columns = [1]
    from_timestamp = datetime(2016, 3, 1, 20, 0, 0, tzinfo=pytz.UTC)
    to_timestamp = datetime(2016, 4, 1, 22, 0, 0, tzinfo=pytz.UTC)
    data_three_rows = cr.read_table_data(infile_path=file)

    data_time_converted = cr.parse_time(
        data=data_three_rows,
        time_zone=time_zone,
        time_format_args_library=time_format_args_library,
        time_columns=time_columns,
        to_utc=True
    )

    expected_datetime_row_1 = datetime(2016, 3, 1, 20, 30, 15, tzinfo=pytz.UTC)
    expected_datetime_row_2 = datetime(2016, 4, 1, 21, 30, 15, tzinfo=pytz.UTC)

    expected_data = DataSet([
        Row([(1, expected_datetime_row_1), (2, '220')]),
        Row([(1, expected_datetime_row_2), (2, '230')]),
    ])

    data_extracted_columns = cr.extract_columns_data(
        data_time_converted, 1, 2, time_column=1,
        from_timestamp=from_timestamp, to_timestamp=to_timestamp)

    assert_two_data_sets_equal(data_extracted_columns, expected_data)
