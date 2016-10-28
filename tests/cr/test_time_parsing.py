# !/usr/bin/env
# -*- coding: utf-8 -*-

import os

from datetime import datetime

import pytest
import pytz

from campbellsciparser import cr
from campbellsciparser.dataset import DataSet
from campbellsciparser.dataset import Row

TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')


def test_datetime_to_string_no_time_zone():
    time_zone = 'Europe/Stockholm'
    pytz_time_zone = pytz.timezone(time_zone)
    expected_datetime = datetime(2012, 11, 25, 22, 0, 0)
    dt = datetime(2012, 11, 25, 22, 0, 0, tzinfo=pytz_time_zone)
    expected_datetime_string = str(expected_datetime)
    dt_string = cr._datetime_to_string(dt=dt)

    assert expected_datetime_string == dt_string


def test_find_first_time_column_name():
    column_names = ['key_1', 'key_2', 'key_3']
    time_columns = ['key_1', 'key_2']
    column_name = cr._find_first_time_column_name(column_names, time_columns)

    assert column_name == 'key_1'


def test_find_first_time_column_name_raise_value_error():
    column_names = ['key_1', 'key_2', 'key_3']
    time_columns = ['key_4']
    with pytest.raises(cr.TimeColumnValueError):
        cr._find_first_time_column_name(column_names, time_columns)


def test_find_first_time_column_name_raise_index_error():
    column_names = ['key_1', 'key_2', 'key_3']
    time_columns = []
    with pytest.raises(IndexError):
        cr._find_first_time_column_name(column_names, time_columns)


def test_parse_custom_time_formats_less_library_args():
    parsing_info = cr._parse_custom_time_formats([], '2016')
    parsed_time_format, parsed_time = parsing_info

    assert not parsed_time_format
    assert not parsed_time


def test_parse_custom_time_formats_less_time_values():
    parsing_info = cr._parse_custom_time_formats(['%Y'])
    parsed_time_format, parsed_time = parsing_info

    assert not parsed_time_format
    assert not parsed_time


def test_parse_custom_time_formats():
    time_format_args_library = ['%Y', '%m', '%d']
    time_values_args = ['2016', '1', '1']
    expected_parsed_time_format = ','.join(time_format_args_library)
    expected_parsed_time_values = ','.join(time_values_args)

    parsing_info = cr._parse_custom_time_formats(
        time_format_args_library, *time_values_args)

    parsed_time_format, parsed_time = parsing_info

    assert parsed_time_format == expected_parsed_time_format
    assert parsed_time == expected_parsed_time_values


def test_parse_time_from_data_row_already_time_zone_aware():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_1_row_time.dat')
    time_zone = 'Etc/GMT-1'
    pytz_time_zone = pytz.timezone(time_zone)
    time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S', '%z']
    time_columns = [i for i in range(7)]

    expected_datetime = datetime(2016, 1, 1, 22, 30, 15, tzinfo=pytz_time_zone)

    data = cr.read_table_data(infile_path=file)
    data_time_converted = cr.parse_time(
        data=data,
        time_zone=time_zone,
        time_format_args_library=time_format_args_library,
        time_columns=time_columns
    )

    data_time_converted_first_row = data_time_converted[0]
    data_time_converted_first_row_dt = data_time_converted_first_row.get(0)

    assert expected_datetime == data_time_converted_first_row_dt


def test_parse_time_from_data_row_not_already_time_zone_aware():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_1_row_time_no_tz.dat')
    time_zone = 'Etc/GMT-1'
    pytz_time_zone = pytz.timezone(time_zone)
    time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S']
    time_columns = [i for i in range(6)]

    expected_datetime = datetime(2016, 1, 1, 22, 30, 15, tzinfo=pytz_time_zone)

    data = cr.read_table_data(infile_path=file)
    data_time_converted = cr.parse_time(
        data=data,
        time_zone=time_zone,
        time_format_args_library=time_format_args_library,
        time_columns=time_columns
    )

    data_time_converted_first_row = data_time_converted[0]
    data_time_converted_first_row_dt = data_time_converted_first_row.get(0)

    assert expected_datetime == data_time_converted_first_row_dt


def test_parse_time_from_data_row_with_column_name():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_1_row_time.dat')
    time_zone = 'Etc/GMT-1'
    time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S', '%z']
    time_columns = [i for i in range(7)]

    data = cr.read_table_data(infile_path=file)
    expected_time_parsed_column = "TIMESTAMP"

    data_time_converted = cr.parse_time(
        data=data,
        time_zone=time_zone,
        time_format_args_library=time_format_args_library,
        time_columns=time_columns,
        time_parsed_column=expected_time_parsed_column
    )

    data_time_converted_first_row = data_time_converted[0]
    time_parsed_column_name = list(data_time_converted_first_row.keys())[0]

    assert time_parsed_column_name == expected_time_parsed_column


def test_parse_time_from_data_row_to_utc():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_1_row_time.dat')
    time_zone = 'Etc/GMT-1'
    time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S', '%z']
    time_columns = [i for i in range(7)]

    expected_datetime = datetime(2016, 1, 1, 21, 30, 15, tzinfo=pytz.UTC)

    data = cr.read_table_data(infile_path=file)
    data_time_converted = cr.parse_time(
        data=data,
        time_zone=time_zone,
        time_format_args_library=time_format_args_library,
        time_columns=time_columns,
        to_utc=True)

    data_time_converted_first_row = data_time_converted[0]
    data_time_converted_first_row_dt = data_time_converted_first_row.get(0)

    assert expected_datetime == data_time_converted_first_row_dt


def test_parse_time_from_data_row_replace_column():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_1_row_time_and_values.dat')
    time_zone = 'Etc/GMT-1'
    time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S']
    time_columns = [i + 1 for i in range(6)]

    expected_datetime = datetime(2016, 1, 1, 21, 30, 15, tzinfo=pytz.UTC)

    data = cr.read_table_data(infile_path=file)
    data_time_converted = cr.parse_time(
        data=data,
        time_zone=time_zone,
        time_format_args_library=time_format_args_library,
        time_columns=time_columns,
        replace_time_column=3,
        to_utc=True
    )

    data_time_converted_first_row = data_time_converted[0]
    data_time_converted_first_row_dt = data_time_converted_first_row.get(3)

    assert expected_datetime == data_time_converted_first_row_dt


def test_parse_time_no_time_columns_error():
    data = list(Row())
    with pytest.raises(cr.TimeColumnValueError):
        cr.parse_time(
            data=data, time_zone='UTC', time_format_args_library=[], time_columns=[])


def test_parse_time_values_no_values():
    pytz_time_zone = pytz.timezone('UTC')
    time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S']
    expected_dt = datetime(1900, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)
    parsed_dt = cr._parse_time_values(pytz_time_zone, time_format_args_library)

    assert parsed_dt == expected_dt


def test_parse_time_values_raise_parsing_error():
    pytz_time_zone = pytz.timezone('UTC')
    time_format_args_library = ['%NotParsable']
    time_values = ['2016-01-01 22:15:30']

    with pytest.raises(cr.TimeParsingError):
        cr._parse_time_values(pytz_time_zone, time_format_args_library,
                              *time_values, ignore_parsing_error=False)


def test_parse_time_values_ignore_parsing_error():
    pytz_time_zone = pytz.timezone('UTC')
    time_format_args_library = ['%NotParsable']
    time_values = ['2016-01-01 22:15:30']
    expected_dt = datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)

    parsed_dt = cr._parse_time_values(pytz_time_zone, time_format_args_library,
                                      *time_values, ignore_parsing_error=True)

    assert parsed_dt == expected_dt


def test_parse_time_values_already_localized():
    pytz_time_zone = pytz.timezone('Etc/GMT-1')
    time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S', '%z']
    time_values = ['2016', '1', '1', '22', '15', '30', '+0100']
    expected_dt = datetime(2016, 1, 1, 22, 15, 30, tzinfo=pytz_time_zone)

    parsed_dt = cr._parse_time_values(pytz_time_zone, time_format_args_library, *time_values)

    assert parsed_dt == expected_dt


def test_parse_time_values_no_lib():
    pytz_time_zone = pytz.timezone('UTC')
    expected_dt = datetime(1900, 1, 1, 0, 0, 0, tzinfo=pytz_time_zone)
    time_values = ['2016-01-01 22:15:30']

    parsed_dt = cr._parse_time_values(pytz_time_zone, [], *time_values)

    assert parsed_dt == expected_dt


def test_parse_time_values():
    pytz_time_zone = pytz.timezone('UTC')
    time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S']
    expected_dt = datetime(2016, 1, 1, 22, 15, 30, tzinfo=pytz.UTC)
    time_values = ['2016', '1', '1', '22', '15', '30']

    parsed_dt = cr._parse_time_values(pytz_time_zone, time_format_args_library, *time_values)

    assert parsed_dt == expected_dt


def test_parse_time_values_to_utc():
    pytz_time_zone = pytz.timezone('Europe/Stockholm')
    time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S']
    expected_dt = datetime(2016, 1, 1, 21, 15, 30, tzinfo=pytz.UTC)
    time_values = ['2016', '1', '1', '22', '15', '30']

    parsed_dt = cr._parse_time_values(pytz_time_zone, time_format_args_library,
                                      *time_values, to_utc=True)

    assert parsed_dt == expected_dt


def test_parse_hour_minute():
    time_string_len_one = '0'
    time_string_len_two = '30'
    time_string_len_three = '945'
    time_string_len_four = '2355'

    assert cr._parse_hourminute(time_string_len_one) == '0000'
    assert cr._parse_hourminute(time_string_len_two) == '0030'
    assert cr._parse_hourminute(time_string_len_three) == '0945'
    assert cr._parse_hourminute(time_string_len_four) == '2355'


def test_parse_hour_minute_raises_error():
    with pytest.raises(cr.TimeColumnValueError):
        cr._parse_hourminute('')
    with pytest.raises(cr.TimeColumnValueError):
        cr._parse_hourminute('12345')


def test_convert_time_zone():
    from_time_zone = 'Europe/Stockholm'
    to_time_zone = 'UTC'
    dt = datetime(2016, 1, 1, 21, 15, 30, tzinfo=pytz.timezone(from_time_zone))

    data = DataSet([
        Row([('Label_1', 'some_value'), ('Label_2', dt), ('Label_3', 'some_other_value')])
    ])

    cr.convert_time_zone(data, time_column='Label_2', to_time_zone=to_time_zone)

    expected_dt = datetime(2016, 1, 1, 20, 15, 30, tzinfo=pytz.timezone(to_time_zone))

    time_converted_time_zone = data[0].get('Label_2')

    assert time_converted_time_zone == expected_dt


def test_convert_time_zone_raises_error():
    with pytest.raises(cr.UnknownPytzTimeZoneError):
        cr.convert_time_zone(DataSet([]), time_column='Label_2', to_time_zone='Foo')