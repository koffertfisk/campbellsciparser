# !/usr/bin/env
# -*- coding: utf-8 -*-

import os.path

from collections import OrderedDict
from datetime import datetime

import pytz

from campbellsciparser.devices.base import CampbellSCIBaseParser
from campbellsciparser.devices.base import TimeColumnValueError
from campbellsciparser.devices.base import TimeParsingError
from tests.devices.common_all import *


class BaseParserTimeParsingTest(unittest.TestCase):

    def test_datetime_to_string_no_time_zone(self):
        baseparser = CampbellSCIBaseParser()
        time_zone = 'Europe/Stockholm'
        pytz_time_zone = pytz.timezone(time_zone)
        expected_datetime = datetime(2012, 11, 25, 22, 0, 0)
        dt = datetime(2012, 11, 25, 22, 0, 0, tzinfo=pytz_time_zone)
        expected_datetime_string = str(expected_datetime)
        dt_string = baseparser._datetime_to_string(dt=dt)

        self.assertEqual(expected_datetime_string, dt_string)

    def test_find_first_time_column_key(self):
        baseparser = CampbellSCIBaseParser()
        headers = ['key_1', 'key_2', 'key_3']
        time_columns = ['key_1', 'key_2']
        index = baseparser._find_first_time_column_key(headers, time_columns)

        self.assertEqual(index, 'key_1')

    def test_find_first_time_column_key_raise_value_error(self):
        baseparser = CampbellSCIBaseParser()
        headers = ['key_1', 'key_2', 'key_3']
        time_columns = ['key_4']
        with self.assertRaises(TimeColumnValueError):
            baseparser._find_first_time_column_key(headers, time_columns)

    def test_find_first_time_column_key_raise_index_error(self):
        baseparser = CampbellSCIBaseParser()
        headers = ['key_1', 'key_2', 'key_3']
        time_columns = []
        with self.assertRaises(IndexError):
            baseparser._find_first_time_column_key(headers, time_columns)

    def test_parse_custom_time_format_less_library_args(self):
        time_zone = 'Europe/Stockholm'
        baseparser = CampbellSCIBaseParser(
            pytz_time_zone=time_zone, time_format_args_library=[])
        
        parsing_info = baseparser._parse_custom_time_format('2016')
        parsed_time_format, parsed_time = parsing_info

        self.assertFalse(parsed_time_format)
        self.assertFalse(parsed_time)

    def test_parse_custom_time_format_less_time_values(self):
        time_zone = 'Europe/Stockholm'
        baseparser = CampbellSCIBaseParser(
            pytz_time_zone=time_zone, time_format_args_library=['%Y'])
        
        parsing_info = baseparser._parse_custom_time_format()
        parsed_time_format, parsed_time = parsing_info

        self.assertFalse(parsed_time_format)
        self.assertFalse(parsed_time)

    def test_parse_custom_time_format(self):
        time_zone = 'Europe/Stockholm'
        time_format_args_library = ['%Y', '%m', '%d']
        time_values_args = ['2016', '1', '1']
        expected_parsed_time_format = ','.join(time_format_args_library)
        expected_parsed_time_values = ','.join(time_values_args)
        baseparser = CampbellSCIBaseParser(
            pytz_time_zone=time_zone, time_format_args_library=time_format_args_library)
        
        parsing_info = baseparser._parse_custom_time_format(*time_values_args)
        parsed_time_format, parsed_time = parsing_info

        self.assertEqual(parsed_time_format, expected_parsed_time_format)
        self.assertEqual(parsed_time, expected_parsed_time_values)

    def test_convert_time_from_data_row_already_time_zone_aware(self):
        file = os.path.join(
            TEST_DEVICES_DIR, 'testdata/base/csv_base_testdata_1_row_time.dat')
        time_zone = 'Etc/GMT-1'
        pytz_time_zone = pytz.timezone(time_zone)
        time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S', '%z']
        time_columns = [i for i in range(6)]
        baseparser = CampbellSCIBaseParser(
            pytz_time_zone=time_zone, time_format_args_library=time_format_args_library)

        expected_datetime = datetime(2016, 1, 1, 22, 30, 15, tzinfo=pytz_time_zone)

        data = baseparser.read_data(infile_path=file)
        data_time_converted = baseparser.convert_time(data=data, time_columns=time_columns)
        data_time_converted_first_row = data_time_converted[0]
        data_time_converted_first_row_dt = data_time_converted_first_row.get(0)

        self.assertEqual(expected_datetime, data_time_converted_first_row_dt)

    def test_convert_time_from_data_row_not_already_time_zone_aware(self):
        file = os.path.join(
            TEST_DEVICES_DIR, 'testdata/base/csv_base_testdata_1_row_time_no_tz.dat')
        time_zone = 'Etc/GMT-1'
        pytz_time_zone = pytz.timezone(time_zone)
        time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S']
        time_columns = [i for i in range(6)]
        baseparser = CampbellSCIBaseParser(
            pytz_time_zone=time_zone, time_format_args_library=time_format_args_library)

        expected_datetime = datetime(2016, 1, 1, 22, 30, 15, tzinfo=pytz_time_zone)

        data = baseparser.read_data(infile_path=file)
        data_time_converted = baseparser.convert_time(data=data, time_columns=time_columns)
        data_time_converted_first_row = data_time_converted[0]
        data_time_converted_first_row_dt = data_time_converted_first_row.get(0)

        self.assertEqual(expected_datetime, data_time_converted_first_row_dt)

    def test_convert_time_from_data_row_with_column_name(self):
        file = os.path.join(
            TEST_DEVICES_DIR, 'testdata/base/csv_base_testdata_1_row_time.dat')
        time_zone = 'Etc/GMT-1'
        time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S', '%z']
        time_columns = [i for i in range(6)]
        baseparser = CampbellSCIBaseParser(
            pytz_time_zone=time_zone, time_format_args_library=time_format_args_library)

        data = baseparser.read_data(infile_path=file)
        expected_time_parsed_column = "TIMESTAMP"

        data_time_converted = baseparser.convert_time(
            data=data,
            time_columns=time_columns,
            time_parsed_column=expected_time_parsed_column
        )

        data_time_converted_first_row = data_time_converted[0]
        time_parsed_column_name = list(data_time_converted_first_row.keys())[0]

        self.assertEqual(time_parsed_column_name, expected_time_parsed_column)

    def test_convert_time_from_data_row_to_utc(self):
        file = os.path.join(
            TEST_DEVICES_DIR, 'testdata/base/csv_base_testdata_1_row_time.dat')
        time_zone = 'Etc/GMT-1'
        time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S', '%z']
        time_columns = [i for i in range(6)]
        baseparser = CampbellSCIBaseParser(
            pytz_time_zone=time_zone, time_format_args_library=time_format_args_library)

        expected_datetime = datetime(2016, 1, 1, 21, 30, 15, tzinfo=pytz.UTC)

        data = baseparser.read_data(infile_path=file)
        data_time_converted = baseparser.convert_time(
            data=data, time_columns=time_columns, to_utc=True)

        data_time_converted_first_row = data_time_converted[0]
        data_time_converted_first_row_dt = data_time_converted_first_row.get(0)

        self.assertEqual(expected_datetime, data_time_converted_first_row_dt)

    def test_convert_time_no_time_columns_error(self):
        data = list(OrderedDict())
        baseparser = CampbellSCIBaseParser()
        with self.assertRaises(TimeColumnValueError):
            baseparser.convert_time(data=data)

    def test_parse_time_values_no_values(self):
        time_zone = 'UTC'
        baseparser = CampbellSCIBaseParser(pytz_time_zone=time_zone)
        expected_dt = datetime(1900, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)
        parsed_dt = baseparser._parse_time_values()

        self.assertEqual(parsed_dt, expected_dt)

    def test_parse_time_values_raise_parsing_error(self):
        time_zone = 'UTC'
        time_format_args_library = ['%NotParsable']
        time_values = ['2016-01-01 22:15:30']
        baseparser = CampbellSCIBaseParser(
            pytz_time_zone=time_zone, time_format_args_library=time_format_args_library)

        with self.assertRaises(TimeParsingError):
            baseparser._parse_time_values(*time_values, ignore_parsing_error=False)

    def test_parse_time_values_ignore_parsing_error(self):
        time_zone = 'UTC'
        time_format_args_library = ['%NotParsable']
        time_values = ['2016-01-01 22:15:30']
        expected_dt = datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)

        baseparser = CampbellSCIBaseParser(
            pytz_time_zone=time_zone, time_format_args_library=time_format_args_library)

        parsed_dt = baseparser._parse_time_values(*time_values, ignore_parsing_error=True)

        self.assertEqual(parsed_dt, expected_dt)

    def test_parse_time_values_already_localized(self):
        time_zone = 'Etc/GMT-1'
        time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S', '%z']
        time_values = ['2016', '1', '1', '22', '15', '30', '+0100']
        expected_dt = datetime(2016, 1, 1, 22, 15, 30, tzinfo=pytz.timezone(time_zone))
        baseparser = CampbellSCIBaseParser(
            pytz_time_zone=time_zone, time_format_args_library=time_format_args_library)

        parsed_dt = baseparser._parse_time_values(*time_values)

        self.assertEqual(parsed_dt, expected_dt)

    def test_parse_time_values_no_lib(self):
        time_zone = 'UTC'
        baseparser = CampbellSCIBaseParser(pytz_time_zone=time_zone)
        expected_dt = datetime(1900, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)
        time_values = ['2016-01-01 22:15:30']

        parsed_dt = baseparser._parse_time_values(*time_values)

        self.assertEqual(parsed_dt, expected_dt)

    def test_parse_time_values(self):
        time_zone = 'UTC'
        time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S']
        baseparser = CampbellSCIBaseParser(
            pytz_time_zone=time_zone, time_format_args_library=time_format_args_library)
        expected_dt = datetime(2016, 1, 1, 22, 15, 30, tzinfo=pytz.UTC)
        time_values = ['2016', '1', '1', '22', '15', '30']

        parsed_dt = baseparser._parse_time_values(*time_values)

        self.assertEqual(parsed_dt, expected_dt)

    def test_parse_time_values_to_utc(self):
        time_zone = 'Europe/Stockholm'
        time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S']
        baseparser = CampbellSCIBaseParser(
            pytz_time_zone=time_zone, time_format_args_library=time_format_args_library)
        expected_dt = datetime(2016, 1, 1, 21, 15, 30, tzinfo=pytz.UTC)
        time_values = ['2016', '1', '1', '22', '15', '30']

        parsed_dt = baseparser._parse_time_values(*time_values, to_utc=True)

        self.assertEqual(parsed_dt, expected_dt)