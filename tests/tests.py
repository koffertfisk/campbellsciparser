#!/usr/bin/env
# -*- coding: utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytz
import shutil
import unittest

from collections import OrderedDict
from datetime import datetime


from campbellsciparser.devices import CampbellSCIBaseParser
from campbellsciparser.devices import TimeColumnValueError
from campbellsciparser.devices import TimeParsingError
from campbellsciparser.devices import UnknownPytzTimeZoneError
from campbellsciparser.devices import CR10Parser
from campbellsciparser.devices import CR10XParser
from campbellsciparser.devices import CR1000Parser


THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class ReadDataTestCase(unittest.TestCase):

    def assertDataLengthEqual(self, data, data_length):
        self.assertEqual(len(data), data_length)

    def assertDataLengthNotEqual(self, data, data_length):
        self.assertNotEqual(len(data), data_length)
        
    def assertTwoDataSetsLengthEqual(self, data_1, data_2):
        self.assertEqual(len(data_1), len(data_2))
        
    def assertTwoDataSetsContentEqual(self, data_1, data_2):

        data_1_list = [list(row.values()) for row in data_1]
        data_2_list = [list(row.values()) for row in data_2]

        data_1_diff_elements = [row for row in data_1_list if row not in data_2_list]
        data_2_diff_elements = [row for row in data_2_list if row not in data_1_list]

        self.assertDataLengthEqual(data_1_diff_elements, 0)
        self.assertDataLengthEqual(data_2_diff_elements, 0)


class ExportFileTestCase(unittest.TestCase):

    def assertExportedFileExists(self, file_path):
        self.assertTrue(os.path.exists(file_path))

    @staticmethod
    def delete_output(file_path):
        dirname = os.path.dirname(file_path)
        shutil.rmtree(dirname)


class BaseParserSetupTest(unittest.TestCase):
    def test_valid_pytz_time_zone(self):
        time_zone = 'Europe/Stockholm'
        try:
            baseparser = CampbellSCIBaseParser(pytz_time_zone=time_zone)
        except UnknownPytzTimeZoneError:
            self.fail("Exception raised unexpectedly")

    def test_invalid_pytz_time_zone(self):
        time_zone = 'Foo'
        with self.assertRaises(UnknownPytzTimeZoneError):
            baseparser = CampbellSCIBaseParser(pytz_time_zone=time_zone)


class BaseParserProcessDataTest(ReadDataTestCase):

    def test_data_generator(self):
        test_list = [1, 2, 3]
        baseparser = CampbellSCIBaseParser()

        self.assertEqual(tuple(baseparser._data_generator(test_list)), (1, 2, 3))

    def test_process_rows_generator_empty(self):
        file = os.path.join(THIS_DIR, 'testdata/all/csv_all_testdata_empty.dat')
        baseparser = CampbellSCIBaseParser()

        self.assertEqual(tuple(baseparser._process_rows(infile_path=file)), ())

    def test_process_rows_generator_exceeding_line_num(self):
        file = os.path.join(THIS_DIR, 'testdata/all/csv_all_testdata_empty.dat')
        baseparser = CampbellSCIBaseParser()

        self.assertEqual(tuple(baseparser._process_rows(infile_path=file, line_num=1)), ())

    def test_process_rows_generator_three_rows(self):
        file = os.path.join(THIS_DIR, 'testdata/base/csv_base_testdata_3_rows.dat')
        baseparser = CampbellSCIBaseParser()
        row_1 = OrderedDict([(0, '1')])
        row_2 = OrderedDict([(0, '1'), (1, '2')])
        row_3 = OrderedDict([(0, '1'), (1, '2'), (2, '3')])

        self.assertEqual(tuple(baseparser._process_rows(infile_path=file)), (row_1, row_2, row_3))

    def test_process_rows_generator_three_rows_slice(self):
        file = os.path.join(THIS_DIR, 'testdata/base/csv_base_testdata_3_rows.dat')
        baseparser = CampbellSCIBaseParser()
        row_2 = OrderedDict([(0, '1'), (1, '2')])
        row_3 = OrderedDict([(0, '1'), (1, '2'), (2, '3')])

        self.assertEqual(tuple(baseparser._process_rows(infile_path=file, line_num=1)), (row_2, row_3))

    def test_process_rows_generator_three_rows_headers(self):
        file = os.path.join(THIS_DIR, 'testdata/base/csv_base_testdata_3_rows.dat')
        baseparser = CampbellSCIBaseParser()
        headers = ['Label_' + str(i) for i in range(3)]
        row_1 = OrderedDict([('Label_0', '1')])
        row_2 = OrderedDict([('Label_0', '1'), ('Label_1', '2')])
        row_3 = OrderedDict([('Label_0', '1'), ('Label_1', '2'), ('Label_2', '3')])

        self.assertEqual(
            tuple(baseparser._process_rows(infile_path=file, headers=headers, line_num=0)), (row_1, row_2, row_3))

    def test_process_rows_generator_three_rows_indices(self):
        file = os.path.join(THIS_DIR, 'testdata/base/csv_base_testdata_3_rows.dat')
        baseparser = CampbellSCIBaseParser()

        row_1 = OrderedDict([(0, '1')])
        row_2 = OrderedDict([(0, '1'), (1, '2')])
        row_3 = OrderedDict([(0, '1'), (1, '2'), (2, '3')])

        self.assertEqual(
            tuple(baseparser._process_rows(infile_path=file, line_num=0)), (row_1, row_2, row_3))

    def test_process_rows_generator_three_rows_less_headers(self):
        file = os.path.join(THIS_DIR, 'testdata/base/csv_base_testdata_3_rows.dat')
        baseparser = CampbellSCIBaseParser()
        headers = ['Label_0']
        row_1 = OrderedDict([('Label_0', '1')])
        row_2 = row_1
        row_3 = row_1

        self.assertEqual(
            tuple(baseparser._process_rows(infile_path=file, headers=headers, line_num=0)), (row_1, row_2, row_3))

    def test_process_rows_generator_three_rows_headers_row(self):
        file = os.path.join(THIS_DIR, 'testdata/base/csv_base_testdata_3_rows_headers.dat')
        baseparser = CampbellSCIBaseParser()
        row_1 = OrderedDict([('Label_0', '1')])
        row_2 = OrderedDict([('Label_0', '1'), ('Label_1', '2')])
        row_3 = OrderedDict([('Label_0', '1'), ('Label_1', '2'), ('Label_2', '3')])

        self.assertEqual(
            tuple(baseparser._process_rows(infile_path=file, header_row=0, line_num=0)), (row_1, row_2, row_3))

    def test_read_rows_generator_three_rows(self):
        file = os.path.join(THIS_DIR, 'testdata/base/csv_base_testdata_3_rows.dat')
        baseparser = CampbellSCIBaseParser()
        row_1 = OrderedDict([(0, '1')])
        row_2 = OrderedDict([(0, '1'), (1, '2')])
        row_3 = OrderedDict([(0, '1'), (1, '2'), (2, '3')])

        self.assertEqual(tuple(baseparser._read_data(infile_path=file)), (row_1, row_2, row_3))

    def test_values_to_strings(self):
        row = OrderedDict([('Label_0', 'string'), ('Label_1', datetime(2016, 1, 1, 22, 30, 0)), ('Label_2', 15.7)])
        expected_row_values = ['string', '2016-01-01 22:30:00', '15.7']
        baseparser = CampbellSCIBaseParser()
        row_values_converted = baseparser._values_to_strings(row=row)

        self.assertListEqual(list(row_values_converted), expected_row_values)

    def test_read_data_length_empty(self):
        file = os.path.join(THIS_DIR, 'testdata/all/csv_all_testdata_empty.dat')
        baseparser = CampbellSCIBaseParser()
        data = baseparser.read_data(infile_path=file)

        self.assertDataLengthEqual(data=data, data_length=0)

    def test_read_data_ten_rows_length(self):
        file = os.path.join(THIS_DIR, 'testdata/base/csv_base_testdata_10_rows.dat')
        baseparser = CampbellSCIBaseParser()
        data = baseparser.read_data(infile_path=file)

        self.assertDataLengthEqual(data=data, data_length=10)

    def test_read_data_line_num_five_rows_length(self):
        file = os.path.join(THIS_DIR, 'testdata/base/csv_base_testdata_10_rows.dat')
        baseparser = CampbellSCIBaseParser()
        data = baseparser.read_data(infile_path=file, line_num=5)

        self.assertDataLengthEqual(data=data, data_length=5)

    def test_read_data_ten_rows_indices(self):
        file = os.path.join(THIS_DIR, 'testdata/base/csv_base_testdata_10_rows.dat')
        expected_headers = [i for i in range(10)]
        baseparser = CampbellSCIBaseParser()
        data = baseparser.read_data(infile_path=file, line_num=0)

        for row in data:
            for name, expected_name in zip(list(row.keys()), expected_headers):
                self.assertEqual(name, expected_name)

    def test_read_data_ten_rows_headers(self):
        file = os.path.join(THIS_DIR, 'testdata/base/csv_base_testdata_10_rows.dat')
        headers = ['Label_' + str(i) for i in range(10)]
        baseparser = CampbellSCIBaseParser()
        data = baseparser.read_data(infile_path=file, headers=headers, line_num=0)

        for row in data:
            for name, expected_name in zip(list(row.keys()), headers):
                self.assertEqual(name, expected_name)

    def test_read_data_three_rows_header_row(self):
        file = os.path.join(THIS_DIR, 'testdata/base/csv_base_testdata_3_rows_headers.dat')
        baseparser = CampbellSCIBaseParser()
        data = baseparser.read_data(infile_path=file, header_row=0, line_num=0)
        expected_headers = ['Label_' + str(i) for i in range(3)]

        for row in data:
            for name, expected_name in zip(list(row.keys()), expected_headers):
                self.assertEqual(name, expected_name)

    def test_read_data_convert_time_no_time_columns(self):
        file = os.path.join(THIS_DIR, 'testdata/all/csv_all_testdata_empty.dat')
        baseparser = CampbellSCIBaseParser()

        with self.assertRaises(TimeColumnValueError):
            baseparser.read_data(infile_path=file, convert_time=True)

    def test_read_data_convert_time(self):
        file = os.path.join(THIS_DIR, 'testdata/base/csv_base_testdata_1_row_time_no_tz.dat')
        time_zone = 'Europe/Stockholm'
        pytz_time_zone = pytz.timezone(time_zone)
        time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S']
        time_columns = [i for i in range(6)]
        expected_datetime = datetime(2016, 1, 1, 22, 30, 15, tzinfo=pytz_time_zone)

        baseparser = CampbellSCIBaseParser(pytz_time_zone=time_zone, time_format_args_library=time_format_args_library)
        data = baseparser.read_data(infile_path=file, convert_time=True, time_columns=time_columns)
        data_first_row = data[0]
        dt_first_row = data_first_row.get(0)

        self.assertEqual(dt_first_row, expected_datetime)

    def test_read_data_convert_time_to_utc(self):
        file = os.path.join(THIS_DIR, 'testdata/base/csv_base_testdata_1_row_time_no_tz.dat')
        time_zone = 'Europe/Stockholm'
        pytz_time_zone = pytz.timezone(time_zone)
        time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S']
        time_columns = [i for i in range(6)]
        expected_datetime = datetime(2016, 1, 1, 21, 30, 15, tzinfo=pytz.UTC)

        baseparser = CampbellSCIBaseParser(pytz_time_zone=time_zone, time_format_args_library=time_format_args_library)
        data = baseparser.read_data(infile_path=file, convert_time=True, time_columns=time_columns, to_utc=True)
        data_first_row = data[0]
        dt_first_row = data_first_row.get(0)

        self.assertEqual(dt_first_row, expected_datetime)

    def test_read_data_time_parsed_column_name(self):
        file = os.path.join(THIS_DIR, 'testdata/base/csv_base_testdata_1_row_time_no_tz.dat')
        time_zone = 'Europe/Stockholm'
        pytz_time_zone = pytz.timezone(time_zone)
        time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S']
        time_columns = [i for i in range(6)]
        expected_datetime = datetime(2016, 1, 1, 22, 30, 15, tzinfo=pytz_time_zone)
        expected_time_parsed_column_name = 'TIMESTAMP'

        baseparser = CampbellSCIBaseParser(pytz_time_zone=time_zone, time_format_args_library=time_format_args_library)

        data = baseparser.read_data(
            infile_path=file, convert_time=True, time_parsed_column=expected_time_parsed_column_name,
            time_columns=time_columns)

        data_first_row = data[0]
        time_parsed_column_name = list(data_first_row.keys())[0]

        self.assertEqual(time_parsed_column_name, expected_time_parsed_column_name)


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
            index = baseparser._find_first_time_column_key(headers, time_columns)

    def test_find_first_time_column_key_raise_index_error(self):
        baseparser = CampbellSCIBaseParser()
        headers = ['key_1', 'key_2', 'key_3']
        time_columns = []
        with self.assertRaises(IndexError):
            index = baseparser._find_first_time_column_key(headers, time_columns)

    def test_parse_custom_time_format_less_library_args(self):
        time_zone = 'Europe/Stockholm'
        baseparser = CampbellSCIBaseParser(pytz_time_zone=time_zone, time_format_args_library=[])
        parsing_info = baseparser._parse_custom_time_format('2016')
        parsed_time_format, parsed_time = parsing_info

        self.assertFalse(parsed_time_format)
        self.assertFalse(parsed_time)

    def test_parse_custom_time_format_less_time_values(self):
        time_zone = 'Europe/Stockholm'
        baseparser = CampbellSCIBaseParser(pytz_time_zone=time_zone, time_format_args_library=['%Y'])
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

        baseparser = CampbellSCIBaseParser(pytz_time_zone=time_zone, time_format_args_library=time_format_args_library)
        parsing_info = baseparser._parse_custom_time_format(*time_values_args)
        parsed_time_format, parsed_time = parsing_info

        self.assertEqual(parsed_time_format, expected_parsed_time_format)
        self.assertEqual(parsed_time, expected_parsed_time_values)

    def test_convert_time_from_data_row_already_time_zone_aware(self):
        file = os.path.join(THIS_DIR, 'testdata/base/csv_base_testdata_1_row_time.dat')
        time_zone = 'Etc/GMT-1'
        pytz_time_zone = pytz.timezone(time_zone)
        time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S', '%z']
        baseparser = CampbellSCIBaseParser(pytz_time_zone=time_zone, time_format_args_library=time_format_args_library)
        expected_datetime = datetime(2016, 1, 1, 22, 30, 15, tzinfo=pytz_time_zone)

        data = baseparser.read_data(infile_path=file)
        data_time_converted = baseparser.convert_time(data=data, time_columns=[0, 1, 2, 3, 4, 5, 6])
        data_time_converted_first_row = data_time_converted[0]
        data_time_converted_first_row_dt = data_time_converted_first_row.get(0)

        self.assertEqual(expected_datetime, data_time_converted_first_row_dt)

    def test_convert_time_from_data_row_not_already_time_zone_aware(self):
        file = os.path.join(THIS_DIR, 'testdata/base/csv_base_testdata_1_row_time_no_tz.dat')
        time_zone = 'Etc/GMT-1'
        pytz_time_zone = pytz.timezone(time_zone)
        time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S']
        baseparser = CampbellSCIBaseParser(pytz_time_zone=time_zone, time_format_args_library=time_format_args_library)
        expected_datetime = datetime(2016, 1, 1, 22, 30, 15, tzinfo=pytz_time_zone)

        data = baseparser.read_data(infile_path=file)
        data_time_converted = baseparser.convert_time(data=data, time_columns=[0, 1, 2, 3, 4, 5])
        data_time_converted_first_row = data_time_converted[0]
        data_time_converted_first_row_dt = data_time_converted_first_row.get(0)

        self.assertEqual(expected_datetime, data_time_converted_first_row_dt)

    def test_convert_time_from_data_row_with_column_name(self):
        file = os.path.join(THIS_DIR, 'testdata/base/csv_base_testdata_1_row_time.dat')
        time_zone = 'Etc/GMT-1'
        time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S', '%z']
        baseparser = CampbellSCIBaseParser(pytz_time_zone=time_zone, time_format_args_library=time_format_args_library)

        data = baseparser.read_data(infile_path=file)
        expected_time_parsed_column = "TIMESTAMP"
        data_time_converted = baseparser.convert_time(data=data, time_columns=[0, 1, 2, 3, 4, 5, 6],
                                                      time_parsed_column=expected_time_parsed_column)
        data_time_converted_first_row = data_time_converted[0]
        time_parsed_column_name = list(data_time_converted_first_row.keys())[0]

        self.assertEqual(time_parsed_column_name, expected_time_parsed_column)

    def test_convert_time_from_data_row_to_utc(self):
        file = os.path.join(THIS_DIR, 'testdata/base/csv_base_testdata_1_row_time.dat')
        time_zone = 'Etc/GMT-1'
        time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S', '%z']
        baseparser = CampbellSCIBaseParser(pytz_time_zone=time_zone, time_format_args_library=time_format_args_library)
        expected_datetime = datetime(2016, 1, 1, 21, 30, 15, tzinfo=pytz.UTC)

        data = baseparser.read_data(infile_path=file)
        data_time_converted = baseparser.convert_time(data=data, time_columns=[0, 1, 2, 3, 4, 5, 6], to_utc=True)
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
        baseparser = CampbellSCIBaseParser(pytz_time_zone=time_zone, time_format_args_library=time_format_args_library)

        with self.assertRaises(TimeParsingError):
            baseparser._parse_time_values(*time_values, ignore_parsing_error=False)

    def test_parse_time_values_ignore_parsing_error(self):
        time_zone = 'UTC'
        time_format_args_library = ['%NotParsable']
        time_values = ['2016-01-01 22:15:30']
        expected_dt = datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)

        baseparser = CampbellSCIBaseParser(pytz_time_zone=time_zone, time_format_args_library=time_format_args_library)

        parsed_dt = baseparser._parse_time_values(*time_values, ignore_parsing_error=True)

        self.assertEqual(parsed_dt, expected_dt)

    def test_parse_time_values_already_localized(self):
        time_zone = 'Etc/GMT-1'
        time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S', '%z']
        time_values = ['2016', '1', '1', '22', '15', '30', '+0100']
        expected_dt = datetime(2016, 1, 1, 22, 15, 30, tzinfo=pytz.timezone(time_zone))
        baseparser = CampbellSCIBaseParser(pytz_time_zone=time_zone, time_format_args_library=time_format_args_library)

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
        baseparser = CampbellSCIBaseParser(pytz_time_zone=time_zone, time_format_args_library=time_format_args_library)
        expected_dt = datetime(2016, 1, 1, 22, 15, 30, tzinfo=pytz.UTC)
        time_values = ['2016', '1', '1', '22', '15', '30']

        parsed_dt = baseparser._parse_time_values(*time_values)

        self.assertEqual(parsed_dt, expected_dt)

    def test_parse_time_values_to_utc(self):
        time_zone = 'Europe/Stockholm'
        time_format_args_library = ['%Y', '%m', '%d', '%H', '%M', '%S']
        baseparser = CampbellSCIBaseParser(pytz_time_zone=time_zone, time_format_args_library=time_format_args_library)
        expected_dt = datetime(2016, 1, 1, 21, 15, 30, tzinfo=pytz.UTC)
        time_values = ['2016', '1', '1', '22', '15', '30']

        parsed_dt = baseparser._parse_time_values(*time_values, to_utc=True)

        self.assertEqual(parsed_dt, expected_dt)


class BaseParserExportToCsvTest(ExportFileTestCase, ReadDataTestCase):

    def test_export_to_csv_file_created(self):
        file = os.path.join(THIS_DIR, 'testdata/base/csv_base_testdata_10_rows.dat')
        output_file = os.path.join(THIS_DIR, 'testoutput/test.dat')
        baseparser = CampbellSCIBaseParser()
        data = baseparser.read_data(infile_path=file)
        baseparser.export_to_csv(data=data, outfile_path=output_file)

        self.assertExportedFileExists(output_file)
        self.delete_output(output_file)

    def test_export_to_csv_file_content(self):
        file = os.path.join(THIS_DIR, 'testdata/base/csv_base_testdata_10_rows.dat')
        output_file = os.path.join(THIS_DIR, 'testoutput/test.dat')
        baseparser = CampbellSCIBaseParser()

        data_source_file = baseparser.read_data(infile_path=file)
        baseparser.export_to_csv(data=data_source_file, outfile_path=output_file)
        data_exported_file = baseparser.read_data(infile_path=output_file)

        self.assertTwoDataSetsLengthEqual(data_1=data_source_file, data_2=data_exported_file)
        self.delete_output(output_file)

    def test_export_to_csv_file_headers(self):
        file = os.path.join(THIS_DIR, 'testdata/base/csv_base_testdata_10_rows.dat')
        output_file = os.path.join(THIS_DIR, 'testoutput/test.dat')
        baseparser = CampbellSCIBaseParser()
        headers = ['Label_0', 'Label_1', 'Label_2']

        data_source_file = baseparser.read_data(infile_path=file, headers=headers)

        baseparser.export_to_csv(data=data_source_file, outfile_path=output_file, export_headers=True)
        data_exported_file = baseparser.read_data(infile_path=output_file, header_row=0)

        for row in data_exported_file:
            for exported_row_name, expected_row_name in zip(list(row.keys()), headers):
                self.assertEqual(exported_row_name, expected_row_name)

        self.delete_output(output_file)

    def test_export_to_csv_file_time_zone(self):
        time_zone = 'Europe/Stockholm'
        pytz_time_zone = pytz.timezone(time_zone)
        dt = datetime(2016, 1, 1, 22, 15, 30, tzinfo=pytz_time_zone)
        data = [OrderedDict([('Time', dt)])]

        output_file = os.path.join(THIS_DIR, 'testoutput/test.dat')
        baseparser = CampbellSCIBaseParser(pytz_time_zone=time_zone)

        baseparser.export_to_csv(data=data, outfile_path=output_file, export_headers=True, include_time_zone=True)
        data_exported_file = baseparser.read_data(infile_path=output_file, header_row=0)

        data_exported_file_first_row = data_exported_file[0]
        exported_time_str = data_exported_file_first_row.get('Time')
        exported_time_dt = datetime.strptime(exported_time_str, '%Y-%m-%d %H:%M:%S%z')

        self.assertEqual(exported_time_dt, dt)
        self.delete_output(output_file)


class BaseParserUpdateHeadersTest(ReadDataTestCase):

    def test_update_column_names(self):
        file = os.path.join(THIS_DIR, 'testdata/base/csv_base_testdata_3_rows.dat')
        baseparser = CampbellSCIBaseParser()
        headers = ['Label_' + str(i) for i in range(3)]

        data = baseparser.read_data(infile_path=file)
        data_updated_headers = baseparser.update_column_names(data=data, headers=headers)

        for row in data_updated_headers:
            self.assertListEqual(list(row.keys()), headers)

    def test_update_column_names_not_matching_row_lengths(self):
        file = os.path.join(THIS_DIR, 'testdata/base/csv_base_testdata_3_rows.dat')
        baseparser = CampbellSCIBaseParser()
        headers = ['Label_' + str(i) for i in range(3)]

        data = baseparser.read_data(infile_path=file)
        data_updated_headers = baseparser.update_column_names(data=data, headers=headers)

        for row in data_updated_headers:
            for updated_row_name, expected_row_name in zip(list(row.keys()), headers):
                self.assertEqual(updated_row_name, expected_row_name)

    def test_update_column_names_output_mismatched_rows(self):
        file = os.path.join(THIS_DIR, 'testdata/base/csv_base_testdata_3_rows.dat')
        baseparser = CampbellSCIBaseParser()
        headers = ['Label_' + str(i) for i in range(3)]

        data = baseparser.read_data(infile_path=file)

        data_updated_headers, data_mismatched_rows = baseparser.update_column_names(
            data=data, headers=headers, match_row_lengths=True, output_mismatched_rows=True)

        for row in data_mismatched_rows:
            self.assertDataLengthNotEqual(row, len(headers))


class CR10ParserTestCase(ReadDataTestCase):
    
    def assertFloatingPointsFixed(self, data_no_fp_fix, data_fp_fixed, replacements_lib):
        unprocessed = []
        for row_no_fp_fix, row_fp_fix in zip(data_no_fp_fix, data_fp_fixed):
            for value_no_fix, value_fix in zip(list(row_no_fp_fix.values()), list(row_fp_fix.values())):
                for source, replacement in replacements_lib.items():
                    if value_no_fix.startswith(source) and not value_fix.startswith(replacement):
                        unprocessed.append(value_no_fix)

        self.assertDataLengthEqual(unprocessed, 0)


class CR10ParserReadMixedDataTest(CR10ParserTestCase):

    def test_length_empty(self):
        file = os.path.join(THIS_DIR, 'testdata/all/csv_all_testdata_empty.dat')
        cr10 = CR10Parser()
        data = cr10.read_mixed_data(infile_path=file)

        self.assertDataLengthEqual(data=data, data_length=0)

    def test_length_ten_rows(self):
        file = os.path.join(THIS_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        cr10 = CR10Parser()
        data = cr10.read_mixed_data(infile_path=file)

        self.assertDataLengthEqual(data=data, data_length=10)

    def test_length_line_num_five_rows(self):
        file = os.path.join(THIS_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        cr10 = CR10Parser()
        data = cr10.read_mixed_data(infile_path=file, line_num=5)

        self.assertDataLengthEqual(data=data, data_length=5)

    def test_fix_floating_points(self):
        file = os.path.join(THIS_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        cr10 = CR10Parser()
        data_no_fp_fix = cr10.read_mixed_data(infile_path=file, fix_floats=False)
        data_fp_fixed = cr10.read_mixed_data(infile_path=file, fix_floats=True)
        replacements = {'.': '0.', '-.': '-0.'}

        self.assertFloatingPointsFixed(data_no_fp_fix=data_no_fp_fix, data_fp_fixed=data_fp_fixed, replacements_lib=replacements)


class CR10ParserReadArrayIdsDataTest(CR10ParserTestCase):

    def test_empty(self):
        file = os.path.join(THIS_DIR, 'testdata/all/csv_all_testdata_empty.dat')
        cr10 = CR10Parser()
        data = cr10.read_array_ids_data(infile_path=file)

        self.assertDataLengthEqual(data=data, data_length=0)

    def test_length_ten_rows(self):
        file = os.path.join(THIS_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        cr10 = CR10Parser()
        data = cr10.read_array_ids_data(infile_path=file)
        data_mixed = [row for array_id, array_id_data in data.items() for row in array_id_data]

        self.assertDataLengthEqual(data=data_mixed, data_length=10)

    def test_compare_length_ten_rows(self):
        file = os.path.join(THIS_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        cr10 = CR10Parser()
        data_mixed = cr10.read_mixed_data(infile_path=file)
        data_split = cr10.read_array_ids_data(infile_path=file)
        data_split_merged = [row for array_id, array_id_data in data_split.items() for row in array_id_data]

        self.assertTwoDataSetsLengthEqual(data_1=data_mixed, data_2=data_split_merged)

    def test_compare_data_ten_rows(self):
        file = os.path.join(THIS_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        cr10 = CR10Parser()
        data_mixed = cr10.read_mixed_data(infile_path=file)
        data_split = cr10.read_array_ids_data(infile_path=file)
        data_split_merged = [row for array_id, array_id_data in data_split.items() for row in array_id_data]

        self.assertTwoDataSetsLengthEqual(data_1=data_mixed, data_2=data_split_merged)

    def test_compare_data_ten_rows_lookup(self):
        file = os.path.join(THIS_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        array_ids_info = {'201': 'label_1', '203': 'label_2', '204': 'label_3', '210': 'label_4'}
        cr10 = CR10Parser()
        data_mixed = cr10.read_mixed_data(infile_path=file)
        data_split_translated = cr10.read_array_ids_data(infile_path=file, array_ids_info=array_ids_info)
        data_split_translated_merged = [row for array_id, array_id_data in data_split_translated.items() for row in array_id_data]

        self.assertTwoDataSetsLengthEqual(data_1=data_mixed, data_2=data_split_translated_merged)


class CR10ParserFilterDataTest(CR10ParserTestCase):

    def test_filter_array_ids_data_no_filter(self):
        file = os.path.join(THIS_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        cr10 = CR10Parser()
        data_mixed = cr10.read_mixed_data(infile_path=file)
        data_split = cr10.read_array_ids_data(infile_path=file)
        data_filtered = cr10.filter_data_by_array_ids(data=data_split)
        data_split_merged = [row for array_id, array_id_data in data_filtered.items() for row in array_id_data]

        self.assertTwoDataSetsLengthEqual(data_1=data_mixed, data_2=data_split_merged)

    def test_filter_array_ids_data_filter_all(self):
        file = os.path.join(THIS_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        cr10 = CR10Parser()
        data_mixed = cr10.read_mixed_data(infile_path=file)
        data_split = cr10.read_array_ids_data(infile_path=file)
        data_split_filtered = cr10.filter_data_by_array_ids(data_split, '201', '203', '204', '210')
        data_split_merged = [row for array_id, array_id_data in data_split_filtered.items() for row in array_id_data]

        self.assertTwoDataSetsLengthEqual(data_1=data_mixed, data_2=data_split_merged)

    def test_filter_array_ids_data_filter(self):
        file_unfiltered = os.path.join(THIS_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        file_filtered = os.path.join(THIS_DIR, 'testdata/cr10/csv_cr10_testdata_filtered_rows.dat')
        cr10 = CR10Parser()
        data_pre_filtered = cr10.read_mixed_data(infile_path=file_filtered)
        data_split_unfiltered = cr10.read_array_ids_data(infile_path=file_unfiltered)
        data_split_filtered = cr10.filter_data_by_array_ids(data_split_unfiltered, '201', '203')
        data_split_filtered_merged = [row for array_id, array_id_data in data_split_filtered.items() for row in array_id_data]

        self.assertTwoDataSetsLengthEqual(data_1=data_pre_filtered, data_2=data_split_filtered_merged)


class CR10ParserExportDataTest(CR10ParserTestCase, ExportFileTestCase):

    def test_export_array_ids_to_csv_empty_library(self):
        file = os.path.join(THIS_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        cr10 = CR10Parser()
        data_split = cr10.read_array_ids_data(infile_path=file)
        with self.assertRaises(ValueError):
            cr10.export_array_ids_to_csv(data=data_split, array_ids_info={})

    def test_export_array_ids_to_csv_empty_info(self):
        file = os.path.join(THIS_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        cr10 = CR10Parser()
        data_split = cr10.read_array_ids_data(infile_path=file)
        with self.assertRaises(ValueError):
            cr10.export_array_ids_to_csv(data=data_split, array_ids_info={'201': None})

    def test_export_array_ids_to_csv_no_file_path(self):
        file = os.path.join(THIS_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        cr10 = CR10Parser()
        data_split = cr10.read_array_ids_data(infile_path=file)
        with self.assertRaises(ValueError):
            cr10.export_array_ids_to_csv(data=data_split, array_ids_info={'201': {}})

    def test_export_array_ids_to_csv_content(self):
        file = os.path.join(THIS_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        output_file = os.path.join(THIS_DIR, 'testoutput/test.dat')
        cr10 = CR10Parser()
        data_split_unfiltered = cr10.read_array_ids_data(infile_path=file)
        data_split_filtered = cr10.filter_data_by_array_ids(data_split_unfiltered, '201')
        data_split_array_id_filtered = data_split_filtered.get('201')

        cr10.export_array_ids_to_csv(data=data_split_unfiltered, array_ids_info={'201': {'file_path': output_file}})
        data_exported_file = cr10.read_mixed_data(infile_path=output_file)

        self.assertTwoDataSetsContentEqual(data_1=data_split_array_id_filtered, data_2=data_exported_file)
        self.delete_output(output_file)

    def test_export_array_ids_to_csv_headers(self):
        file = os.path.join(THIS_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        output_file_1 = os.path.join(THIS_DIR, 'testoutput/test_1.dat')
        output_file_2 = os.path.join(THIS_DIR, 'testoutput/test_2.dat')
        cr10 = CR10Parser()
        data_split_unfiltered = cr10.read_array_ids_data(infile_path=file)
        data_split_filtered = cr10.filter_data_by_array_ids(data_split_unfiltered, '201', '203')

        data_split_filtered_201 = data_split_filtered.get('201')
        data_split_filtered_203 = data_split_filtered.get('203')

        data_split_filtered_201_first_row = data_split_filtered_201[0]
        data_split_filtered_203_first_row = data_split_filtered_203[0]

        data_split_filtered_201_headers = [str(key) for key in data_split_filtered_201_first_row.keys()]
        data_split_filtered_203_headers = [str(key) for key in data_split_filtered_203_first_row.keys()]

        array_id_info_1 = {'file_path': output_file_1}
        array_id_info_2 = {'file_path': output_file_2}
        array_id_info = {'201': array_id_info_1, '203': array_id_info_2}

        cr10.export_array_ids_to_csv(data=data_split_filtered, array_ids_info=array_id_info, export_headers=True)
        data_exported_file_1 = cr10.read_mixed_data(infile_path=output_file_1)
        data_exported_file_2 = cr10.read_mixed_data(infile_path=output_file_2)

        data_exported_file_1_headers_row = data_exported_file_1[0]
        data_exported_file_1_headers = [value for value in data_exported_file_1_headers_row.values()]

        data_exported_file_2_headers_row = data_exported_file_2[0]
        data_exported_file_2_headers = [value for value in data_exported_file_2_headers_row.values()]

        self.assertListEqual(data_split_filtered_201_headers, data_exported_file_1_headers)
        self.assertListEqual(data_split_filtered_203_headers, data_exported_file_2_headers)
        self.delete_output(output_file_1)

    def test_export_array_ids_to_csv_updated_headers(self):
        file = os.path.join(THIS_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        output_file = os.path.join(THIS_DIR, 'testoutput/test.dat')
        cr10 = CR10Parser()

        data_split_unfiltered = cr10.read_array_ids_data(infile_path=file)
        data_split_filtered = cr10.filter_data_by_array_ids(data_split_unfiltered, '203')
        data_split_array_id_filtered = data_split_filtered.get('203')

        headers = ['Array_Id', 'Year', 'Day', 'Hour/Minute', 'Wind_Speed', 'Wind_Direction']
        data_updated_headers = cr10.update_column_names(data=data_split_array_id_filtered, headers=headers)
        array_ids_info = {'203': {'file_path': output_file}}
        cr10.export_array_ids_to_csv(data=data_updated_headers, array_ids_info=array_ids_info, export_headers=True)

        data_exported_file = cr10.read_mixed_data(infile_path=output_file)
        data_headers_updated_first_row = data_exported_file[0]
        data_headers_updated_headers = [value for value in data_headers_updated_first_row.values()]

        self.assertListEqual(headers, data_headers_updated_headers)
        self.delete_output(output_file)


class ConvertTimeTest(ExportFileTestCase):

    def test_export_to_csv_time_converted_data_no_time_zone(self):
        file = os.path.join(THIS_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        output_file = os.path.join(THIS_DIR, 'testoutput/test.dat')
        array_ids_info = {'201': 'label_1'}
        expected_datetime = datetime(2012, 11, 25, 22, 0, 0)
        expected_datetime_string = str(expected_datetime)

        cr10 = CR10Parser()
        data = cr10.read_array_ids_data(infile_path=file, array_ids_info=array_ids_info)
        data_time_converted = cr10.convert_time(data=data.get('label_1'), time_columns=[1, 2, 3])

        array_ids_export_info = {'201': {'file_path': output_file}}
        cr10.export_array_ids_to_csv(data=data_time_converted, array_ids_info=array_ids_export_info)
        data_exported_file = cr10.read_mixed_data(infile_path=output_file)
        data_time_converted_first_row = data_exported_file[0]
        exported_datetime_string = data_time_converted_first_row.get(1)

        self.assertEqual(expected_datetime_string, exported_datetime_string)
        self.delete_output(output_file)

    def test_export_to_csv_time_converted_data_time_zone(self):
        file = os.path.join(THIS_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        output_file = os.path.join(THIS_DIR, 'testoutput/test.dat')
        array_ids_info = {'201': 'label_1'}
        expected_datetime = datetime(2012, 11, 25, 22, 0, 0, tzinfo=pytz.UTC)
        expected_datetime_string = expected_datetime.strftime('%Y-%m-%d %H:%M:%S%z')
        cr10 = CR10Parser()

        data = cr10.read_array_ids_data(infile_path=file, array_ids_info=array_ids_info)
        data_time_converted = cr10.convert_time(data=data.get('label_1'), time_columns=[1, 2, 3])

        array_ids_export_info = {'201': {'file_path': output_file}}
        cr10.export_array_ids_to_csv(data=data_time_converted, array_ids_info=array_ids_export_info,
                                     include_time_zone=True)
        data_exported_file = cr10.read_mixed_data(infile_path=output_file)
        data_time_converted_first_row = data_exported_file[0]
        exported_datetime_string = data_time_converted_first_row.get(1)

        self.assertEqual(expected_datetime_string, exported_datetime_string)
        self.delete_output(output_file)


class CR10ParserConvertTimeTest(ConvertTimeTest):

    def test_convert_time(self):
        time_zone = 'UTC'
        year = '16'
        day = '30'
        hour_minute = '2230'
        parsed_time_expected = datetime(2016, 1, 30, 22, 30, 0, tzinfo=pytz.UTC)

        cr10 = CR10Parser(time_zone)
        parsed_time = cr10._parse_time_values(*[year, day, hour_minute], to_utc=False)

        self.assertEqual(parsed_time, parsed_time_expected)

    def test_convert_time_to_utc(self):
        time_zone = 'Europe/Stockholm'
        year = '16'
        day = '30'
        hour_minute = '2230'
        parsed_time_expected = datetime(2016, 1, 30, 21, 30, 0, tzinfo=pytz.UTC)

        cr10 = CR10Parser(time_zone)
        parsed_time = cr10._parse_time_values(*[year, day, hour_minute], to_utc=True)

        self.assertEqual(parsed_time, parsed_time_expected)
        

class CR10XParserConvertTimeTest(ConvertTimeTest):

    def test_convert_time(self):
        time_zone = 'UTC'
        year = '2016'
        day = '30'
        hour_minute = '2230'
        parsed_time_expected = datetime(2016, 1, 30, 22, 30, 0, tzinfo=pytz.UTC)

        cr10x = CR10XParser(time_zone)
        parsed_time = cr10x._parse_time_values(*[year, day, hour_minute], to_utc=False)

        self.assertEqual(parsed_time, parsed_time_expected)

