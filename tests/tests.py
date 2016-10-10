#!/usr/bin/env
# -*- coding: utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytz
import shutil
import unittest

from datetime import datetime

from campbellsciparser import device


THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class ReadDataTestCase(unittest.TestCase):

    def assertDataLengthEqual(self, data, data_length):
        self.assertEqual(len(data), data_length)

    def assertFloatingPointsFixed(self, data_no_fp_fix, data_fp_fixed, replacements_lib):
        unprocessed = []
        for row_no_fp_fix, row_fp_fix in zip(data_no_fp_fix, data_fp_fixed):
            for value_no_fix, value_fix in zip(list(row_no_fp_fix.values()), list(row_fp_fix.values())):
                for source, replacement in replacements_lib.items():
                    if value_no_fix.startswith(source) and not value_fix.startswith(replacement):
                        unprocessed.append(value_no_fix)

        self.assertDataLengthEqual(unprocessed, 0)

    def assertExportedFileExists(self, file_path):
        self.assertTrue(os.path.exists(file_path))
        self.delete_output(file_path)

    def delete_output(self, file_path):
        dirname = os.path.dirname(file_path)
        shutil.rmtree(dirname)


class ReadArrayIdsDataTestCase(ReadDataTestCase):

    def assertDataLengthEqual(self, data_1, data_2):
        self.assertEqual(len(data_1), len(data_2))

    def assertDataContentEqual(self, data_1, data_2):

        data_1_list = [list(row.values()) for row in data_1]
        data_2_list = [list(row.values()) for row in data_2]

        data_1_diff_elements = [row for row in data_1_list if row not in data_2_list]
        data_2_diff_elements = [row for row in data_2_list if row not in data_1_list]

        ReadDataTestCase.assertDataLengthEqual(self, data_1_diff_elements, 0)
        ReadDataTestCase.assertDataLengthEqual(self, data_2_diff_elements, 0)


class ReadMixedDataTest(ReadDataTestCase):

    def test_length_empty(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_empty.dat')
        cr10x = device.CR10XParser()
        data = cr10x.read_mixed_data(infile_path=file)

        self.assertDataLengthEqual(data=data, data_length=0)

    def test_length_ten_rows(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        cr10x = device.CR10XParser()
        data = cr10x.read_mixed_data(infile_path=file)

        self.assertDataLengthEqual(data=data, data_length=10)

    def test_length_line_num_five_rows(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        cr10x = device.CR10XParser()
        data = cr10x.read_mixed_data(infile_path=file, line_num=5)

        self.assertDataLengthEqual(data=data, data_length=5)

    def test_fix_floating_points(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        cr10x = device.CR10XParser()
        data_no_fp_fix = cr10x.read_mixed_data(infile_path=file, fix_floats=False)
        data_fp_fixed = cr10x.read_mixed_data(infile_path=file, fix_floats=True)
        replacements = {'.': '0.', '-.': '-0.'}

        self.assertFloatingPointsFixed(data_no_fp_fix=data_no_fp_fix, data_fp_fixed=data_fp_fixed, replacements_lib=replacements)


class ReadArrayIdsDataTest(ReadArrayIdsDataTestCase):

    def test_empty(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_empty.dat')
        cr10x = device.CR10XParser()
        data = cr10x.read_array_ids_data(infile_path=file)

        ReadDataTestCase.assertDataLengthEqual(self, data=data, data_length=0)

    def test_length_ten_rows(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        cr10x = device.CR10XParser()
        data = cr10x.read_array_ids_data(infile_path=file)
        data_mixed = [row for array_id, array_id_data in data.items() for row in array_id_data]

        ReadDataTestCase.assertDataLengthEqual(self, data=data_mixed, data_length=10)

    def test_compare_length_ten_rows(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        cr10x = device.CR10XParser()
        data_mixed = cr10x.read_mixed_data(infile_path=file)
        data_split = cr10x.read_array_ids_data(infile_path=file)
        data_split_merged = [row for array_id, array_id_data in data_split.items() for row in array_id_data]

        ReadArrayIdsDataTestCase.assertDataLengthEqual(self, data_1=data_mixed, data_2=data_split_merged)

    def test_compare_data_ten_rows(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        cr10x = device.CR10XParser()
        data_mixed = cr10x.read_mixed_data(infile_path=file)
        data_split = cr10x.read_array_ids_data(infile_path=file)
        data_split_merged = [row for array_id, array_id_data in data_split.items() for row in array_id_data]

        self.assertDataContentEqual(data_1=data_mixed, data_2=data_split_merged)

    def test_compare_data_ten_rows_lookup(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        array_ids_info = {'201': 'label_1', '203': 'label_2', '204': 'label_3', '210': 'label_4'}
        cr10x = device.CR10XParser()
        data_mixed = cr10x.read_mixed_data(infile_path=file)
        data_split_translated = cr10x.read_array_ids_data(infile_path=file, array_ids_info=array_ids_info)
        data_split_translated_merged = [row for array_id, array_id_data in data_split_translated.items() for row in array_id_data]

        self.assertDataContentEqual(data_1=data_mixed, data_2=data_split_translated_merged)


class FilterDataTest(ReadArrayIdsDataTestCase):

    def test_filter_array_ids_data_no_filter(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        cr10x = device.CR10XParser()
        data_mixed = cr10x.read_mixed_data(infile_path=file)
        data_split = cr10x.read_array_ids_data(infile_path=file)
        data_filtered = cr10x.filter_data_by_array_ids(data=data_split)
        data_split_merged = [row for array_id, array_id_data in data_filtered.items() for row in array_id_data]

        self.assertDataContentEqual(data_1=data_mixed, data_2=data_split_merged)

    def test_filter_array_ids_data_filter_all(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        cr10x = device.CR10XParser()
        data_mixed = cr10x.read_mixed_data(infile_path=file)
        data_split = cr10x.read_array_ids_data(infile_path=file)
        data_split_filtered = cr10x.filter_data_by_array_ids('201', '203', '204', '210', data=data_split)
        data_split_merged = [row for array_id, array_id_data in data_split_filtered.items() for row in array_id_data]

        self.assertDataContentEqual(data_1=data_mixed, data_2=data_split_merged)

    def test_filter_array_ids_data_filter(self):
        file_unfiltered = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        file_filtered = os.path.join(THIS_DIR, 'testdata/csv_testdata_filtered_rows.dat')
        cr10x = device.CR10XParser()
        data_pre_filtered = cr10x.read_mixed_data(infile_path=file_filtered)
        data_split_unfiltered = cr10x.read_array_ids_data(infile_path=file_unfiltered)
        data_split_filtered = cr10x.filter_data_by_array_ids('201', '203', data=data_split_unfiltered)
        data_split_filtered_merged = [row for array_id, array_id_data in data_split_filtered.items() for row in array_id_data]

        self.assertDataContentEqual(data_1=data_pre_filtered, data_2=data_split_filtered_merged)


class ExportDataTest(ReadArrayIdsDataTestCase):

    def test_export_to_csv_file_created(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        output_file = os.path.join(THIS_DIR, 'testoutput/test.dat')
        cr10x = device.CR10XParser()
        data_mixed = cr10x.read_mixed_data(infile_path=file)
        cr10x.export_to_csv(data=data_mixed, outfile_path=output_file)

        self.assertExportedFileExists(output_file)

    def test_export_to_csv_file_content(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        output_file = os.path.join(THIS_DIR, 'testoutput/test.dat')
        cr10x = device.CR10XParser()
        data_source_file_mixed = cr10x.read_mixed_data(infile_path=file)
        cr10x.export_to_csv(data=data_source_file_mixed, outfile_path=output_file)
        data_exported_file_mixed = cr10x.read_mixed_data(infile_path=output_file)

        self.assertDataContentEqual(data_1=data_source_file_mixed, data_2=data_exported_file_mixed)
        self.delete_output(output_file)

    def test_export_to_csv_file_headers(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        output_file = os.path.join(THIS_DIR, 'testoutput/test.dat')
        cr10x = device.CR10XParser()
        data_source_file_mixed = cr10x.read_mixed_data(infile_path=file)
        data_source_file_mixed_first_row = data_source_file_mixed[0]
        data_source_file_mixed_headers = [str(key) for key in data_source_file_mixed_first_row.keys()]

        cr10x.export_to_csv(data=data_source_file_mixed, outfile_path=output_file, export_headers=True)
        data_exported_file_mixed = cr10x.read_mixed_data(infile_path=output_file)
        data_exported_file_headers_row = data_exported_file_mixed[0]
        data_exported_file_headers = [value for value in data_exported_file_headers_row.values()]

        self.assertListEqual(data_source_file_mixed_headers, data_exported_file_headers)
        self.delete_output(output_file)

    def test_export_array_ids_to_csv_empty_library(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        cr10x = device.CR10XParser()
        data_split = cr10x.read_array_ids_data(infile_path=file)
        with self.assertRaises(ValueError):
            cr10x.export_array_ids_to_csv(data=data_split, array_ids_info={})

    def test_export_array_ids_to_csv_empty_info(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        cr10x = device.CR10XParser()
        data_split = cr10x.read_array_ids_data(infile_path=file)
        with self.assertRaises(ValueError):
            cr10x.export_array_ids_to_csv(data=data_split, array_ids_info={'201': None})

    def test_export_array_ids_to_csv_no_file_path(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        cr10x = device.CR10XParser()
        data_split = cr10x.read_array_ids_data(infile_path=file)
        with self.assertRaises(ValueError):
            cr10x.export_array_ids_to_csv(data=data_split, array_ids_info={'201': {}})

    def test_export_array_ids_to_csv_content(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        output_file = os.path.join(THIS_DIR, 'testoutput/test.dat')
        cr10x = device.CR10XParser()
        data_split_unfiltered = cr10x.read_array_ids_data(infile_path=file)
        data_split_filtered = cr10x.filter_data_by_array_ids('201', data=data_split_unfiltered)
        data_split_array_id_filtered = data_split_filtered.get('201')

        cr10x.export_array_ids_to_csv(data=data_split_unfiltered, array_ids_info={'201': {'file_path': output_file}})
        data_exported_file = cr10x.read_mixed_data(infile_path=output_file)

        self.assertDataContentEqual(data_1=data_split_array_id_filtered, data_2=data_exported_file)
        self.delete_output(output_file)

    def test_export_array_ids_to_csv_headers(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        output_file_1 = os.path.join(THIS_DIR, 'testoutput/test_1.dat')
        output_file_2 = os.path.join(THIS_DIR, 'testoutput/test_2.dat')
        cr10x = device.CR10XParser()
        data_split_unfiltered = cr10x.read_array_ids_data(infile_path=file)
        data_split_filtered = cr10x.filter_data_by_array_ids('201', '203', data=data_split_unfiltered)

        data_split_filtered_201 = data_split_filtered.get('201')
        data_split_filtered_203 = data_split_filtered.get('203')

        data_split_filtered_201_first_row = data_split_filtered_201[0]
        data_split_filtered_203_first_row = data_split_filtered_203[0]

        data_split_filtered_201_headers = [str(key) for key in data_split_filtered_201_first_row.keys()]
        data_split_filtered_203_headers = [str(key) for key in data_split_filtered_203_first_row.keys()]

        array_id_info_1 = {'file_path': output_file_1}
        array_id_info_2 = {'file_path': output_file_2}
        array_id_info = {'201': array_id_info_1, '203': array_id_info_2}

        cr10x.export_array_ids_to_csv(data=data_split_filtered, array_ids_info=array_id_info, export_headers=True)
        data_exported_file_1 = cr10x.read_mixed_data(infile_path=output_file_1)
        data_exported_file_2 = cr10x.read_mixed_data(infile_path=output_file_2)

        data_exported_file_1_headers_row = data_exported_file_1[0]
        data_exported_file_1_headers = [value for value in data_exported_file_1_headers_row.values()]

        data_exported_file_2_headers_row = data_exported_file_2[0]
        data_exported_file_2_headers = [value for value in data_exported_file_2_headers_row.values()]

        self.assertListEqual(data_split_filtered_201_headers, data_exported_file_1_headers)
        self.assertListEqual(data_split_filtered_203_headers, data_exported_file_2_headers)
        self.delete_output(output_file_1)

    def test_export_array_ids_to_csv_updated_headers(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        output_file = os.path.join(THIS_DIR, 'testoutput/test.dat')
        cr10x = device.CR10XParser()

        data_split_unfiltered = cr10x.read_array_ids_data(infile_path=file)
        data_split_filtered = cr10x.filter_data_by_array_ids('203', data=data_split_unfiltered)
        data_split_array_id_filtered = data_split_filtered.get('203')

        headers = ['Array_Id', 'Year', 'Day', 'Hour/Minute', 'Wind_Speed', 'Wind_Direction']
        data_headers_updated = cr10x.update_headers(data=data_split_array_id_filtered, headers=headers)
        array_ids_info = {'203': {'file_path': output_file}}
        cr10x.export_array_ids_to_csv(data=data_headers_updated, array_ids_info=array_ids_info, export_headers=True)

        data_exported_file = cr10x.read_mixed_data(infile_path=output_file)
        data_headers_updated_first_row = data_exported_file[0]
        data_headers_updated_headers = [value for value in data_headers_updated_first_row.values()]

        self.assertListEqual(headers, data_headers_updated_headers)
        self.delete_output(output_file)


class ConvertTimeTest(ReadDataTestCase):

    def test_convert_cr10x_time(self):
        time_zone = 'UTC'
        year = '2016'
        day = '30'
        hour_minute = '2230'
        parsed_time_expected = datetime(2016, 1, 30, 22, 30, 0, tzinfo=pytz.UTC)

        cr10x = device.CR10XParser(time_zone)
        parsed_time = cr10x.parse_time_values(*[year, day, hour_minute], to_utc=False)

        self.assertEqual(parsed_time, parsed_time_expected)

    def test_convert_cr10x_time_to_utc(self):
        time_zone = 'Europe/Stockholm'
        year = '2016'
        day = '30'
        hour_minute = '2230'
        parsed_time_expected = datetime(2016, 1, 30, 21, 30, 0, tzinfo=pytz.UTC)

        cr10x = device.CR10XParser(time_zone)
        parsed_time = cr10x.parse_time_values(*[year, day, hour_minute], to_utc=True)

        self.assertEqual(parsed_time, parsed_time_expected)

    def test_convert_cr10x_time_from_row(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        array_ids_info = {'201': 'label_1'}
        expected_datetime = datetime(2012, 11, 25, 22, 0, 0, tzinfo=pytz.UTC)
        cr10x = device.CR10XParser()

        data = cr10x.read_array_ids_data(infile_path=file, array_ids_info=array_ids_info)
        data_time_converted = cr10x.convert_time(data=data.get('label_1'), time_columns=[1, 2, 3])
        data_time_converted_first_row = data_time_converted[0]
        data_time_converted_first_row_dt = data_time_converted_first_row.get(1)

        self.assertEqual(expected_datetime, data_time_converted_first_row_dt)

    def test_convert_cr10x_time_from_row_with_column_name(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        array_ids_info = {'201': 'label_1'}
        expected_datetime = datetime(2012, 11, 25, 22, 0, 0, tzinfo=pytz.UTC)
        cr10x = device.CR10XParser()

        data = cr10x.read_array_ids_data(infile_path=file, array_ids_info=array_ids_info)
        data_time_converted = cr10x.convert_time(data=data.get('label_1'), time_columns=[1, 2, 3],
                                                 time_parsed_column="TIMESTAMP")
        data_time_converted_first_row = data_time_converted[0]
        data_time_converted_first_row_dt = data_time_converted_first_row.get("TIMESTAMP")

        self.assertEqual(expected_datetime, data_time_converted_first_row_dt)

    def test_export_to_csv_time_converted_data_no_time_zone(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        output_file = os.path.join(THIS_DIR, 'testoutput/test.dat')
        array_ids_info = {'201': 'label_1'}
        expected_datetime = datetime(2012, 11, 25, 22, 0, 0)
        expected_datetime_string = str(expected_datetime)

        cr10x = device.CR10XParser()
        data = cr10x.read_array_ids_data(infile_path=file, array_ids_info=array_ids_info)
        data_time_converted = cr10x.convert_time(data=data.get('label_1'), time_columns=[1, 2, 3])

        array_ids_export_info = {'201': {'file_path': output_file}}
        cr10x.export_array_ids_to_csv(data=data_time_converted, array_ids_info=array_ids_export_info)
        data_exported_file = cr10x.read_mixed_data(infile_path=output_file)
        data_time_converted_first_row = data_exported_file[0]
        exported_datetime_string = data_time_converted_first_row.get(1)

        self.assertEqual(expected_datetime_string, exported_datetime_string)
        self.delete_output(output_file)

    def test_export_to_csv_time_converted_data_time_zone(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        output_file = os.path.join(THIS_DIR, 'testoutput/test.dat')
        array_ids_info = {'201': 'label_1'}
        expected_datetime = datetime(2012, 11, 25, 22, 0, 0, tzinfo=pytz.UTC)
        expected_datetime_string = str(expected_datetime)
        cr10x = device.CR10XParser()

        data = cr10x.read_array_ids_data(infile_path=file, array_ids_info=array_ids_info)
        data_time_converted = cr10x.convert_time(data=data.get('label_1'), time_columns=[1, 2, 3])

        array_ids_export_info = {'201': {'file_path': output_file}}
        cr10x.export_array_ids_to_csv(data=data_time_converted, array_ids_info=array_ids_export_info,
                                      include_time_zone=True)
        data_exported_file = cr10x.read_mixed_data(infile_path=output_file)
        data_time_converted_first_row = data_exported_file[0]
        exported_datetime_string = data_time_converted_first_row.get(1)

        self.assertEqual(expected_datetime_string, exported_datetime_string)
        self.delete_output(output_file)


class HeadersTest(ReadArrayIdsDataTestCase):

    def test_update_headers(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        cr10x = device.CR10XParser()
        headers = ['Array_Id', 'Year', 'Day', 'Hour/Minute', 'Wind_Speed', 'Wind_Direction']

        data_split_unfiltered = cr10x.read_array_ids_data(infile_path=file)
        data_split_filtered = cr10x.filter_data_by_array_ids('203', data=data_split_unfiltered)
        data_split_array_id_filtered = data_split_filtered.get('203')

        data_headers_updated = cr10x.update_headers(data=data_split_array_id_filtered, headers=headers)
        data_headers_updated_first_row = data_headers_updated[0]
        data_headers_updated_headers = [key for key in data_headers_updated_first_row.keys()]

        self.assertListEqual(headers, data_headers_updated_headers)

    #def test_update_headers_output_mismatched_rows(self):