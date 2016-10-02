#!/usr/bin/env
# -*- coding: utf-8 -*-

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import shutil
import unittest

import pytz

from datetime import datetime

from campbellsciparser import device, timeparser

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class ReadDataTestCase(unittest.TestCase):

    def assertDataLengthEqual(self, data, data_length):
        self.assertEqual(len(data), data_length)

    def assertFloatingPointsFixed(self, data_no_fp_fix, data_fp_fixed, replacements_lib):
        unprocessed = []
        for row_no_fp_fix, row_fp_fix in zip(data_no_fp_fix, data_fp_fixed):
            for value_no_fix, value_fix in zip(row_no_fp_fix, row_fp_fix):
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
        data_1_diff_elements = [row for row in data_1 if row not in data_2]
        ReadDataTestCase.assertDataLengthEqual(self, data_1_diff_elements, 0)
        data_2_diff_elements = [row for row in data_2 if row not in data_1]
        ReadDataTestCase.assertDataLengthEqual(self, data_2_diff_elements, 0)


class ReadMixedDataTest(ReadDataTestCase):

    def test_length_empty(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_empty.dat')
        data = device.CR10X.read_mixed_data(infile_path=file)

        self.assertDataLengthEqual(data=data, data_length=0)

    def test_length_ten_rows(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        data = device.CR10X.read_mixed_data(infile_path=file)

        self.assertDataLengthEqual(data=data, data_length=10)

    def test_length_line_num_five_rows(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        data = device.CR10X.read_mixed_data(infile_path=file, line_num=5)

        self.assertDataLengthEqual(data=data, data_length=5)

    def test_fix_floating_points(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        data_no_fp_fix = device.CR10X.read_mixed_data(infile_path=file, fix_floats=False)
        data_fp_fixed = device.CR10X.read_mixed_data(infile_path=file, fix_floats=True)
        replacements = {'.': '0.', '-.': '-0.'}

        self.assertFloatingPointsFixed(data_no_fp_fix=data_no_fp_fix, data_fp_fixed=data_fp_fixed, replacements_lib=replacements)


class ReadArrayIdsDataTest(ReadArrayIdsDataTestCase):

    def test_empty(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_empty.dat')
        data = device.CR10X.read_array_ids_data(infile_path=file)

        ReadDataTestCase.assertDataLengthEqual(self, data=data, data_length=0)

    def test_length_ten_rows(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        data = device.CR10X.read_array_ids_data(infile_path=file)
        data_mixed = [row for array_id, array_id_data in data.items() for row in array_id_data]

        ReadDataTestCase.assertDataLengthEqual(self, data=data_mixed, data_length=10)

    def test_compare_length_ten_rows(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        data_mixed = device.CR10X.read_mixed_data(infile_path=file)
        data_split = device.CR10X.read_array_ids_data(infile_path=file)
        data_split_merged = [row for array_id, array_id_data in data_split.items() for row in array_id_data]

        ReadArrayIdsDataTestCase.assertDataLengthEqual(self, data_1=data_mixed, data_2=data_split_merged)

    def test_compare_data_ten_rows(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        data_mixed = device.CR10X.read_mixed_data(infile_path=file)
        data_split = device.CR10X.read_array_ids_data(infile_path=file)
        data_split_merged = [row for array_id, array_id_data in data_split.items() for row in array_id_data]

        self.assertDataContentEqual(data_1=data_mixed, data_2=data_split_merged)

    def test_compare_data_ten_rows_lookup(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        array_ids_info = {'201': 'label_1', '203': 'label_2', '204': 'label_3', '210': 'label_4'}
        data_mixed = device.CR10X.read_mixed_data(infile_path=file)
        data_split_translated = device.CR10X.read_array_ids_data(infile_path=file, array_ids_info=array_ids_info)
        data_split_translated_merged = [row for array_id, array_id_data in data_split_translated.items() for row in array_id_data]

        self.assertDataContentEqual(data_1=data_mixed, data_2=data_split_translated_merged)


class FilterDataTest(ReadArrayIdsDataTestCase):

    def test_filter_array_ids_data_no_filter(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        data_mixed = device.CR10X.read_mixed_data(infile_path=file)
        data_split = device.CR10X.read_array_ids_data(infile_path=file)
        data_filtered = device.CR10X.filter_data_by_array_ids(data=data_split)
        data_split_merged = [row for array_id, array_id_data in data_filtered.items() for row in array_id_data]

        self.assertDataContentEqual(data_1=data_mixed, data_2=data_split_merged)

    def test_filter_array_ids_data_filter_all(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        data_mixed = device.CR10X.read_mixed_data(infile_path=file)
        data_split = device.CR10X.read_array_ids_data(infile_path=file)
        data_split_filtered = device.CR10X.filter_data_by_array_ids('201', '203', '204', '210', data=data_split)
        data_split_merged = [row for array_id, array_id_data in data_split_filtered.items() for row in array_id_data]

        self.assertDataContentEqual(data_1=data_mixed, data_2=data_split_merged)

    def test_filter_array_ids_data_filter(self):
        file_unfiltered = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        file_filtered = os.path.join(THIS_DIR, 'testdata/csv_testdata_filtered_rows.dat')
        data_pre_filtered = device.CR10X.read_mixed_data(infile_path=file_filtered)
        data_split_unfiltered = device.CR10X.read_array_ids_data(infile_path=file_unfiltered)
        data_split_filtered = device.CR10X.filter_data_by_array_ids('201', '203', data=data_split_unfiltered)
        data_split_filtered_merged = [row for array_id, array_id_data in data_split_filtered.items() for row in array_id_data]

        self.assertDataContentEqual(data_1=data_pre_filtered, data_2=data_split_filtered_merged)


class ExportDataTest(ReadArrayIdsDataTestCase):

    def test_export_to_csv_file_created(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        output_file = os.path.join(THIS_DIR, 'testoutput/test.dat')
        data_mixed = device.CR10X.read_mixed_data(infile_path=file)
        device.CR10X.export_to_csv(data=data_mixed, outfile_path=output_file)

        self.assertExportedFileExists(output_file)

    def test_export_to_csv_file_content(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        output_file = os.path.join(THIS_DIR, 'testoutput/test.dat')
        data_source_file_mixed = device.CR10X.read_mixed_data(infile_path=file)
        device.CR10X.export_to_csv(data=data_source_file_mixed, outfile_path=output_file)
        data_exported_file_mixed = device.CR10X.read_mixed_data(infile_path=output_file)

        self.assertDataContentEqual(data_1=data_source_file_mixed, data_2=data_exported_file_mixed)
        self.delete_output(output_file)

    def test_export_to_csv_file_header(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        output_file = os.path.join(THIS_DIR, 'testoutput/test.dat')
        data_source_file_mixed = device.CR10X.read_mixed_data(infile_path=file)

        headers = ["Array_Id","Year_RTM","Day_RTM","Hour_Minute_RTM","Tot_rad_AVG","Air_Temp2_AVG","Air_Temp1",
                  "Humidity_AVG","Wind_spd_S_WVT","Wind_spd_U_WVT","Wind_dir_DU_WVT","Wind_dir_SDU_WVT",
                  "Wind_spd3_AVG","BadTemp_AVG","PAR_AVG","Air_Pres_AVG"]

        device.CR10X.export_to_csv(data=data_source_file_mixed, outfile_path=output_file, headers=headers)
        data_exported_file_mixed = device.CR10X.read_mixed_data(infile_path=output_file)
        data_exported_file_header = data_exported_file_mixed[0]

        self.assertDataContentEqual(data_1=headers, data_2=data_exported_file_header)
        self.delete_output(output_file)

    def test_export_array_ids_to_csv_empty_library(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        data_split = device.CR10X.read_array_ids_data(infile_path=file)
        with self.assertRaises(ValueError):
            device.CR10X.export_array_ids_to_csv(data=data_split, array_ids_info={})

    def test_export_array_ids_to_csv_empty_info(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        data_split = device.CR10X.read_array_ids_data(infile_path=file)
        with self.assertRaises(ValueError):
            device.CR10X.export_array_ids_to_csv(data=data_split, array_ids_info={'201': None})

    def test_export_array_ids_to_csv_no_file_path(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        data_split = device.CR10X.read_array_ids_data(infile_path=file)
        with self.assertRaises(ValueError):
            device.CR10X.export_array_ids_to_csv(data=data_split, array_ids_info={'201': {'header': []}})

    def test_export_array_ids_to_csv_content(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        output_file = os.path.join(THIS_DIR, 'testoutput/test.dat')
        data_split_unfiltered = device.CR10X.read_array_ids_data(infile_path=file)
        data_split_filtered = device.CR10X.filter_data_by_array_ids('201', data=data_split_unfiltered)
        data_split_array_id_filtered = data_split_filtered.get('201')

        device.CR10X.export_array_ids_to_csv(data=data_split_unfiltered, array_ids_info={'201': {'file_path': output_file}})
        data_exported_file = device.CR10X.read_mixed_data(infile_path=output_file)

        self.assertDataContentEqual(data_1=data_split_array_id_filtered, data_2=data_exported_file)
        self.delete_output(output_file)

    def test_export_array_ids_to_csv_headers(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        output_file_1 = os.path.join(THIS_DIR, 'testoutput/test_1.dat')
        output_file_2 = os.path.join(THIS_DIR, 'testoutput/test_2.dat')
        data_split_unfiltered = device.CR10X.read_array_ids_data(infile_path=file)
        data_split_filtered = device.CR10X.filter_data_by_array_ids('201', '203', data=data_split_unfiltered)

        headers_1 = ["Array_Id", "Year", "Day", "Hour/Minute", "Tot_rad_AVG", "Air_Temp2_AVG", "Air_Temp1",
                     "Humidity_AVG", "Wind_spd_S_WVT", "Wind_spd_U_WVT", "Wind_dir_DU_WVT", "Wind_dir_SDU_WVT",
                     "Wind_spd3_AVG", "BadTemp_AVG", "PAR_AVG", "Air_Pres_AVG"]

        headers_2 = ["Array_Id", "Year", "Day", "Hour/Minute", "Wind_spd_AVG", "Wind_dir_AVG"]

        array_id_info_1 = {'file_path': output_file_1, 'headers': headers_1}
        array_id_info_2 = {'file_path': output_file_2, 'headers': headers_2}
        array_id_info = {'201': array_id_info_1, '203': array_id_info_2}

        device.CR10X.export_array_ids_to_csv(data=data_split_filtered, array_ids_info=array_id_info)
        data_exported_file_1 = device.CR10X.read_mixed_data(infile_path=output_file_1)
        data_exported_file_2 = device.CR10X.read_mixed_data(infile_path=output_file_2)

        self.assertDataContentEqual(data_1=headers_1, data_2=data_exported_file_1[0])
        self.assertDataContentEqual(data_1=headers_2, data_2=data_exported_file_2[0])
        self.delete_output(output_file_1)


class ConvertTimeTest(ReadDataTestCase):

    def test_convert_cr10x_time(self):
        time_zone = 'UTC'
        year = '2016'
        day = '30'
        hour_minute = '2230'
        parsed_time_expected = datetime(2016, 1, 30, 22, 30, 0, tzinfo=pytz.UTC)

        time_parser = timeparser.CR10XTimeParser(time_zone)
        parsed_time = time_parser.parse_time(*[year, day, hour_minute], to_utc=False)

        self.assertEqual(parsed_time, parsed_time_expected)

    def test_convert_cr10x_time_to_utc(self):
        time_zone = 'UTC'
        year = '2016'
        day = '30'
        hour_minute = '2230'
        parsed_time_expected = datetime(2016, 1, 30, 22, 30, 0, tzinfo=pytz.UTC)

        time_parser = timeparser.CR10XTimeParser(time_zone)
        parsed_time = time_parser.parse_time(*[year, day, hour_minute], to_utc=True)

        self.assertEqual(parsed_time, parsed_time_expected)

    def test_convert_cr10x_time_from_row(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        array_ids_info = {'201': 'label_1'}
        expected_datetime = datetime(2012, 11, 25, 22, 0, 0, tzinfo=pytz.UTC)

        data = device.CR10X.read_array_ids_data(infile_path=file, array_ids_info=array_ids_info)
        data_time_converted = device.CR10X.convert_time(
            data=data.get('label_1'), time_columns=[1, 2, 3], data_time_zone='UTC'
        )
        headers_post_conversion, data_post_conversion = data_time_converted

        self.assertEqual(expected_datetime, data_post_conversion[0][1])

    def test_convert_cr10x_time_from_row_header_name(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        array_ids_info = {'201': 'label_1'}

        headers = ["Array_Id", "Year", "Day", "Hour/Minute", "Tot_rad_AVG", "Air_Temp2_AVG", "Air_Temp1",
                   "Humidity_AVG", "Wind_spd_S_WVT", "Wind_spd_U_WVT", "Wind_dir_DU_WVT", "Wind_dir_SDU_WVT",
                   "Wind_spd3_AVG", "BadTemp_AVG", "PAR_AVG", "Air_Pres_AVG"]

        time_columns = ["Year", "Day", "Hour/Minute"]
        converted_time_header = "TMSTAMP"

        expected_headers_post_conversion = [
            "Array_Id", "TMSTAMP", "Tot_rad_AVG", "Air_Temp2_AVG", "Air_Temp1",
            "Humidity_AVG", "Wind_spd_S_WVT", "Wind_spd_U_WVT", "Wind_dir_DU_WVT", "Wind_dir_SDU_WVT",
            "Wind_spd3_AVG", "BadTemp_AVG", "PAR_AVG", "Air_Pres_AVG"
        ]

        data = device.CR10X.read_array_ids_data(infile_path=file, array_ids_info=array_ids_info)
        data_time_converted = device.CR10X.convert_time(
            data=data.get('label_1'), headers=headers, time_parsed_column=converted_time_header,
            time_columns=time_columns, data_time_zone='UTC'
        )
        headers_post_conversion, data_post_conversion = data_time_converted

        self.assertListEqual(headers_post_conversion, expected_headers_post_conversion)

    def test_export_to_csv_time_converted_data_no_time_zone(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        output_file = os.path.join(THIS_DIR, 'testoutput/test.dat')
        array_ids_info = {'201': 'label_1'}
        expected_datetime = datetime(2012, 11, 25, 22, 0, 0)
        expected_datetime_string = str(expected_datetime)

        data = device.CR10X.read_array_ids_data(infile_path=file, array_ids_info=array_ids_info)
        data_time_converted = device.CR10X.convert_time(
            data=data.get('label_1'), time_columns=[1, 2, 3], data_time_zone='UTC'
        )
        headers_post_conversion, data_post_conversion = data_time_converted

        array_ids_export_info = {'201': {'file_path': output_file}}
        device.CR10X.export_array_ids_to_csv(data=data_post_conversion, array_ids_info=array_ids_export_info)
        data_exported_file = device.CR10X.read_mixed_data(infile_path=output_file)
        exported_datetime_string = data_exported_file[0][1]

        self.assertEqual(expected_datetime_string, exported_datetime_string)
        self.delete_output(output_file)

    def test_export_to_csv_time_converted_data_time_zone(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        output_file = os.path.join(THIS_DIR, 'testoutput/test.dat')
        array_ids_info = {'201': 'label_1'}
        expected_datetime = datetime(2012, 11, 25, 22, 0, 0, tzinfo=pytz.UTC)
        expected_datetime_string = str(expected_datetime)

        data = device.CR10X.read_array_ids_data(infile_path=file, array_ids_info=array_ids_info)
        data_time_converted = device.CR10X.convert_time(
            data=data.get('label_1'), time_columns=[1, 2, 3], data_time_zone='UTC'
        )
        headers_post_conversion, data_post_conversion = data_time_converted

        array_ids_export_info = {'201': {'file_path': output_file}}
        device.CR10X.export_array_ids_to_csv(data=data_post_conversion, array_ids_info=array_ids_export_info,
                                             include_time_zone=True)
        data_exported_file = device.CR10X.read_mixed_data(infile_path=output_file)
        exported_datetime_string = data_exported_file[0][1]

        self.assertEqual(expected_datetime_string, exported_datetime_string)
        self.delete_output(output_file)
