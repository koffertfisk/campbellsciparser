#!/usr/bin/env
# -*- coding: utf-8 -*-

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import shutil
import unittest

from campbellsciparser.device import CR10X

THIS_DIR = os.path.dirname(os.path.abspath(__file__))


class GetDataTestCase(unittest.TestCase):

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


class GetArrayIdsDataTestCase(GetDataTestCase):

    def assertDataLengthEqual(self, data_1, data_2):
        self.assertEqual(len(data_1), len(data_2))

    def assertDataContentEqual(self, data_1, data_2):
        data_1_diff_elements = [row for row in data_1 if row not in data_2]
        GetDataTestCase.assertDataLengthEqual(self, data_1_diff_elements, 0)

        data_2_diff_elements = [row for row in data_2 if row not in data_1]
        GetDataTestCase.assertDataLengthEqual(self, data_2_diff_elements, 0)


class GetDataTest(GetDataTestCase):

    def test_length_empty(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_empty.dat')
        data = CR10X.get_data(file)

        self.assertDataLengthEqual(data, 0)

    def test_length_ten_rows(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        data = CR10X.get_data(file)

        self.assertDataLengthEqual(data, 10)

    def test_length_line_num_five_rows(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        data = CR10X.get_data(file, line_num=5)

        self.assertDataLengthEqual(data, 5)

    def test_fix_floating_points(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        data_no_fp_fix = CR10X.get_data(file, fix_floats=False)
        data_fp_fixed = CR10X.get_data(file, fix_floats=True)
        replacements = {'.': '0.', '-.': '-0.'}

        self.assertFloatingPointsFixed(data_no_fp_fix, data_fp_fixed, replacements)


class GetArrayIdsData(GetArrayIdsDataTestCase):

    def test_empty(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_empty.dat')
        data = CR10X.get_array_ids_data(file)

        GetDataTestCase.assertDataLengthEqual(self, data, 0)

    def test_length_ten_rows(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        data = CR10X.get_array_ids_data(file)
        data_mixed = []
        for array_id, array_id_data in data.items():
            for row in array_id_data:
                data_mixed.append(row)

        GetDataTestCase.assertDataLengthEqual(self, data_mixed, 10)

    def test_compare_length_ten_rows(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        data_mixed = CR10X.get_data(file)
        data_split = CR10X.get_array_ids_data(file)
        data_split_merged = []
        for array_id, array_id_data in data_split.items():
            for row in array_id_data:
                data_split_merged.append(row)

        GetArrayIdsDataTestCase.assertDataLengthEqual(self, data_mixed, data_split_merged)

    def test_compare_data_ten_rows(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        data_mixed = CR10X.get_data(file)
        data_split = CR10X.get_array_ids_data(file)
        data_split_merged = []
        for array_id, array_id_data in data_split.items():
            for row in array_id_data:
                data_split_merged.append(row)

        self.assertDataContentEqual(data_mixed, data_split_merged)

    def test_compare_data_ten_rows_lookup(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        data_mixed = CR10X.get_data(file)
        data_split_translated = CR10X.get_array_ids_data(file, array_ids_table={'201': 'label_1', '203': 'label_2', '204': 'label_3', '210': 'label_4'})
        data_split_translated_merged = []
        for array_name, array_name_data in data_split_translated.items():
            for row in array_name_data:
                data_split_translated_merged.append(row)

        self.assertDataContentEqual(data_mixed, data_split_translated_merged)

    def test_filter_array_ids_data_no_filter(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        data_mixed = CR10X.get_data(file)
        data_split = CR10X.get_array_ids_data(file)
        data_filtered = CR10X.filter_array_ids(data_split)
        data_split_merged = []
        for array_id, array_id_data in data_filtered.items():
            for row in array_id_data:
                data_split_merged.append(row)

        self.assertDataContentEqual(data_mixed, data_split_merged)

    def test_filter_array_ids_data_filter_all(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        data_mixed = CR10X.get_data(file)
        data_split = CR10X.get_array_ids_data(file)
        data_split_filtered = CR10X.filter_array_ids(data_split, '201', '203', '204', '210')
        data_split_merged = []
        for array_id, array_id_data in data_split_filtered.items():
            for row in array_id_data:
                data_split_merged.append(row)

        self.assertDataContentEqual(data_mixed, data_split_merged)

    def test_filter_array_ids_data_filter(self):
        file_unfiltered = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        file_filtered = os.path.join(THIS_DIR, 'testdata/csv_testdata_filtered_rows.dat')
        data_pre_filtered = CR10X.get_data(file_filtered)
        data_split_unfiltered = CR10X.get_array_ids_data(file_unfiltered)
        data_split_filtered = CR10X.filter_array_ids(data_split_unfiltered, '201', '203')
        data_split_filtered_merged = []
        for array_id, array_id_data in data_split_filtered.items():
            for row in array_id_data:
                data_split_filtered_merged.append(row)

        self.assertDataContentEqual(data_pre_filtered, data_split_filtered_merged)

    def test_export_to_csv_file_created(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        output_file = os.path.join(THIS_DIR, 'testoutput/test.dat')
        data_mixed = CR10X.get_data(file)
        CR10X.export_to_csv(data_mixed, output_file)

        self.assertExportedFileExists(output_file)

    def test_export_to_csv_file_content(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        output_file = os.path.join(THIS_DIR, 'testoutput/test.dat')
        data_source_file_mixed = CR10X.get_data(file)
        CR10X.export_to_csv(data_source_file_mixed, output_file)
        data_exported_file_mixed = CR10X.get_data(output_file)

        self.assertDataContentEqual(data_source_file_mixed, data_exported_file_mixed)

        self.delete_output(output_file)

    def test_export_to_csv_file_header(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        output_file = os.path.join(THIS_DIR, 'testoutput/test.dat')
        data_source_file_mixed = CR10X.get_data(file)
        header = ["Array_Id","Year_RTM","Day_RTM","Hour_Minute_RTM","Tot_rad_AVG","Air_Temp2_AVG","Air_Temp1",
                  "Humidity_AVG","Wind_spd_S_WVT","Wind_spd_U_WVT","Wind_dir_DU_WVT","Wind_dir_SDU_WVT",
                  "Wind_spd3_AVG","BadTemp_AVG","PAR_AVG","Air_Pres_AVG"]
        CR10X.export_to_csv(data_source_file_mixed, output_file, header=header)
        data_exported_file_mixed = CR10X.get_data(output_file)
        data_exported_file_header = data_exported_file_mixed[0]
        self.assertDataContentEqual(header, data_exported_file_header)

        self.delete_output(output_file)

    def test_export_array_ids_to_csv_empty_library(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        data_split = CR10X.get_array_ids_data(file)
        with self.assertRaises(ValueError):
            CR10X.export_array_ids_to_csv(data_split, {})

    def test_export_array_ids_to_csv_empty_info(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        data_split = CR10X.get_array_ids_data(file)
        with self.assertRaises(ValueError):
            CR10X.export_array_ids_to_csv(data_split, {'201': None})

    def test_export_array_ids_to_csv_no_file_path(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        data_split = CR10X.get_array_ids_data(file)
        with self.assertRaises(ValueError):
            CR10X.export_array_ids_to_csv(data_split, {'201': {'header': []}})

    def test_export_array_ids_to_csv_content(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        output_file = os.path.join(THIS_DIR, 'testoutput/test.dat')
        data_split_unfiltered = CR10X.get_array_ids_data(file)
        data_split_filtered = CR10X.filter_array_ids(data_split_unfiltered, '201')
        data_split_array_id_filtered = data_split_filtered.get('201')
        CR10X.export_array_ids_to_csv(data_split_unfiltered, {'201': {'file_path': output_file}})
        data_exported_file = CR10X.get_data(output_file)
        self.assertDataContentEqual(data_split_array_id_filtered, data_exported_file)

        self.delete_output(output_file)

    def test_export_array_ids_to_csv_headers(self):
        file = os.path.join(THIS_DIR, 'testdata/csv_testdata_10_rows.dat')
        output_file_1 = os.path.join(THIS_DIR, 'testoutput/test_1.dat')
        output_file_2 = os.path.join(THIS_DIR, 'testoutput/test_2.dat')
        data_split_unfiltered = CR10X.get_array_ids_data(file)
        data_split_filtered = CR10X.filter_array_ids(data_split_unfiltered, '201', '203')
        headers_1 = ["Array_Id","Year_RTM","Day_RTM","Hour_Minute_RTM","Tot_rad_AVG","Air_Temp2_AVG","Air_Temp1",
                     "Humidity_AVG","Wind_spd_S_WVT","Wind_spd_U_WVT","Wind_dir_DU_WVT","Wind_dir_SDU_WVT",
                     "Wind_spd3_AVG","BadTemp_AVG","PAR_AVG","Air_Pres_AVG"]
        headers_2 = ["Array_ID", "Year_RTM", "Day_RTM", "Hour_Minute_RTM", "Wind_spd_AVG", "Wind_dir_AVG"]
        array_id_info_1 = {'file_path': output_file_1, 'header': headers_1}
        array_id_info_2 = {'file_path': output_file_2, 'header': headers_2}
        CR10X.export_array_ids_to_csv(data_split_filtered, {'201': array_id_info_1, '203': array_id_info_2})
        data_exported_file_1 = CR10X.get_data(output_file_1)
        data_exported_file_2 = CR10X.get_data(output_file_2)
        self.assertDataContentEqual(headers_1, data_exported_file_1[0])
        self.assertDataContentEqual(headers_2, data_exported_file_2[0])

        self.delete_output(output_file_1)