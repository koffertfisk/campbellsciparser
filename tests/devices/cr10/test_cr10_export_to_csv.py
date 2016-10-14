#!/usr/bin/env
# -*- coding: utf-8 -*-

import os.path

from campbellsciparser.devices.cr10 import CR10Parser
from tests.devices.common_all import *
from tests.devices.cr10.common_cr10 import *


class CR10ParserExportDataTest(CR10ParserTestCase, ExportFileTestCase):

    def test_export_array_ids_to_csv_empty_library(self):
        file = os.path.join(TEST_DEVICES_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        cr10 = CR10Parser()
        data_split = cr10.read_array_ids_data(infile_path=file)
        with self.assertRaises(ValueError):
            cr10.export_array_ids_to_csv(data=data_split, array_ids_info={})

    def test_export_array_ids_to_csv_empty_info(self):
        file = os.path.join(TEST_DEVICES_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        cr10 = CR10Parser()
        data_split = cr10.read_array_ids_data(infile_path=file)
        with self.assertRaises(ValueError):
            cr10.export_array_ids_to_csv(data=data_split, array_ids_info={'201': None})

    def test_export_array_ids_to_csv_no_file_path(self):
        file = os.path.join(TEST_DEVICES_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        cr10 = CR10Parser()
        data_split = cr10.read_array_ids_data(infile_path=file)
        with self.assertRaises(ValueError):
            cr10.export_array_ids_to_csv(data=data_split, array_ids_info={'201': {}})

    def test_export_array_ids_to_csv_content(self):
        file = os.path.join(TEST_DEVICES_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        output_file = os.path.join(TEST_DEVICES_DIR, 'testoutput/test.dat')
        cr10 = CR10Parser()

        data_split_unfiltered = cr10.read_array_ids_data(infile_path=file)
        data_split_filtered = cr10.filter_data_by_array_ids(data_split_unfiltered, '201')
        data_split_array_id_filtered = data_split_filtered.get('201')

        cr10.export_array_ids_to_csv(
            data=data_split_unfiltered,
            array_ids_info={'201': {'file_path': output_file}})

        data_exported_file = cr10.read_mixed_data(infile_path=output_file)

        self.assertTwoDataSetsContentEqual(
            data_1=data_split_array_id_filtered, data_2=data_exported_file)

        self.delete_output(output_file)

    def test_export_array_ids_to_csv_headers(self):
        file = os.path.join(TEST_DEVICES_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        output_file_1 = os.path.join(TEST_DEVICES_DIR, 'testoutput/test_1.dat')
        output_file_2 = os.path.join(TEST_DEVICES_DIR, 'testoutput/test_2.dat')
        cr10 = CR10Parser()

        data_split_unfiltered = cr10.read_array_ids_data(infile_path=file)
        data_split_filtered = cr10.filter_data_by_array_ids(
            data_split_unfiltered, '201', '203')

        data_split_filtered_201 = data_split_filtered.get('201')
        data_split_filtered_203 = data_split_filtered.get('203')

        data_split_filtered_201_first_row = data_split_filtered_201[0]
        data_split_filtered_203_first_row = data_split_filtered_203[0]

        data_split_filtered_201_headers = [str(key) for key
                                           in data_split_filtered_201_first_row.keys()]
        data_split_filtered_203_headers = [str(key) for key
                                           in data_split_filtered_203_first_row.keys()]

        array_id_info_1 = {'file_path': output_file_1}
        array_id_info_2 = {'file_path': output_file_2}
        array_ids_info = {'201': array_id_info_1, '203': array_id_info_2}

        cr10.export_array_ids_to_csv(
            data=data_split_filtered, array_ids_info=array_ids_info, export_headers=True)

        data_exported_file_1 = cr10.read_mixed_data(infile_path=output_file_1)
        data_exported_file_2 = cr10.read_mixed_data(infile_path=output_file_2)

        data_exported_file_1_headers_row = data_exported_file_1[0]
        data_exported_file_1_headers = [value for value
                                        in data_exported_file_1_headers_row.values()]

        data_exported_file_2_headers_row = data_exported_file_2[0]
        data_exported_file_2_headers = [value for value
                                        in data_exported_file_2_headers_row.values()]

        self.assertListEqual(data_split_filtered_201_headers, data_exported_file_1_headers)
        self.assertListEqual(data_split_filtered_203_headers, data_exported_file_2_headers)
        self.delete_output(output_file_1)

    def test_export_array_ids_to_csv_updated_headers(self):
        file = os.path.join(TEST_DEVICES_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        output_file = os.path.join(TEST_DEVICES_DIR, 'testoutput/test.dat')
        cr10 = CR10Parser()

        data_split_unfiltered = cr10.read_array_ids_data(infile_path=file)
        data_split_filtered = cr10.filter_data_by_array_ids(data_split_unfiltered, '203')
        data_split_array_id_filtered = data_split_filtered.get('203')

        headers = ['Array_Id', 'Year', 'Day', 'Hour/Minute', 'Wind_Speed', 'Wind_Direction']
        data_updated_headers = cr10.update_column_names(
            data=data_split_array_id_filtered, headers=headers)

        array_ids_info = {'203': {'file_path': output_file}}
        cr10.export_array_ids_to_csv(
            data=data_updated_headers, array_ids_info=array_ids_info, export_headers=True)

        data_exported_file = cr10.read_mixed_data(infile_path=output_file)
        data_headers_updated_first_row = data_exported_file[0]
        data_headers_updated_headers = [value for value
                                        in data_headers_updated_first_row.values()]

        self.assertListEqual(headers, data_headers_updated_headers)
        self.delete_output(output_file)