#!/usr/bin/env
# -*- coding: utf-8 -*-

import os.path

from campbellsciparser.devices.cr10 import CR10Parser
from tests.devices.common_all import *
from tests.devices.cr10.common_cr10 import *


class CR10ParserFilterDataTest(CR10ParserTestCase):

    def test_filter_array_ids_data_no_filter(self):
        file = os.path.join(TEST_DEVICES_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        cr10 = CR10Parser()

        data_mixed = cr10.read_mixed_data(infile_path=file)
        data_split = cr10.read_array_ids_data(infile_path=file)
        data_filtered = cr10.filter_data_by_array_ids(data=data_split)
        data_split_merged = [row for array_id, array_id_data in data_filtered.items()
                             for row in array_id_data]

        self.assertTwoDataSetsLengthEqual(data_1=data_mixed, data_2=data_split_merged)

    def test_filter_array_ids_data_filter_all(self):
        file = os.path.join(TEST_DEVICES_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        cr10 = CR10Parser()

        data_mixed = cr10.read_mixed_data(infile_path=file)
        data_split = cr10.read_array_ids_data(infile_path=file)
        data_split_filtered = cr10.filter_data_by_array_ids(
            data_split, '201', '203', '204', '210')

        data_split_merged = [row for array_id, array_id_data in data_split_filtered.items()
                             for row in array_id_data]

        self.assertTwoDataSetsLengthEqual(data_1=data_mixed, data_2=data_split_merged)

    def test_filter_array_ids_data_filter(self):
        file_unfiltered = os.path.join(
            TEST_DEVICES_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')

        file_filtered = os.path.join(
            TEST_DEVICES_DIR, 'testdata/cr10/csv_cr10_testdata_filtered_rows.dat')

        cr10 = CR10Parser()

        data_pre_filtered = cr10.read_mixed_data(infile_path=file_filtered)
        data_split_unfiltered = cr10.read_array_ids_data(infile_path=file_unfiltered)
        data_split_filtered = cr10.filter_data_by_array_ids(
            data_split_unfiltered, '201', '203')

        data_split_filtered_merged = [
            row for array_id, array_id_data in data_split_filtered.items()
            for row in array_id_data
        ]

        self.assertTwoDataSetsLengthEqual(data_1=data_pre_filtered, data_2=data_split_filtered_merged)