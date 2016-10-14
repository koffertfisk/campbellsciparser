#!/usr/bin/env
# -*- coding: utf-8 -*-

import os.path

from campbellsciparser.devices.cr10 import CR10Parser
from tests.devices.common_all import *
from tests.devices.cr10.common_cr10 import *


class CR10ParserReadArrayIdsDataTest(CR10ParserTestCase):

    def test_empty(self):
        file = os.path.join(TEST_DEVICES_DIR, 'testdata/all/csv_all_testdata_empty.dat')
        cr10 = CR10Parser()
        data = cr10.read_array_ids_data(infile_path=file)

        self.assertDataLengthEqual(data=data, data_length=0)

    def test_length_ten_rows(self):
        file = os.path.join(TEST_DEVICES_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        cr10 = CR10Parser()
        data = cr10.read_array_ids_data(infile_path=file)
        data_mixed = [row for array_id, array_id_data in data.items()
                      for row in array_id_data]

        self.assertDataLengthEqual(data=data_mixed, data_length=10)

    def test_compare_length_ten_rows(self):
        file = os.path.join(TEST_DEVICES_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        cr10 = CR10Parser()

        data_mixed = cr10.read_mixed_data(infile_path=file)
        data_split = cr10.read_array_ids_data(infile_path=file)
        data_split_merged = [row for array_id, array_id_data in data_split.items()
                             for row in array_id_data]

        self.assertTwoDataSetsLengthEqual(data_1=data_mixed, data_2=data_split_merged)

    def test_compare_data_ten_rows(self):
        file = os.path.join(TEST_DEVICES_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        cr10 = CR10Parser()

        data_mixed = cr10.read_mixed_data(infile_path=file)
        data_split = cr10.read_array_ids_data(infile_path=file)
        data_split_merged = [row for array_id, array_id_data in data_split.items()
                             for row in array_id_data]

        self.assertTwoDataSetsLengthEqual(data_1=data_mixed, data_2=data_split_merged)

    def test_compare_data_ten_rows_lookup(self):
        file = os.path.join(TEST_DEVICES_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        array_ids_names = {
            '201': 'label_1', '203': 'label_2', '204': 'label_3', '210': 'label_4'
        }
        cr10 = CR10Parser()

        data_mixed = cr10.read_mixed_data(infile_path=file)
        data_split_translated = cr10.read_array_ids_data(
            infile_path=file, array_id_names=array_ids_names)

        data_split_translated_merged = [
            row for array_id, array_id_data in data_split_translated.items()
            for row in array_id_data
        ]

        self.assertTwoDataSetsLengthEqual(data_1=data_mixed, data_2=data_split_translated_merged)