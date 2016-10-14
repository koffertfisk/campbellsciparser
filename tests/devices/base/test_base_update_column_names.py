# !/usr/bin/env
# -*- coding: utf-8 -*-

import os.path

from campbellsciparser.devices.base import CampbellSCIBaseParser

from tests.devices.common_all import *


class BaseParserUpdateColumnNamesTest(ReadDataTestCase):

    def test_update_column_names(self):
        file = os.path.join(TEST_DEVICES_DIR, 'testdata/base/csv_base_testdata_3_rows.dat')
        baseparser = CampbellSCIBaseParser()
        headers = ['Label_' + str(i) for i in range(3)]

        data = baseparser.read_data(infile_path=file)
        data_updated_headers = baseparser.update_column_names(data=data, headers=headers)

        for row in data_updated_headers:
            self.assertListEqual(list(row.keys()), headers)

    def test_update_column_names_not_matching_row_lengths(self):
        file = os.path.join(TEST_DEVICES_DIR, 'testdata/base/csv_base_testdata_3_rows.dat')
        baseparser = CampbellSCIBaseParser()
        headers = ['Label_' + str(i) for i in range(3)]

        data = baseparser.read_data(infile_path=file)
        data_updated_headers = baseparser.update_column_names(data=data, headers=headers)

        for row in data_updated_headers:
            for updated_row_name, expected_row_name in zip(list(row.keys()), headers):
                self.assertEqual(updated_row_name, expected_row_name)

    def test_update_column_names_output_mismatched_rows(self):
        file = os.path.join(TEST_DEVICES_DIR, 'testdata/base/csv_base_testdata_3_rows.dat')
        baseparser = CampbellSCIBaseParser()
        headers = ['Label_' + str(i) for i in range(3)]

        data = baseparser.read_data(infile_path=file)

        data_updated_headers, data_mismatched_rows = baseparser.update_column_names(
            data=data,
            headers=headers,
            match_row_lengths=True,
            output_mismatched_rows=True
        )

        for row in data_mismatched_rows:
            self.assertDataLengthNotEqual(row, len(headers))
