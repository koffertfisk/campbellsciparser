#!/usr/bin/env
# -*- coding: utf-8 -*-

import os.path

from campbellsciparser.devices.cr10 import CR10Parser
from tests.devices.common_all import *
from tests.devices.cr10.common_cr10 import *


class CR10ParserReadMixedDataTest(CR10ParserTestCase):

    def test_length_empty(self):
        file = os.path.join(TEST_DEVICES_DIR, 'testdata/all/csv_all_testdata_empty.dat')
        cr10 = CR10Parser()
        data = cr10.read_mixed_data(infile_path=file)

        self.assertDataLengthEqual(data=data, data_length=0)

    def test_length_ten_rows(self):
        file = os.path.join(TEST_DEVICES_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        cr10 = CR10Parser()
        data = cr10.read_mixed_data(infile_path=file)

        self.assertDataLengthEqual(data=data, data_length=10)

    def test_length_line_num_five_rows(self):
        file = os.path.join(TEST_DEVICES_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        cr10 = CR10Parser()
        data = cr10.read_mixed_data(infile_path=file, line_num=5)

        self.assertDataLengthEqual(data=data, data_length=5)

    def test_fix_floating_points(self):
        file = os.path.join(TEST_DEVICES_DIR, 'testdata/cr10/csv_cr10_testdata_10_rows.dat')
        cr10 = CR10Parser()
        data_no_fp_fix = cr10.read_mixed_data(infile_path=file, fix_floats=False)
        data_fp_fixed = cr10.read_mixed_data(infile_path=file, fix_floats=True)
        replacements = {'.': '0.', '-.': '-0.'}

        self.assertFloatingPointsFixed(
            data_no_fp_fix=data_no_fp_fix,
            data_fp_fixed=data_fp_fixed,
            replacements_lib=replacements
        )
