#!/usr/bin/env
# -*- coding: utf-8 -*-

import os

from campbellsciparser.devices import CR10Parser

TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')


def assert_floating_points_fixed(data_no_fp_fix, data_fp_fixed, replacements_lib):
    unprocessed = []
    for row_no_fp_fix, row_fp_fix in zip(data_no_fp_fix, data_fp_fixed):
        for value_no_fix, value_fix in zip(list(row_no_fp_fix.values()),
                                           list(row_fp_fix.values())):
            for source, replacement in replacements_lib.items():
                if value_no_fix.startswith(source) and not value_fix.startswith(
                        replacement):
                    unprocessed.append(value_no_fix)

    assert len(unprocessed) == 0


def test_length_empty():
    file = os.path.join(TEST_DATA_DIR, 'csv_cr10_testdata_empty.dat')
    cr10 = CR10Parser()
    data = cr10.read_mixed_data(infile_path=file)

    assert len(data) == 0


def test_length_ten_rows():
    file = os.path.join(TEST_DATA_DIR, 'csv_cr10_testdata_10_rows.dat')
    cr10 = CR10Parser()
    data = cr10.read_mixed_data(infile_path=file)

    assert len(data) == 10


def test_length_line_num_five_rows():
    file = os.path.join(TEST_DATA_DIR, 'csv_cr10_testdata_10_rows.dat')
    cr10 = CR10Parser()
    data = cr10.read_mixed_data(infile_path=file, line_num=5)

    assert len(data) == 5


def test_fix_floating_points():
    file = os.path.join(TEST_DATA_DIR, 'csv_cr10_testdata_10_rows.dat')
    cr10 = CR10Parser()
    data_no_fp_fix = cr10.read_mixed_data(infile_path=file, fix_floats=False)
    data_fp_fixed = cr10.read_mixed_data(infile_path=file, fix_floats=True)
    replacements = {'.': '0.', '-.': '-0.'}

    assert_floating_points_fixed(
        data_no_fp_fix=data_no_fp_fix,
        data_fp_fixed=data_fp_fixed,
        replacements_lib=replacements
    )
