#!/usr/bin/env
# -*- coding: utf-8 -*-

import os

from campbellsciparser import cr
from campbellsciparser.dataset import DataSet

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
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_empty.dat')
    data = cr.read_mixed_array_data(infile_path=file)
    assert len(data) == 0

    data = cr.read_array_ids_data(infile_path=file)
    assert len(data) == 0


def test_length_ten_rows():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_mixed_array_10_rows.dat')
    data = cr.read_mixed_array_data(infile_path=file)
    assert len(data) == 10

    data = cr.read_array_ids_data(infile_path=file)
    data_mixed = DataSet([row for array_id, array_id_data in data.items()
                          for row in array_id_data])
    assert len(data_mixed) == 10


def test_length_line_num_five_rows():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_mixed_array_10_rows.dat')
    data = cr.read_mixed_array_data(infile_path=file, first_line_num=5)

    assert len(data) == 5


def test_fix_floating_points():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_mixed_array_10_rows.dat')
    data_no_fp_fix = cr.read_mixed_array_data(infile_path=file, fix_floats=False)
    data_fp_fixed = cr.read_mixed_array_data(infile_path=file, fix_floats=True)
    replacements = {'.': '0.', '-.': '-0.'}

    assert_floating_points_fixed(
        data_no_fp_fix=data_no_fp_fix,
        data_fp_fixed=data_fp_fixed,
        replacements_lib=replacements
    )


def test_compare_length_ten_rows():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_mixed_array_10_rows.dat')

    data_mixed = cr.read_mixed_array_data(infile_path=file)
    data_split = cr.read_array_ids_data(infile_path=file)
    data_split_merged = DataSet([row for array_id, array_id_data in data_split.items()
                                 for row in array_id_data])

    assert len(data_mixed) == len(data_split_merged)


def test_compare_data_ten_rows():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_mixed_array_10_rows.dat')

    data_mixed = cr.read_mixed_array_data(infile_path=file)
    data_split = cr.read_array_ids_data(infile_path=file)
    data_split_merged = DataSet([row for array_id, array_id_data in data_split.items()
                                 for row in array_id_data])

    assert len(data_mixed) == len(data_split_merged)


def test_compare_data_ten_rows_lookup():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_mixed_array_10_rows.dat')
    array_ids_names = {
        '201': 'label_1', '203': 'label_2', '204': 'label_3', '210': 'label_4'
    }

    data_mixed = cr.read_mixed_array_data(infile_path=file)
    data_split_translated = cr.read_array_ids_data(
        infile_path=file, array_id_names=array_ids_names)

    data_split_translated_merged = DataSet([
        row for array_id, array_id_data in data_split_translated.items()
        for row in array_id_data
    ])

    assert len(data_mixed) == len(data_split_translated_merged)
