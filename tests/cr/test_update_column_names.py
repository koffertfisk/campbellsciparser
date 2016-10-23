# !/usr/bin/env
# -*- coding: utf-8 -*-

import os

from campbellsciparser import cr

TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')


def test_update_column_names():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_3_rows.dat')
    column_names = ['Label_' + str(i) for i in range(3)]

    data = cr.read_table_data(infile_path=file)
    data_updated_headers = cr.update_column_names(data=data, column_names=column_names)

    for row in data_updated_headers:
        assert len(list(row.keys())) == len(column_names) and (
            sorted(list(row.keys())) == sorted(column_names))


def test_update_column_names_not_matching_row_lengths():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_3_rows.dat')
    column_names = ['Label_' + str(i) for i in range(3)]

    data = cr.read_table_data(infile_path=file)
    data_updated_headers = cr.update_column_names(data=data, column_names=column_names)

    for row in data_updated_headers:
        for updated_row_name, expected_row_name in zip(list(row.keys()), column_names):
            assert updated_row_name == expected_row_name


def test_update_column_names_output_mismatched_rows():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_3_rows.dat')
    column_names = ['Label_' + str(i) for i in range(3)]

    data = cr.read_table_data(infile_path=file)

    data_updated_headers, data_mismatched_rows = cr.update_column_names(
        data=data,
        column_names=column_names,
        match_row_lengths=True,
        get_mismatched_row_lengths=True
    )

    for row in data_mismatched_rows:
        assert len(row) != len(column_names)
