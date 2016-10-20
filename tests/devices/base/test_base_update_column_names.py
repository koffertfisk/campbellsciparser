# !/usr/bin/env
# -*- coding: utf-8 -*-

import os

from campbellsciparser.devices import CRGeneric

TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')


def test_update_column_names():
    file = os.path.join(TEST_DATA_DIR, 'csv_base_testdata_3_rows.dat')
    baseparser = CRGeneric()
    headers = ['Label_' + str(i) for i in range(3)]

    data = baseparser.read_data(infile_path=file)
    data_updated_headers = baseparser.update_column_names(data=data, headers=headers)

    for row in data_updated_headers:
        assert len(list(row.keys())) == len(headers) and (
            sorted(list(row.keys())) == sorted(headers))


def test_update_column_names_not_matching_row_lengths():
    file = os.path.join(TEST_DATA_DIR, 'csv_base_testdata_3_rows.dat')
    baseparser = CRGeneric()
    headers = ['Label_' + str(i) for i in range(3)]

    data = baseparser.read_data(infile_path=file)
    data_updated_headers = baseparser.update_column_names(data=data, headers=headers)

    for row in data_updated_headers:
        for updated_row_name, expected_row_name in zip(list(row.keys()), headers):
            assert updated_row_name == expected_row_name


def test_update_column_names_output_mismatched_rows():
    file = os.path.join(TEST_DATA_DIR, 'csv_base_testdata_3_rows.dat')
    baseparser = CRGeneric()
    headers = ['Label_' + str(i) for i in range(3)]

    data = baseparser.read_data(infile_path=file)

    data_updated_headers, data_mismatched_rows = baseparser.update_column_names(
        data=data,
        headers=headers,
        match_row_lengths=True,
        output_mismatched_rows=True
    )

    for row in data_mismatched_rows:
        assert len(row) != len(headers)
