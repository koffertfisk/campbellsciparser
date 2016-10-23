#!/usr/bin/env
# -*- coding: utf-8 -*-

import os

from campbellsciparser import cr

TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')


def assert_two_lists_equal(list_1, list_2):
    assert len(list_1) == len(list_2) and (sorted(list_1) == sorted(list_2))


def assert_two_data_sets_equal(list_1, list_2):
    assert len(list_1) == len(list_2)

    for dict_1, dict_2 in zip(list_1, list_2):
        for name_value_1, name_value_2 in zip(dict_1.items(), dict_2.items()):
            assert name_value_1 == name_value_2


def test_filter_array_ids_data_no_filter():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_mixed_array_10_rows.dat')

    data_mixed = cr.read_mixed_array_data(infile_path=file)
    data_split = cr.read_array_ids_data(infile_path=file)
    data_filtered = cr.filter_data_by_array_ids(data=data_split)
    data_split_merged = [row for array_id, array_id_data in data_filtered.items()
                         for row in array_id_data]

    assert len(data_mixed) == len(data_split_merged)


def test_filter_array_ids_data_filter_all():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_mixed_array_10_rows.dat')

    data_mixed = cr.read_mixed_array_data(infile_path=file)
    data_split = cr.read_array_ids_data(infile_path=file)
    data_split_filtered = cr.filter_data_by_array_ids(
        data_split, '201', '203', '204', '210')

    data_split_merged = [row for array_id, array_id_data in data_split_filtered.items()
                         for row in array_id_data]

    assert len(data_mixed) == len(data_split_merged)


def test_filter_array_ids_data_filter():
    file_unfiltered = os.path.join(
        TEST_DATA_DIR, 'csv_testdata_mixed_array_10_rows.dat')

    file_filtered = os.path.join(
        TEST_DATA_DIR, 'csv_testdata_mixed_array_filtered_rows.dat')

    data_pre_filtered = cr.read_mixed_array_data(infile_path=file_filtered)
    data_split_unfiltered = cr.read_array_ids_data(infile_path=file_unfiltered)
    data_split_filtered = cr.filter_data_by_array_ids(
        data_split_unfiltered, '201', '203')

    data_split_filtered_merged = [
        row for array_id, array_id_data in data_split_filtered.items()
        for row in array_id_data
    ]

    len(data_pre_filtered) == len(data_split_filtered_merged)