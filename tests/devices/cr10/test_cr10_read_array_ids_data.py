#!/usr/bin/env
# -*- coding: utf-8 -*-

import os

from campbellsciparser.devices import CR10Parser

TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')


def assert_two_lists_equal(list_1, list_2):
    assert len(list_1) == len(list_2) and (sorted(list_1) == sorted(list_2))


def assert_two_data_sets_equal(list_1, list_2):
    assert len(list_1) == len(list_2)

    for dict_1, dict_2 in zip(list_1, list_2):
        for name_value_1, name_value_2 in zip(dict_1.items(), dict_2.items()):
            assert name_value_1 == name_value_2


def test_empty():
    file = os.path.join(TEST_DATA_DIR, 'csv_cr10_testdata_empty.dat')
    cr10 = CR10Parser()
    data = cr10.read_array_ids_data(infile_path=file)

    assert len(data) == 0


def test_length_ten_rows():
    file = os.path.join(TEST_DATA_DIR, 'csv_cr10_testdata_10_rows.dat')
    cr10 = CR10Parser()
    data = cr10.read_array_ids_data(infile_path=file)
    data_mixed = [row for array_id, array_id_data in data.items()
                  for row in array_id_data]

    assert len(data_mixed) == 10


def test_compare_length_ten_rows():
    file = os.path.join(TEST_DATA_DIR, 'csv_cr10_testdata_10_rows.dat')
    cr10 = CR10Parser()

    data_mixed = cr10.read_mixed_data(infile_path=file)
    data_split = cr10.read_array_ids_data(infile_path=file)
    data_split_merged = [row for array_id, array_id_data in data_split.items()
                         for row in array_id_data]

    assert len(data_mixed) == len(data_split_merged)


def test_compare_data_ten_rows():
    file = os.path.join(TEST_DATA_DIR, 'csv_cr10_testdata_10_rows.dat')
    cr10 = CR10Parser()

    data_mixed = cr10.read_mixed_data(infile_path=file)
    data_split = cr10.read_array_ids_data(infile_path=file)
    data_split_merged = [row for array_id, array_id_data in data_split.items()
                         for row in array_id_data]

    assert len(data_mixed) == len(data_split_merged)


def test_compare_data_ten_rows_lookup():
    file = os.path.join(TEST_DATA_DIR, 'csv_cr10_testdata_10_rows.dat')
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

    assert len(data_mixed) == len(data_split_translated_merged)