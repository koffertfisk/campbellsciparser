#!/usr/bin/env
# -*- coding: utf-8 -*-

import os
import tempfile

import pytest

from campbellsciparser import cr
from campbellsciparser.dataset import DataSet
from campbellsciparser.dataset import Row

TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')


def assert_two_lists_equal(list_1, list_2):
    assert len(list_1) == len(list_2) and (sorted(list_1) == sorted(list_2))


def assert_two_data_sets_equal(list_1, list_2):
    assert len(list_1) == len(list_2)

    for dict_1, dict_2 in zip(list_1, list_2):
        for name_value_1, name_value_2 in zip(dict_1.items(), dict_2.items()):
            assert name_value_1 == name_value_2


def test_export_array_ids_to_csv_empty_library():
    with pytest.raises(cr.ArrayIdsInfoValueError):
        cr.export_array_ids_to_csv(data={}, array_ids_info={})


def test_export_array_ids_to_csv_insufficient_info():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_mixed_array_10_rows.dat')
    data = cr.read_array_ids_data(infile_path=file)

    with pytest.raises(cr.ArrayIdsExportInfoError):
        cr.export_array_ids_to_csv(data=data, array_ids_info={'201': None})

    with pytest.raises(cr.ArrayIdsExportInfoError):
        cr.export_array_ids_to_csv(data=data, array_ids_info={'201': {}})


def test_export_array_ids_to_csv_content():
    with tempfile.TemporaryDirectory() as temp_dir:
        output_file_100 = os.path.join(temp_dir, 'test_100.dat')
        output_file_101 = os.path.join(temp_dir, 'test_101.dat')

        data = {
            '100': DataSet([Row([(0, '100'), (1, '2016'), (2, '123'), (3, '54.2')])]),
            '101': DataSet([Row([
                (0, '101'), (1, '2016'), (2, '123'), (3, '1245'), (4, '44.2')
            ])])
        }

        cr.export_array_ids_to_csv(
            data=data,
            array_ids_info={
                '100': {'file_path': output_file_100},
                '101': {'file_path': output_file_101}
            }
        )

        data_exported_file_100 = cr.read_mixed_array_data(infile_path=output_file_100)
        assert_two_data_sets_equal(data.get('100'), data_exported_file_100)

        data_exported_file_101 = cr.read_mixed_array_data(infile_path=output_file_101)
        assert_two_data_sets_equal(data.get('101'), data_exported_file_101)


def test_export_array_ids_to_csv_headers():
    with tempfile.TemporaryDirectory() as temp_dir:
        output_file_100 = os.path.join(temp_dir, 'test_100.dat')
        output_file_101 = os.path.join(temp_dir, 'test_101.dat')

        data = {
            '100': DataSet([Row([
                ('ID', '100'), ('Year', '2016'), ('Julian Day', '123'), ('Data', '54.2')])]),
            '101': DataSet([Row([
                ('ID', '101'), ('Year', '2016'), ('Julian Day', '123'),
                ('Hour/Minute', '1245'), ('Data', '44.2')])])
        }

        data_100_column_names = list(data.get('100')[0].keys())
        data_101_column_names = list(data.get('101')[0].keys())

        array_id_info_100 = {'file_path': output_file_100}
        array_id_info_101 = {'file_path': output_file_101}
        array_ids_info = {'100': array_id_info_100, '101': array_id_info_101}

        cr.export_array_ids_to_csv(
            data=data, array_ids_info=array_ids_info, export_header=True)

        data_exported_file_100 = cr.read_mixed_array_data(infile_path=output_file_100)
        data_exported_file_101 = cr.read_mixed_array_data(infile_path=output_file_101)

        data_exported_file_100_headers_row = data_exported_file_100[0]
        data_exported_file_100_headers = [value for value
                                          in data_exported_file_100_headers_row.values()]

        data_exported_file_101_headers_row = data_exported_file_101[0]
        data_exported_file_101_headers = [value for value
                                          in data_exported_file_101_headers_row.values()]

        assert_two_lists_equal(data_exported_file_100_headers, data_100_column_names)
        assert_two_lists_equal(data_exported_file_101_headers, data_101_column_names)
