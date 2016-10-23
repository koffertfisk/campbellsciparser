#!/usr/bin/env
# -*- coding: utf-8 -*-

import os
import shutil

import pytest

from campbellsciparser import cr

TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')


def assert_two_lists_equal(list_1, list_2):
    assert len(list_1) == len(list_2) and (sorted(list_1) == sorted(list_2))


def assert_two_data_sets_equal(list_1, list_2):
    assert len(list_1) == len(list_2)

    for dict_1, dict_2 in zip(list_1, list_2):
        for name_value_1, name_value_2 in zip(dict_1.items(), dict_2.items()):
            assert name_value_1 == name_value_2


def delete_output(file_path):
    dirname = os.path.dirname(file_path)
    shutil.rmtree(dirname)


def test_export_array_ids_to_csv_empty_library():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_mixed_array_10_rows.dat')
    data_split = cr.read_array_ids_data(infile_path=file)
    with pytest.raises(cr.ArrayIdsInfoError):
        cr.export_array_ids_to_csv(data=data_split, array_ids_info={})


def test_export_array_ids_to_csv_empty_info():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_mixed_array_10_rows.dat')
    data_split = cr.read_array_ids_data(infile_path=file)
    with pytest.raises(cr.ArrayIdsExportInfoError):
        cr.export_array_ids_to_csv(data=data_split, array_ids_info={'201': None})


def test_export_array_ids_to_csv_no_file_path():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_mixed_array_10_rows.dat')
    data_split = cr.read_array_ids_data(infile_path=file)
    with pytest.raises(cr.ArrayIdsExportInfoError):
        cr.export_array_ids_to_csv(data=data_split, array_ids_info={'201': {}})


def test_export_array_ids_to_csv_content():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_mixed_array_10_rows.dat')
    output_file = os.path.join(TEST_DATA_DIR, 'testoutput/test.dat')

    data_split_unfiltered = cr.read_array_ids_data(infile_path=file)
    data_split_filtered = cr.filter_data_by_array_ids(data_split_unfiltered, '201')
    data_split_array_id_filtered = data_split_filtered.get('201')

    cr.export_array_ids_to_csv(
        data=data_split_unfiltered,
        array_ids_info={'201': {'file_path': output_file}})

    data_exported_file = cr.read_mixed_array_data(infile_path=output_file)

    assert_two_data_sets_equal(data_split_array_id_filtered, data_exported_file)

    delete_output(output_file)


def test_export_array_ids_to_csv_column_names():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_mixed_array_10_rows.dat')
    output_file_1 = os.path.join(TEST_DATA_DIR, 'testoutput/test_1.dat')
    output_file_2 = os.path.join(TEST_DATA_DIR, 'testoutput/test_2.dat')

    data_split_unfiltered = cr.read_array_ids_data(infile_path=file)
    data_split_filtered = cr.filter_data_by_array_ids(
        data_split_unfiltered, '201', '203')

    data_split_filtered_201 = data_split_filtered.get('201')
    data_split_filtered_203 = data_split_filtered.get('203')

    data_split_filtered_201_first_row = data_split_filtered_201[0]
    data_split_filtered_203_first_row = data_split_filtered_203[0]

    data_split_filtered_201_headers = [str(key) for key
                                       in data_split_filtered_201_first_row.keys()]
    data_split_filtered_203_headers = [str(key) for key
                                       in data_split_filtered_203_first_row.keys()]

    array_id_info_1 = {'file_path': output_file_1}
    array_id_info_2 = {'file_path': output_file_2}
    array_ids_info = {'201': array_id_info_1, '203': array_id_info_2}

    cr.export_array_ids_to_csv(
        data=data_split_filtered, array_ids_info=array_ids_info, export_header=True)

    data_exported_file_1 = cr.read_mixed_array_data(infile_path=output_file_1)
    data_exported_file_2 = cr.read_mixed_array_data(infile_path=output_file_2)

    data_exported_file_1_headers_row = data_exported_file_1[0]
    data_exported_file_1_headers = [value for value
                                    in data_exported_file_1_headers_row.values()]

    data_exported_file_2_headers_row = data_exported_file_2[0]
    data_exported_file_2_headers = [value for value
                                    in data_exported_file_2_headers_row.values()]

    assert_two_lists_equal(data_split_filtered_201_headers, data_exported_file_1_headers)
    assert_two_lists_equal(data_split_filtered_203_headers, data_exported_file_2_headers)

    delete_output(output_file_1)


def test_export_array_ids_to_csv_updated_column_names():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_mixed_array_10_rows.dat')
    output_file = os.path.join(TEST_DATA_DIR, 'testoutput/test.dat')

    data_split_unfiltered = cr.read_array_ids_data(infile_path=file)
    data_split_filtered = cr.filter_data_by_array_ids(data_split_unfiltered, '203')
    data_split_array_id_filtered = data_split_filtered.get('203')

    column_names = ['Array_Id', 'Year', 'Day', 'Hour/Minute', 'Wind_Speed', 'Wind_Direction']
    data_updated_headers = cr.update_column_names(
        data=data_split_array_id_filtered, column_names=column_names)

    array_ids_info = {'203': {'file_path': output_file}}
    cr.export_array_ids_to_csv(
        data=data_updated_headers, array_ids_info=array_ids_info, export_header=True)

    data_exported_file = cr.read_mixed_array_data(infile_path=output_file)
    data_updated_column_names_first_row = data_exported_file[0]
    data_updated_column_names = [value for value
                                    in data_updated_column_names_first_row.values()]

    assert_two_lists_equal(column_names, data_updated_column_names)

    delete_output(output_file)
