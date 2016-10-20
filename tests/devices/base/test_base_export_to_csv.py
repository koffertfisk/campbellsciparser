#!/usr/bin/env
# -*- coding: utf-8 -*-

import os
import shutil

from collections import OrderedDict
from datetime import datetime

import pytz

from campbellsciparser.devices import CRGeneric

TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')


def assert_two_data_sets_equal(list_1, list_2):
    assert len(list_1) == len(list_2)

    for dict_1, dict_2 in zip(list_1, list_2):
        for name_value_1, name_value_2 in zip(dict_1.items(), dict_2.items()):
            assert name_value_1 == name_value_2


def delete_output(file_path):
    dirname = os.path.dirname(file_path)
    shutil.rmtree(dirname)


def test_export_to_csv_file_created():
    file = os.path.join(TEST_DATA_DIR, 'csv_base_testdata_10_rows.dat')
    output_file = os.path.join(TEST_DATA_DIR, 'testoutput/test.dat')
    baseparser = CRGeneric()

    data = baseparser.read_data(infile_path=file)
    baseparser.export_to_csv(data=data, outfile_path=output_file)

    assert os.path.exists(output_file)
    delete_output(output_file)


def test_export_to_csv_file_content():
    file = os.path.join(TEST_DATA_DIR, 'csv_base_testdata_10_rows.dat')
    output_file = os.path.join(TEST_DATA_DIR, 'testoutput/test.dat')
    baseparser = CRGeneric()

    data_source_file = baseparser.read_data(infile_path=file)
    baseparser.export_to_csv(data=data_source_file, outfile_path=output_file)
    data_exported_file = baseparser.read_data(infile_path=output_file)

    assert_two_data_sets_equal(data_source_file, data_exported_file)
    delete_output(output_file)


def test_export_to_csv_file_headers():
    file = os.path.join(TEST_DATA_DIR, 'csv_base_testdata_10_rows.dat')
    output_file = os.path.join(TEST_DATA_DIR, 'testoutput/test.dat')
    baseparser = CRGeneric()
    headers = ['Label_' + str(i) for i in range(3)]

    data_source_file = baseparser.read_data(infile_path=file, headers=headers)
    baseparser.export_to_csv(data=data_source_file, outfile_path=output_file,
                             export_headers=True)
    data_exported_file = baseparser.read_data(infile_path=output_file, header_row=0)

    for row in data_exported_file:
        for exported_row_name, expected_row_name in zip(list(row.keys()), headers):
            assert exported_row_name == expected_row_name

    delete_output(output_file)


def test_export_to_csv_file_time_zone():
    time_zone = 'Europe/Stockholm'
    pytz_time_zone = pytz.timezone(time_zone)
    dt = datetime(2016, 1, 1, 22, 15, 30, tzinfo=pytz_time_zone)
    data = [OrderedDict([('TIMESTAMP', dt)])]

    output_file = os.path.join(TEST_DATA_DIR, 'testoutput/test.dat')
    baseparser = CRGeneric(pytz_time_zone=time_zone)

    baseparser.export_to_csv(
        data=data,
        outfile_path=output_file,
        export_headers=True,
        include_time_zone=True)

    data_exported_file = baseparser.read_data(infile_path=output_file, header_row=0)

    data_exported_file_first_row = data_exported_file[0]
    exported_time_str = data_exported_file_first_row.get('TIMESTAMP')
    exported_time_dt = datetime.strptime(exported_time_str, '%Y-%m-%d %H:%M:%S%z')

    assert exported_time_dt == dt

    delete_output(output_file)


def test_export_to_csv_file_no_time_zone():
    time_zone = 'Europe/Stockholm'
    pytz_time_zone = pytz.timezone(time_zone)
    dt = datetime(2016, 1, 1, 22, 15, 30, tzinfo=pytz_time_zone)
    expected_dt = datetime.strptime(
        dt.strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')

    data = [OrderedDict([('TIMESTAMP', dt)])]

    output_file = os.path.join(TEST_DATA_DIR, 'testoutput/test.dat')
    baseparser = CRGeneric(pytz_time_zone=time_zone)

    baseparser.export_to_csv(
        data=data,
        outfile_path=output_file,
        export_headers=True,
        include_time_zone=False)

    data_exported_file = baseparser.read_data(infile_path=output_file, header_row=0)

    data_exported_file_first_row = data_exported_file[0]
    exported_time_str = data_exported_file_first_row.get('TIMESTAMP')
    exported_time_dt = datetime.strptime(exported_time_str, '%Y-%m-%d %H:%M:%S')

    assert exported_time_dt == expected_dt

    delete_output(output_file)
