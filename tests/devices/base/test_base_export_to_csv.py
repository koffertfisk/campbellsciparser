#!/usr/bin/env
# -*- coding: utf-8 -*-

import os.path

from collections import OrderedDict
from datetime import datetime

import pytz

from campbellsciparser.devices.base import CampbellSCIBaseParser
from tests.devices.common_all import *


class BaseParserExportToCsvTest(ExportFileTestCase, ReadDataTestCase):

    def test_export_to_csv_file_created(self):
        file = os.path.join(TEST_DEVICES_DIR, 'testdata/base/csv_base_testdata_10_rows.dat')
        output_file = os.path.join(TEST_DEVICES_DIR, 'testoutput/test.dat')
        baseparser = CampbellSCIBaseParser()

        data = baseparser.read_data(infile_path=file)
        baseparser.export_to_csv(data=data, outfile_path=output_file)

        self.assertExportedFileExists(output_file)
        self.delete_output(output_file)

    def test_export_to_csv_file_content(self):
        file = os.path.join(TEST_DEVICES_DIR, 'testdata/base/csv_base_testdata_10_rows.dat')
        output_file = os.path.join(TEST_DEVICES_DIR, 'testoutput/test.dat')
        baseparser = CampbellSCIBaseParser()

        data_source_file = baseparser.read_data(infile_path=file)
        baseparser.export_to_csv(data=data_source_file, outfile_path=output_file)
        data_exported_file = baseparser.read_data(infile_path=output_file)

        self.assertTwoDataSetsLengthEqual(data_1=data_source_file, data_2=data_exported_file)
        self.delete_output(output_file)

    def test_export_to_csv_file_headers(self):
        file = os.path.join(TEST_DEVICES_DIR, 'testdata/base/csv_base_testdata_10_rows.dat')
        output_file = os.path.join(TEST_DEVICES_DIR, 'testoutput/test.dat')
        baseparser = CampbellSCIBaseParser()
        headers = ['Label_' + str(i) for i in range(3)]

        data_source_file = baseparser.read_data(infile_path=file, headers=headers)
        baseparser.export_to_csv(data=data_source_file, outfile_path=output_file,
                                 export_headers=True)
        data_exported_file = baseparser.read_data(infile_path=output_file, header_row=0)

        for row in data_exported_file:
            for exported_row_name, expected_row_name in zip(list(row.keys()), headers):
                self.assertEqual(exported_row_name, expected_row_name)

        self.delete_output(output_file)

    def test_export_to_csv_file_time_zone(self):
        time_zone = 'Europe/Stockholm'
        pytz_time_zone = pytz.timezone(time_zone)
        dt = datetime(2016, 1, 1, 22, 15, 30, tzinfo=pytz_time_zone)
        data = [OrderedDict([('TIMESTAMP', dt)])]

        output_file = os.path.join(TEST_DEVICES_DIR, 'testoutput/test.dat')
        baseparser = CampbellSCIBaseParser(pytz_time_zone=time_zone)

        baseparser.export_to_csv(
            data=data,
            outfile_path=output_file,
            export_headers=True,
            include_time_zone=True)

        data_exported_file = baseparser.read_data(infile_path=output_file, header_row=0)

        data_exported_file_first_row = data_exported_file[0]
        exported_time_str = data_exported_file_first_row.get('TIMESTAMP')
        exported_time_dt = datetime.strptime(exported_time_str, '%Y-%m-%d %H:%M:%S%z')

        self.assertEqual(exported_time_dt, dt)
        self.delete_output(output_file)

    def test_export_to_csv_file_no_time_zone(self):
        time_zone = 'Europe/Stockholm'
        pytz_time_zone = pytz.timezone(time_zone)
        dt = datetime(2016, 1, 1, 22, 15, 30, tzinfo=pytz_time_zone)
        expected_dt = datetime.strptime(
            dt.strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')

        data = [OrderedDict([('TIMESTAMP', dt)])]

        output_file = os.path.join(TEST_DEVICES_DIR, 'testoutput/test.dat')
        baseparser = CampbellSCIBaseParser(pytz_time_zone=time_zone)

        baseparser.export_to_csv(
            data=data,
            outfile_path=output_file,
            export_headers=True,
            include_time_zone=False)

        data_exported_file = baseparser.read_data(infile_path=output_file, header_row=0)

        data_exported_file_first_row = data_exported_file[0]
        exported_time_str = data_exported_file_first_row.get('TIMESTAMP')
        exported_time_dt = datetime.strptime(exported_time_str, '%Y-%m-%d %H:%M:%S')

        self.assertEqual(exported_time_dt, expected_dt)
        self.delete_output(output_file)
