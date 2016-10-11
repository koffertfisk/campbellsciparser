#!/usr/bin/env
# -*- coding: utf-8 -*-

import os.path
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from campbellsciparser.devices.base import CampbellSCIBaseParser

if __name__ == '__main__':
    campbellparser = CampbellSCIBaseParser('Etc/GMT-1', [])
    #cr10x = CR10XParser('Etc/GMT-1')
    home = os.path.expanduser('~')
    fpath = os.path.join(home, 'Jobb', 'Data', 'data', 'csv_testdata_10_rows.dat')
    data = campbellparser.read_data(infile_path=fpath, convert_time=True, time_columns=[1])
    print(data)
    #data_split = cr10x.filter_data_by_array_ids(data, '201')
    #data_converted = cr10x.convert_time(data=data_split.get('201'), time_columns=[1,2,3])