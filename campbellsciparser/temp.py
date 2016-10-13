#!/usr/bin/env
# -*- coding: utf-8 -*-

import os.path
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collections import OrderedDict

from campbellsciparser.devices.base import CampbellSCIBaseParser

if __name__ == '__main__':
    #campbellparser = CampbellSCIBaseParser('Etc/GMT-1', [])
    #cr10x = CR10XParser('Etc/GMT-1')
    home = os.path.expanduser('~')
    fpath = os.path.join(home, 'Jobb', 'Data', 'data', 'csv_all_testdata_3_rows_headers.dat')
    baseparser = CampbellSCIBaseParser()

    data = baseparser.read_data(infile_path=fpath, header_row=0)
    headers = ['A', 'B', 'C']
    data_updated = baseparser.update_headers(data=data, headers=headers, match_row_lengths=True)
    print(data_updated)
