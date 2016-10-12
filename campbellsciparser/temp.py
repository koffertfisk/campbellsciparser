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
    fpath = os.path.join(home, 'Jobb', 'Data', 'data', 'csv_base_testdata_3_rows_headers.dat')
    baseparser = CampbellSCIBaseParser()
    #row_1 = OrderedDict([('Label_0', '1')])
    #row_2 = OrderedDict([('Label_0', '1'), ('Label_1', '2')])
    #row_3 = OrderedDict([('Label_0', '1'), ('Label_1', '2'), ('Label_2', '3')])

    for row in baseparser._process_rows(infile_path=fpath, header_row=0, line_num=0):
        print(row)