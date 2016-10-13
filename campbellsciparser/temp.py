#!/usr/bin/env
# -*- coding: utf-8 -*-

import os.path
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collections import OrderedDict

from campbellsciparser.devices.cr1000 import CR1000Parser

if __name__ == '__main__':
    #campbellparser = CampbellSCIBaseParser('Etc/GMT-1', [])
    #cr10x = CR10XParser('Etc/GMT-1')
    home = os.path.expanduser('~')
    fpath = os.path.join(home, 'Jobb', 'Data', 'data', 'cr1000test.dat')
    cr1000 = CR1000Parser()
    data = cr1000.read_data(infile_path=fpath, convert_time=True, time_columns=[0])
    print(data)
    #data = baseparser.read_data(infile_path=fpath, header_row=0)
    #headers = ['A', 'B', 'C']
    #data_updated = baseparser.update_column_names(data=data, headers=headers, match_row_lengths=True)
    #print(data_updated)
