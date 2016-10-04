#!/usr/bin/env
# -*- coding: utf-8 -*-

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from campbellsciparser import device

if __name__ == '__main__':
    fpath = 'C:/Users/Marcus/Desktop/data/OutputFrom2016/Island_Output5min.dat'
    cr1000 = device.CR1000(time_zone='Etc/GMT-1')
    data = cr1000.read_data(fpath, line_num=2, convert_time=True, time_columns=[0])
    print(data[0])