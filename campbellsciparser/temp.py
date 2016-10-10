#!/usr/bin/env
# -*- coding: utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from campbellsciparser import device

if __name__ == '__main__':
    cr10 = device.CR10Parser('Etc/GMT-1', ['%j', '%H%M'])
    print(cr10.__dict__)