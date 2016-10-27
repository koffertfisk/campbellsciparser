#!/usr/bin/env
# -*- coding: utf-8 -*-

from collections import OrderedDict


class DataSet(list):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class Row(OrderedDict):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
