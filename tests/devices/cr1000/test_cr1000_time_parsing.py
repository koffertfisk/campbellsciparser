#!/usr/bin/env
# -*- coding: utf-8 -*-

import unittest

from datetime import datetime

import pytz

from campbellsciparser.devices.cr1000 import CR1000Parser


class CR1000TimeParsingTest(unittest.TestCase):

    def test_parse_time(self):
        time_zone = 'UTC'
        timestamp = '2016-01-30 22:30:00'
        parsed_time_expected = datetime(2016, 1, 30, 22, 30, 0, tzinfo=pytz.UTC)

        cr10x = CR1000Parser(time_zone)
        parsed_time = cr10x._parse_time_values(*[timestamp], to_utc=False)

        self.assertEqual(parsed_time, parsed_time_expected)
