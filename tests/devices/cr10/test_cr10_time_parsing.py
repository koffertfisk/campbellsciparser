#!/usr/bin/env
# -*- coding: utf-8 -*-

import unittest

from datetime import datetime

import pytz

from campbellsciparser.devices.cr10 import CR10Parser


class CR10TimeParsingTest(unittest.TestCase):

    def test_parse_time(self):
        time_zone = 'UTC'
        year = '16'
        day = '30'
        hour_minute = '2230'
        parsed_time_expected = datetime(2016, 1, 30, 22, 30, 0, tzinfo=pytz.UTC)

        cr10 = CR10Parser(time_zone)
        parsed_time = cr10._parse_time_values(*[year, day, hour_minute], to_utc=False)

        self.assertEqual(parsed_time, parsed_time_expected)
