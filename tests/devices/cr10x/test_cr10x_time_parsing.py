#!/usr/bin/env
# -*- coding: utf-8 -*-

from datetime import datetime

import pytz

from campbellsciparser.devices import CR10XParser


def test_parse_time():
    time_zone = 'UTC'
    year = '2016'
    day = '30'
    hour_minute = '2230'
    parsed_time_expected = datetime(2016, 1, 30, 22, 30, 0, tzinfo=pytz.UTC)

    cr10x = CR10XParser(time_zone)
    parsed_time = cr10x._parse_time_values(*[year, day, hour_minute], to_utc=False)

    assert parsed_time == parsed_time_expected
