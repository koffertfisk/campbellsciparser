#!/usr/bin/env
# -*- coding: utf-8 -*-

from datetime import datetime

import pytz

from campbellsciparser.devices import CR1000Parser


def test_parse_time():
    time_zone = 'UTC'
    timestamp = '2016-01-30 22:30:00'
    parsed_time_expected = datetime(2016, 1, 30, 22, 30, 0, tzinfo=pytz.UTC)

    cr10x = CR1000Parser(time_zone)
    parsed_time = cr10x._parse_time_values(*[timestamp], to_utc=False)

    assert parsed_time == parsed_time_expected
