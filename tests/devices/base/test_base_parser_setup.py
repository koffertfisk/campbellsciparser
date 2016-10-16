#!/usr/bin/env
# -*- coding: utf-8 -*-

import pytest

from campbellsciparser.devices import CampbellSCIBaseParser
from campbellsciparser.devices import UnknownPytzTimeZoneError


def test_valid_pytz_time_zone():
    time_zone = 'Europe/Stockholm'
    try:
        baseparser = CampbellSCIBaseParser(pytz_time_zone=time_zone)
    except UnknownPytzTimeZoneError:
        pytest.fail("Exception raised unexpectedly")


def test_invalid_pytz_time_zone():
    time_zone = 'Foo'
    with pytest.raises(UnknownPytzTimeZoneError):
        baseparser = CampbellSCIBaseParser(pytz_time_zone=time_zone)
