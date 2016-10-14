#!/usr/bin/env
# -*- coding: utf-8 -*-

from campbellsciparser.devices.base import CampbellSCIBaseParser
from campbellsciparser.devices.base import UnknownPytzTimeZoneError
from tests.devices.common_all import *


class BaseParserSetupTest(unittest.TestCase):
    def test_valid_pytz_time_zone(self):
        time_zone = 'Europe/Stockholm'
        try:
            baseparser = CampbellSCIBaseParser(pytz_time_zone=time_zone)
        except UnknownPytzTimeZoneError:
            self.fail("Exception raised unexpectedly")

    def test_invalid_pytz_time_zone(self):
        time_zone = 'Foo'
        with self.assertRaises(UnknownPytzTimeZoneError):
            baseparser = CampbellSCIBaseParser(pytz_time_zone=time_zone)