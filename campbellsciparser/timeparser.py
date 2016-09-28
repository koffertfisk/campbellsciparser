#!/usr/bin/env
# -*- coding: utf-8 -*-

"""Module for parsing and converting Campbell data logger time formats. """

import pytz
import time

from collections import namedtuple
from datetime import datetime


class TimeConversionException(Exception):
    """Base class for exceptions in this module"""
    pass


class TimeColumnValueError(ValueError):
    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)

class UnsupportedTimeFormatError(ValueError):
    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)

class CampbellTimeParser(object):
    """Base class for converting Campbell time formats to timestamp. """

    def __init__(self, time_zone, time_format_library):
        """Class initializer.
        Args:
            time_zone (string): Raw data pytz time zone, used for localization.
            time_format_library (list): List of expected time string representations.

        """

        self.time_zone = pytz.timezone(time_zone)
        self.time_format_library = time_format_library

    def parse_custom_format(self, *args):
        return {}

    def parse_time(self, *args, to_utc=False):
        """Converts the given raw data time format to a datetime object (local time zone -> UTC).
        Args:
            args:
        """
        parsed_time_info = self.parse_custom_format(*args)

        try:
            t = time.strptime(parsed_time_info.get('parsed_time'), parsed_time_info.get('parsed_time_format'))
            dt = datetime.fromtimestamp(time.mktime(t))
            loc_dt = self.time_zone.localize(dt)
        except ValueError:
            print("Could not parse time string {0} using the format {1}".format(
                parsed_time_info.get('parsed_time'), parsed_time_info.get('parsed_time_format'))
            )
            loc_dt = datetime.fromtimestamp(0, self.time_zone)

        parsed_dt = loc_dt
        if to_utc:
            utc_dt = loc_dt.astimezone(pytz.utc)
            parsed_dt = utc_dt

        return parsed_dt


class CR10XTimeParser(TimeParser):

    def __init__(self, time_zone='UTC'):
        TimeParser.__init__(self, time_zone=time_zone, time_format_library=['%Y', '%j', 'Hour/Minute'])

    def parse_custom_format(self, *timevalues):
        """Parses the custom time format CR10X.
        Args:
            timeargs (string): Time strings to be parsed.
        Returns:
            Parsed time format string representation and value.

        """
        time_values = list(timevalues)
        if len(time_values) > 2:
            raise UnsupportedTimeFormatError(
                "CR10XTimeParser only supports Year, Day, Hour/Minute, got {0} time values".format(len(time_values)))
        found_time_format_args = []
        parsing_info = namedtuple('ParsedTimeInfo', ['parsed_time_format', 'parsed_time'])

        for i, value in enumerate(time_values):
            found_time_format_args.append(self.time_format_library[i])
            if i == 2:     # Time string "Hour/Minute" reached
                parsed_time_format, parsed_time = self._parse_hourminute(value)
                found_time_format_args[2] = parsed_time_format
                time_values[2] = parsed_time

        time_format_str = ','.join(found_time_format_args)
        time_values_str = ','.join(time_values)

        return parsing_info(time_format_str, time_values_str)

    def _parse_hourminute(self, hour_minute_str):
        """Parses the CR10X time format column 'Hour/Minute'.

        Args:
            hour_minute_str (string): Hour/Minute string to be parsed.
        Returns:
            The time parsed in the format HH:MM.

        """
        hour = 0
        parsed_time_format = "%H:%M"
        parsed_time = ""
        parsing_info = namedtuple('ParsedHourMinInfo', ['parsed_time_format', 'parsed_time'])

        if len(hour_minute_str) == 1:            # 0 - 9
            minute = hour_minute_str
            parsed_time = "00:0" + minute
        elif len(hour_minute_str) == 2:           # 10 - 59
            minute = hour_minute_str[-2:]
            parsed_time = "00:" + minute
        elif len(hour_minute_str) == 3:          # 100 - 959
            hour = hour_minute_str[:1]
            minute = hour_minute_str[-2:]
            parsed_time = "0" + hour + ":" + minute
        elif len(hour_minute_str) == 4:          # 1000 - 2359
            hour = hour_minute_str[:2]
            minute = hour_minute_str[-2:]
            parsed_time = hour + ":" + minute
        else:
            raise ValueError("Hour/Minute {0} could not be parsed!".format(hour_minute_str))

        return parsing_info(parsed_time_format, parsed_time)
