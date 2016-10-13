#!/usr/bin/env
# -*- coding: utf-8 -*-

from campbellsciparser.devices.base import *


class CR1000Parser(CampbellSCIBaseParser):
    """Parses and exports data files collected by Campbell Scientific CR1000 data loggers. """

    def __init__(self, pytz_time_zone='UTC', time_format_args_library=None):
        """Initializes the data logger parser with time arguments for the CR1000 model.

        Args:
            pytz_time_zone (str): Data pytz time zone, used for localization. See pytz docs for reference.
            time_format_args_library (list): List of expected time string representations.

        """
        if not time_format_args_library:
            time_format_args_library = ['%Y-%m-%d %H:%M:%S']

        super().__init__(pytz_time_zone, time_format_args_library)

    def _parse_custom_time_format(self, *timevalues):
        """Parses the CR1000 custom time format.

        Args:
            *timevalues (str): Time strings to be parsed.

        Returns:
            Parsed time format string representation and value.

        Raises:
            IndexError: If the time format arguments library is empty or no time value is given.

        """
        parsing_info = namedtuple('ParsedTimeInfo', ['parsed_time_format', 'parsed_time'])

        return parsing_info(self.time_format_args_library[0], timevalues[0])
