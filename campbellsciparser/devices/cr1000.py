# -*- coding: utf-8 -*-
"""
CR1000Parser
---------------------
Utility for parsing and exporting data collected by Campbell Scientific CR1000 dataloggers.

"""

from campbellsciparser.devices.base import *


class CR1000Parser(CampbellSCIBaseParser):
    """Custom parser setup for the CR1000.

    CR1000 datalogger specific details:

        * The CR1000 typically outputs each rows' timestamp using the format
        Year-Month-Day Hour:Minute:Seconds. The parser's default time
        format arguments library is set to match this pattern. It will, in other words,
        be able to parse the following example time values: ['2016-01-01 22:30:15'].

    Args
    ----
    pytz_time_zone (str): String representation of a valid pytz time zone. (See pytz docs
        for more information). The time zone refers to collected data's time zone, which
        defaults to UTC and is used for localization and time conversion.
    time_format_args_library (list): List of the maximum expected string format columns
        sequence to match against when parsing time values. Defaults to the most common
        CR1000 time representation setup.

    """
    def __init__(self, pytz_time_zone='UTC', time_format_args_library=None):

        if not time_format_args_library:
            time_format_args_library = ['%Y-%m-%d %H:%M:%S']

        super().__init__(pytz_time_zone, time_format_args_library)
