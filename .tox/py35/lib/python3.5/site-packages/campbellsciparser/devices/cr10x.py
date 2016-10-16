# -*- coding: utf-8 -*-
"""
CR10XParser
---------------------
Utility for parsing and exporting data collected by Campbell Scientific CR10X dataloggers.

"""

from campbellsciparser.devices.cr10 import * 


class CR10XParser(CR10Parser):
    """Custom parser setup for the CR10X.

    CR10X datalogger specific details:

        * The CR10X output data almost identical to its predecessor (CR10), with the only
        notable difference that the year is output with century as a decimal number, hence,
        the default time format arguments library configuration is customized to adapt to
        this change. Thus, the default configuration would be able to parse the following
        example time values: ['2016', '1', '1315'].

    Args
    ----
    pytz_time_zone (str): String representation of a valid pytz time zone. (See pytz docs
        for more information). The time zone refers to collected data's time zone, which
        defaults to UTC and is used for localization and time conversion.
    time_format_args_library (list): List of the maximum expected string format columns
        sequence to match against when parsing time values. Defaults to the most common
        CR10X time representation setup.

    """
    def __init__(self, pytz_time_zone='UTC', time_format_args_library=None):

        if not time_format_args_library:
            time_format_args_library = ['%Y', '%j', '%H%M']

        CampbellSCIBaseParser.__init__(self, pytz_time_zone, time_format_args_library)