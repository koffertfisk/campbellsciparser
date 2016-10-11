#!/usr/bin/env
# -*- coding: utf-8 -*-

from campbellsciparser.devices.cr10 import * 


class CR10XParser(CR10Parser):
    """Parses and exports data files collected by Campbell Scientific CR10X data loggers. """

    def __init__(self, time_zone='UTC'):
        """Initializes the data logger parser with time arguments for the CR10X model.

        Args:
            time_zone (str): Data pytz time zone, used for localization. See pytz docs for reference.

        """
        
        CampbellSCIBaseParser.__init__(self, time_zone, time_format_args_library=['%Y', '%j', '%H%M'])