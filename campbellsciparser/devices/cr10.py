#!/usr/bin/env
# -*- coding: utf-8 -*-

from collections import defaultdict

from campbellsciparser.devices.base import *


class ArrayIdsInfoError(ValueError):
    pass


class ArrayIdsExportInfoError(ArrayIdsInfoError):
    pass


class ArrayIdsFilePathError(ArrayIdsInfoError):
    pass


class CR10Parser(CampbellSCIBaseParser):
    """Parses and exports data files collected by Campbell Scientific CR10 data loggers. """
    
    def __init__(self, pytz_time_zone='UTC'):
        """Initializes the data logger parser with time arguments for the CR10 model.
        
        Args:
            pytz_time_zone (str): Data pytz time zone, used for localization. See pytz docs for reference.

        """
            
        super().__init__(pytz_time_zone, time_format_args_library=['%y', '%j', '%H%M'])
        
    def _parse_custom_time_format(self, *time_values):
        """Parses the CR10 custom time format.

        Args:
            *time_values (str): Time strings to be parsed.

        Returns:
            Parsed time format string representation and value.

        Raises:
            UnsupportedTimeFormatError: If more than three arguments is being passed.

        """
        
        if len(time_values) > 3:
            msg = "The CR10 time parser only supports Year, Day, Hour/Minute, got {0} time values"
            msg = msg.format(len(time_values))

            raise UnsupportedTimeFormatError(msg)
        
        parsing_info = namedtuple('ParsedTimeInfo', ['parsed_time_format', 'parsed_time'])
        found_time_format_args = []
        found_time_values = []
        
        for format_arg, time_arg in zip(self.time_format_args_library, time_values):
            if format_arg == '%H%M':
                time_arg = self._parse_hourminute(time_arg)
            found_time_format_args.append(format_arg)
            found_time_values.append(time_arg)

        time_format_str = ','.join(found_time_format_args)
        time_values_str = ','.join(found_time_values)

        return parsing_info(time_format_str, time_values_str)
    
    @staticmethod
    def _parse_hourminute(hour_minute_str):
        """
            Parses the CR10 custom time format column 'Hour/Minute'. The time in the format
            HHMM is determined by the length of the given time string since the CR10 does not 
            output hours and minutes with leading zeros (except for midnight, which is output as 0). 
            
            Examples of the CR10's 'Hour/Minute' pattern and how they would be parsed:
            
            CR10 time string: 5
            HHMM time string: 0005
            
            CR10 time string: 35
            HHMM time string: 0035
            
            CR10 time string: 159
            HHMM time string: 0159
            
            CR10 time string: 1337
            HHMM time string: 1337
            
        Args:
            hour_minute_str (str): Hour/Minute string to be parsed.

        Returns:
            The time parsed in the format HHMM.

        """
        parsed_time = ""

        if len(hour_minute_str) == 1:            # 0 - 9
            minute = hour_minute_str
            parsed_time = "000" + minute
        elif len(hour_minute_str) == 2:           # 10 - 59
            minute = hour_minute_str[-2:]
            parsed_time = "00" + minute
        elif len(hour_minute_str) == 3:          # 100 - 959
            hour = hour_minute_str[:1]
            minute = hour_minute_str[-2:]
            parsed_time = "0" + hour + minute
        elif len(hour_minute_str) == 4:          # 1000 - 2359
            hour = hour_minute_str[:2]
            minute = hour_minute_str[-2:]
            parsed_time = hour + minute
        else:
            raise ValueError("Hour/Minute {0} could not be parsed!".format(hour_minute_str))

        return parsed_time

    @staticmethod
    def _process_mixed_rows(infile_path, line_num=0, fix_floats=True):
        """Helper method for _read_data.

        Args:
            infile_path (str): Input file's absolute path.
            line_num (int): Line number to start at. NOTE: Zero-based numbering.
            fix_floats (bool): Correct leading zeros floating points since the CR10 does not output leading zeros.

        Returns:
            Each processed row represented as an OrderedDict, returned one at a time.

        """
        with open(infile_path, 'r') as f:
            rows = csv.reader(f)
            replacements = {'.': '0.', '-.': '-0.'}    # Patterns to look for
            for row in rows:
                if line_num <= (rows.line_num - 1):   # Correct reader for zero-based numbering
                    if fix_floats:
                        for i, value in enumerate(row):
                            for source, replacement in replacements.items():
                                if value.startswith(source):
                                    row[i] = value.replace(source, replacement)

                    yield OrderedDict([(i, value) for i, value in enumerate(row)])

    @staticmethod
    def _read_mixed_data(infile_path, line_num=0, fix_floats=True):
        """Produces a generator object of data read from given file input starting from a given line number.

        Args:
            infile_path (str): Input file's absolute path.
            line_num (int): Line number to start at. NOTE: Zero-based numbering.
            fix_floats (bool): Correct leading zeros floating points since the CR10 does not output leading zeros.

        Returns:
            Each processed row, one at a time.

        """
        for row in CR10Parser._process_mixed_rows(infile_path=infile_path, line_num=line_num, fix_floats=fix_floats):
            yield row

    def export_array_ids_to_csv(self, data, array_ids_info, export_headers=False, mode='a', include_time_zone=False):
        """Export array ids data as separate comma-separated values files.

        Args:
            data (dict(list(OrderedDict))): Array id separated data.
            array_ids_info (dict(dict)): Array ids to export. Contains output file paths and file headers.
            export_headers (bool): Write file headers at the top of the output file.
            mode (str): Output file open mode, defaults to append. See Python Docs for other mode options.
            include_time_zone (bool): Include time zone in string converted datetime values.

        Raises:
            ArrayIdsInfoError: If not at least one array id in array_ids_info is found.
            ArrayIdsExportInfoError: If no information for a certain array id is found.
            ArrayIdsFilePathError: If no output file path for a certain array id is found.

        """
        if len(array_ids_info) < 1:
            raise ArrayIdsInfoError("At least one array id must be given!")

        data_filtered = self.filter_data_by_array_ids(data, *array_ids_info.keys())

        for array_id, array_id_data in data_filtered.items():
            export_info = array_ids_info.get(array_id)
            if not export_info:
                raise ArrayIdsExportInfoError("No information was found for array id {0}".format(array_id))
            file_path = export_info.get('file_path')
            if not file_path:
                raise ArrayIdsFilePathError("Not file path was found for array id {0}".format(array_id))

            super().export_to_csv(data=array_id_data, outfile_path=file_path, export_headers=export_headers, mode=mode,
                                  include_time_zone=include_time_zone)

    @staticmethod
    def filter_data_by_array_ids(data, *array_ids):
        """Filter data set by array ids.

        Args:
            *array_ids: Array ids to filter by. If no arguments are given, return unfiltered data set.
            data (dict(list(OrderedDict)), list): Array id separated or mixed data.

        Returns:
            Filtered data set if array ids are given, unfiltered otherwise. If a mixed data set is given, return
            unfiltered data set split by its array ids.

        Raises:
            DataTypeError: if anything else than list or dictionary data is given.

        """
        data_filtered = defaultdict(list)

        if isinstance(data, dict):
            if not array_ids:
                return data     # Return unfiltered data set
            for array_id, array_id_data in data.items():
                if array_id in array_ids:
                    data_filtered[array_id] = array_id_data
        elif isinstance(data, list):
            for row in CampbellSCIBaseParser._data_generator(data):
                try:
                    array_id_name = list(row.values())[0]
                except IndexError:
                    continue
                if not array_ids:
                    data_filtered[array_id_name].append(row)   # Append to unfiltered data set, but split by array ids.
                else:
                    if array_id_name in array_ids:
                        data_filtered[array_id_name].append(row)
        else:
            msg = "Data collection of type {0} not supported. Valid collection types are dict and list.".format(type(data))
            raise DataTypeError(msg)

        return data_filtered

    @staticmethod
    def read_array_ids_data(infile_path, line_num=0, fix_floats=True, array_ids_info=None):
        """Parses data filtered by array id (each rows' first element) from a given file.

        Args:
            infile_path (str): Input file's absolute path.
            line_num (int): Line number to start at. NOTE: Zero-based numbering.
            fix_floats (bool): Correct leading zeros floating points since the CR10 does not output leading zeros.
            array_ids_info (dict): Lookup table for array id name translation.

        Returns:
            All data found from the given line number onwards, filtered by array id.

        """
        if not array_ids_info:
            array_ids_info = {}
        array_ids = [key for key in array_ids_info.keys()]
        data_mixed = CR10Parser.read_mixed_data(infile_path=infile_path, line_num=line_num, fix_floats=fix_floats)
        data_by_array_ids = CR10Parser.filter_data_by_array_ids(data_mixed, *array_ids)

        for array_id, array_name in array_ids_info.items():
            if array_id in data_by_array_ids:
                if array_name:
                    data_by_array_ids[array_name] = data_by_array_ids.pop(array_id)

        return data_by_array_ids

    @staticmethod
    def read_mixed_data(infile_path, line_num=0, fix_floats=True):
        """Parses data from a given file without filtering.

        Args:
            infile_path (str): Input file's absolute path.
            line_num (int): Line number to start at. NOTE: Zero-based numbering.
            fix_floats (bool): Correct leading zeros floating points since the CR10 does not output leading zeros.

        Returns:
            All data found from the given line number onwards.

        """
        return [row for row in CR10Parser._read_mixed_data(infile_path=infile_path, line_num=line_num, fix_floats=fix_floats)]