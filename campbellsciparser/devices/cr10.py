# -*- coding: utf-8 -*-
"""
CR10Parser
---------------------
Utility for parsing and exporting data collected by Campbell Scientific CR10 dataloggers.

"""

from collections import defaultdict

from campbellsciparser.devices.base import *


class ArrayIdsInfoError(ValueError):
    pass


class ArrayIdsExportInfoError(ArrayIdsInfoError):
    pass


class ArrayIdsFilePathError(ArrayIdsInfoError):
    pass


class DataTypeError(TypeError):
    pass


class UnsupportedTimeFormatError(ValueError):
    pass


class CR10Parser(CampbellSCIBaseParser):
    """Custom parser setup for the CR10.

    CR10 datalogger specific details:

        * The first column value in each row in an output file is typically an integer
        which is used for table identification, often referred to as an "array ID".
        This parser assumes that all input files have an array ID as their first column
        value in each row.

        * The CR10 typically outputs each rows' timestamp using the format
        Year, Julian Day, Hour/Minute (as separate columns). The parser's default time
        format arguments library is set to match this pattern. It will, in other words,
        be able to parse the following example time values:

            ['16']
            ['16', '1']
            ['16', '1', '0']
            ['16', '1', '5']
            ['16', '1', '35']
            ['16', '1', '542']
            ['16', '1', '1315']

        However, the user can override this default configuration to match other patterns.
        For instance, the following configuration would be able to parse time columns that
        does not lead with a year: ['%j', '%H%M']. See the section
        "strftime() and strptime() Behavior" (as of October 2016) in the Python docs for
        valid time string formats.

        * Data is typically output in a "mixed" format, meaning that data from multiple
         table definitions (hence, different array ID:s) is stored in the same file.

        All parsed data is represented using the same data structure as the base parser
        (see CampbellSCIBaseParser documentation for reference) with the exception that
        data filtered by array ID is stored in a dictionary where the keys represent array
        ID:s and the values the data to each corresponding array ID.

    Args
    ----
    pytz_time_zone (str): String representation of a valid pytz time zone. (See pytz docs
        for more information). The time zone refers to collected data's time zone, which
        defaults to UTC and is used for localization and time conversion.
    time_format_args_library (list): List of the maximum expected string format columns
        sequence to match against when parsing time values. Defaults to the most common
        CR10 time representation setup.

    """
    def __init__(self, pytz_time_zone='UTC', time_format_args_library=None):

        if not time_format_args_library:
            time_format_args_library = ['%y', '%j', '%H%M']

        super().__init__(pytz_time_zone, time_format_args_library)
        
    def _parse_custom_time_format(self, *time_values):
        """
        Parses the CR10 specific time representations based on its time format
        args library.

        Args
        ----
        time_values (str): Time strings to be matched against the CR10 parser time
            format library.

        Returns
        -------
        Two comma separated strings in a namedtuple; one for the time string formats and
        one for the time values.

        """
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
        output hours and minutes with leading zeros (except for midnight, which is output
        as 0).

        Examples of the CR10's 'Hour/Minute' pattern and how they would be parsed:

        CR10 time string: '5'
        HHMM time string: '0005'

        CR10 time string: '35'
        HHMM time string: '0035'

        CR10 time string: '159'
        HHMM time string: '0159'

        CR10 time string: '2345'
        HHMM time string: '2345'

        Args
        ----
        hour_minute_str (str): Hour/Minute string to be parsed.

        Returns
        -------
        The time parsed in the format HHMM.

        Raises
        ------
        TimeColumnValueError: If the given time column value could not be parsed.

        """
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
            msg = "Hour/Minute {0} could not be parsed!".format(hour_minute_str)
            raise TimeColumnValueError(msg)

        return parsed_time

    @staticmethod
    def _process_mixed_rows(infile_path, line_num=0, fix_floats=True):
        """Iterator for _read_mixed_data.

        Args
        ----
        infile_path (str): Input file's absolute path.
        line_num (int): Line number to start at. NOTE: Zero-based numbering.
        fix_floats (bool): Correct leading zeros floating points since the CR10 does not
            output leading zeros.

        Returns
        -------
        Each processed row represented as an OrderedDict, returned one at a time.

        """
        with open(infile_path, 'r') as f:
            rows = csv.reader(f)
            replacements = {'.': '0.', '-.': '-0.'}    # Patterns to look for
            for row in rows:
                # Correct reader for zero-based numbering
                if line_num <= (rows.line_num - 1):
                    if fix_floats:
                        for i, value in enumerate(row):
                            for source, replacement in replacements.items():
                                if value.startswith(source):
                                    row[i] = value.replace(source, replacement)

                    yield OrderedDict([(i, value) for i, value in enumerate(row)])

    @staticmethod
    def _read_mixed_data(infile_path, line_num=0, fix_floats=True):
        """Iterate over mixed data read from given file starting at a given line number.

        Args
        ----
        infile_path (str): Input file's absolute path.
        line_num (int): Line number to start at. NOTE: Zero-based numbering.
        fix_floats (bool): Correct leading zeros floating points since the CR10 does not
            output leading zeros.

        Returns
        -------
        Each processed row, one at a time.

        """
        for row in CR10Parser._process_mixed_rows(infile_path=infile_path, line_num=line_num, fix_floats=fix_floats):
            yield row

    def export_array_ids_to_csv(self, data, array_ids_info, export_headers=False,
                                mode='a', include_time_zone=False):
        """Write array id separated data to a CSV file.

        Args
        ----
        data (dict(list(OrderedDict))): Array id separated data.
        array_ids_info (dict(dict)): Array ids to export. Contains output file paths and
            file headers.
        export_headers (bool): Write file headers at the top of the output file.
        mode (str): Output file open mode, defaults to append. See Python Docs for other
            mode options.
        include_time_zone (bool): Include time zone in string converted datetime values.

        Raises
        ------
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
                msg = "No information was found for array id {0}".format(array_id)
                raise ArrayIdsExportInfoError(msg)
            file_path = export_info.get('file_path')
            if not file_path:
                msg = "Not file path was found for array id {0}".format(array_id)
                raise ArrayIdsFilePathError(msg)

            super().export_to_csv(
                data=array_id_data,
                outfile_path=file_path,
                export_headers=export_headers,
                mode=mode,
                include_time_zone=include_time_zone)

    @staticmethod
    def filter_data_by_array_ids(data, *array_ids):
        """Filter data set by array ids.

        Args
        ----
        array_ids: Array ids to filter by. If no arguments are given, return unfiltered
            data set.
        data (dict(list(OrderedDict)), list): Array id separated or mixed data.

        Returns
        -------
        Filtered data set if array ids are given, unfiltered otherwise. If a mixed
        data set is given, return unfiltered data set split by its array ids.

        Raises:
            DataTypeError: if anything else than list or dictionary data is given.

        """
        data_filtered = defaultdict(list)

        if isinstance(data, dict):
            if not array_ids:
                return data
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
                    # Append to unfiltered data set, but split by array ids.
                    data_filtered[array_id_name].append(row)
                else:
                    if array_id_name in array_ids:
                        data_filtered[array_id_name].append(row)
        else:
            msg = "Data collection of type {0} not supported. "
            msg += "Valid collection types are dict and list.".format(type(data))
            raise DataTypeError(msg)

        return data_filtered

    @staticmethod
    def read_array_ids_data(infile_path, line_num=0, fix_floats=True, array_id_names=None):
        """Parses data filtered by array id (each rows' first element) read from a file.

        Args
        ----
        infile_path (str): Input file's absolute path.
        line_num (int): Line number to start at. NOTE: Zero-based numbering.
        fix_floats (bool): Correct leading zeros floating points since the CR10 does not
            output leading zeros.
        array_id_names (dict): Lookup table for array id name translation.

        Returns
        -------
        All data found from the given line number onwards, filtered by array id.

        """
        if not array_id_names:
            array_id_names = {}
        array_ids = [array_id for array_id in array_id_names.keys()]

        data_mixed = CR10Parser.read_mixed_data(
            infile_path=infile_path,
            line_num=line_num,
            fix_floats=fix_floats)

        data_by_array_ids = CR10Parser.filter_data_by_array_ids(data_mixed, *array_ids)

        for array_id, array_name in array_id_names.items():
            if array_id in data_by_array_ids:
                if array_name:
                    data_by_array_ids[array_name] = data_by_array_ids.pop(array_id)

        return data_by_array_ids

    @staticmethod
    def read_mixed_data(infile_path, line_num=0, fix_floats=True):
        """
        Reads mixed data from a file (without array ids filtering) and stores it
        in the base parser's data structure format (see CampbellSCIBaseParser class
        documentation for details).

        Args
        ----
        infile_path (str): Input file's absolute path.
        line_num (int): Line number to start at. NOTE: Zero-based numbering.
        fix_floats (bool): Correct leading zeros floating points since the CR10 does not
            output leading zeros.

        Returns
        -------
        All data found from the given line number onwards.

        """
        return [row for row in CR10Parser._read_mixed_data(
            infile_path=infile_path, line_num=line_num, fix_floats=fix_floats)]
