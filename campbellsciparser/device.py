#!/usr/bin/env
# -*- coding: utf-8 -*-

"""Module for parsing and exporting data collected by Campbell Scientific data loggers. """

import csv
import os
import time

from collections import defaultdict, namedtuple, OrderedDict
from datetime import datetime

import pytz


class TimeConversionException(Exception):
    pass


class TimeColumnValueError(ValueError):
    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)


class UnsupportedTimeFormatError(ValueError):
    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)


class DataTypeError(TypeError):
    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)


class ArrayIdsInfoError(ValueError):
    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)


class ArrayIdsExportInfoError(ArrayIdsInfoError):
    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)


class ArrayIdsFilePathError(ArrayIdsInfoError):
    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)


class CampbellSCILoggerParser(object):
    """Base class for parsing and exporting data collected by Campbell Scientific data loggers. """

    def __init__(self, time_zone, time_format_args_library=None):
        """Initializes the data logger parser with time arguments (needed for time parsing and time conversion).

        Args:
            time_zone (str): Data pytz time zone, used for localization. See pytz docs for reference.
            time_format_args_library (list): List of expected time string representations.

        """
        self.time_zone = pytz.timezone(time_zone)
        self.time_format_args_library = time_format_args_library

    @staticmethod
    def _data_generator(data):
        """Turns data set (list of ordered dictionaries) into a generator.

        Args:
            data (list(OrderedDict)): Data set (list of ordered dictionaries) to process.

        Returns:
            Each row, one at a time.

        """
        for row in data:
            yield row

    @staticmethod
    def _datetime_to_str_no_time_zone(dt):
        """Produces a string representation from a datetime object without time zone information.

        Args:
            dt (datetime): Datetime to process.

        Returns:
            String representation of the given datetime excluding the time zone.

        """
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _find_first_time_column_key(headers, time_columns):
        """
            Search for the first time representation column within the headers. Used for inserting the parsed time
            column at the found position.

        Args:
            headers (list): List of data file headers or indices.
            time_columns (list): List of columns, or indices representing time values.

        Returns:
            The index of which the first time column is expected.

        Raises:
            TimeColumnValueError: If the first time column is not present in the headers.

        """
        for key in headers:
            if key == time_columns[0]:
                return key
        else:
            raise TimeColumnValueError("First time column '{0}' not found in headers!".format(time_columns[0]))

    def _parse_custom_time_format(self, *args):
        """Base method for parsing Campbell data logger specific time formats.

        Args:
            *args (str): Time strings to be parsed.

        Returns:
            Parsed time string format representation and its parsed value.

        """
        parsing_info = namedtuple('ParsedCustomTimeInfo', ['parsed_time_format', 'parsed_time'])

        return parsing_info("", "")

    @staticmethod
    def _process_rows(infile_path, headers=None, header_row=None, line_num=0):
        """Helper method for _read_data.

        Args:
            infile_path (str): Input file's absolute path.
            headers (list): Headers to map to each rows' values.
            header_row (int): Input file's header row to map to each rows' values.
            line_num (int): Line number to start at. NOTE: Zero-based numbering.

        Returns:
            Each processed row represented as an OrderedDict, returned one at a time.

        """
        with open(infile_path, 'r') as f:
            rows = csv.reader(f)
            if headers:
                pass
            elif header_row and header_row >= 0:
                headers = rows.__next__()
                for i in range(header_row):
                    headers = rows.__next__()
            if headers:
                for row in rows:
                    if line_num <= (rows.line_num - 1):
                        yield OrderedDict([(header, value) for header, value in zip(headers, row)])
            else:
                for row in rows:
                    if line_num <= (rows.line_num - 1):
                        yield OrderedDict([(i, value) for i, value in enumerate(row)])

    @staticmethod
    def _read_data(infile_path, headers=None, header_row=None, line_num=0):
        """Produces a generator object of data read from given file input starting from a given line number.

        Args:
            infile_path (str): Input file's absolute path.
            headers (list): Headers to map to each rows' values.
            header_row (int): Input file's header row to map to each rows' values.
            line_num (int): Line number to start at. NOTE: Zero-based numbering.

        Returns:
            Each processed row, one at a time.

        """
        for row in CampbellSCILoggerParser._process_rows(infile_path, headers=headers, header_row=header_row, line_num=line_num):
            yield row

    def _row_str_conversion(self, row, include_time_zone=False):
        """Produces a list for the values in a row, converted to strings.

        Args:
            row (OrderedDict): List of values read from a row.
            include_time_zone (bool): Include time zone in string converted datetime objects.

        Returns:
            String representations for each value.

        """
        for key, value in row.items():
            if not include_time_zone and isinstance(value, datetime):
                row[key] = self._datetime_to_str_no_time_zone(value)
            else:
                row[key] = str(value)

        return row.values()

    def convert_time(self, data, time_parsed_column=None, time_columns=None, to_utc=False):
        """Converts specific time columns from a data set into a single timestamp column.

        Args:
             data (list(OrderedDict)): Data set to convert.
             time_parsed_column (str): Converted time column name. If not given, use index names.
             time_columns (list): Column(s) (names or indices) to use for time converting.
             to_utc (bool): Convert time to UTC.

        Returns:
            Time converted data set.

        Raises:
            TimeColumnValueError: If not at least one time column is given.

        """
        if not time_columns:
            raise TimeColumnValueError("At least one time column is required!")

        data_converted = []

        for row in self._data_generator(data):
            first_time_column_key = self._find_first_time_column_key(list(row.keys()), time_columns)
            row_time_column_values = [value for key, value in row.items() if key in time_columns]
            row_time_converted = self.parse_time_values(*row_time_column_values, to_utc=to_utc)

            old_key = first_time_column_key
            new_key = old_key
            if time_parsed_column:
                new_key = time_parsed_column

            row_converted = OrderedDict((new_key if key == old_key else key, value) for key, value in row.items())
            row_converted[new_key] = row_time_converted

            for time_column in time_columns:
                if time_column in row_converted and time_column != new_key:
                    del row_converted[time_column]

            data_converted.append(row_converted)

        return data_converted

    def export_to_csv(self, data, outfile_path, export_headers=False, mode='a', include_time_zone=False):
        """Export data as a comma-separated values file.

        Args:
            data (list(OrderedDict)): Data set to export.
            outfile_path (str): Output file's absolute path.
            export_headers (bool): Write file headers at the top of the output file.
            mode (str): Output file open mode, defaults to append. See Python Docs for other mode options.
            include_time_zone (bool): Include time zone in string converted datetime values.

        """
        os.makedirs(os.path.dirname(outfile_path), exist_ok=True)

        if os.path.exists(outfile_path) and export_headers:
            with open(outfile_path, 'r') as temp_f:
                f_list = [line.strip() for line in temp_f]
            if len(f_list) > 0:
                export_headers = False

        with open(outfile_path, mode) as f_out:
            for row in CampbellSCILoggerParser._data_generator(data):
                if export_headers:
                    headers = [str(key) for key in row.keys()]
                    f_out.write(",".join(headers) + "\n")
                    export_headers = False
                f_out.write(",".join(self._row_str_conversion(row, include_time_zone)) + "\n")

    def parse_time_values(self, to_utc=False, *args):
        """Base method for converting Campbell data logger specific time representations to a datetime object.

        Args:
            to_utc (bool): Convert to UTC.
            *args (str): Time strings to be parsed.

        Returns:
            Timestamp converted time.

        """
        parsed_time_format, parsed_time = self._parse_custom_time_format(*args)

        try:
            t = time.strptime(parsed_time, parsed_time_format)
            dt = datetime.fromtimestamp(time.mktime(t))
            loc_dt = self.time_zone.localize(dt)
        except ValueError:
            print("Could not parse time string {0} using the format {1}".format(parsed_time, parsed_time_format))
            loc_dt = datetime.fromtimestamp(0, self.time_zone)

        parsed_dt = loc_dt

        if to_utc:
            utc_dt = loc_dt.astimezone(pytz.utc)
            parsed_dt = utc_dt

        return parsed_dt

    def read_data(self, infile_path, headers=None, header_row=None, line_num=0, convert_time=False,
                  time_parsed_column=None, time_columns=None, to_utc=False):
        """Parses data from a given file without filtering.

        Args:
            infile_path (str): Input file's absolute path.
            headers (list): Headers to map to each rows' values.
            header_row (int): Input file's header row to map to each rows' values.
            line_num (int): Line number to start at. NOTE: Zero-based numbering.
            convert_time (bool): Convert Campbell data logger specific time representations to timestamp.
            time_parsed_column (str): Converted time column name. If not given, use index names.
            time_columns (list): Column(s) (names or indices) to use for time converting.
            to_utc (bool): Convert time to UTC.

        Returns:
            All data found from the given line number onwards.

        """
        data = [row for row in self._read_data(infile_path=infile_path, headers=headers, header_row=header_row, line_num=line_num)]

        if convert_time:
            data = self.convert_time(data=data, time_parsed_column=time_parsed_column,
                                     time_columns=time_columns, to_utc=to_utc)
        return data

    @staticmethod
    def update_headers(data, headers, output_mismatched_rows=False):

        data_headers_updated = []
        data_headers_length_mismatched = []

        for row in CampbellSCILoggerParser._data_generator(data):
            if len(headers) == len(row.values()):
                data_headers_updated.append(OrderedDict([(name, value) for name, value in zip(headers, row.values())]))
            else:
                if output_mismatched_rows:
                    data_headers_length_mismatched.append(row)

        if output_mismatched_rows:
            return data_headers_updated, data_headers_length_mismatched

        return data_headers_updated


class CR10XParser(CampbellSCILoggerParser):
    """Parses and exports data files collected by Campbell Scientific CR10X data loggers. """

    def __init__(self, time_zone='UTC', time_format_args_library=None):
        """Initializes the data logger parser with time arguments for the CR10X model.

        Args:
            time_zone (str): Data pytz time zone, used for localization. See pytz docs for reference.
            time_format_args_library (list): List of expected time string representations.

        """
        if not time_format_args_library:
            time_format_args_library = ['%Y', '%j', 'Hour/Minute']

        super().__init__(time_zone, time_format_args_library)

    @staticmethod
    def _parse_hourminute(hour_minute_str):
        """Parses the CR10X custom time format column 'Hour/Minute'.

        Args:
            hour_minute_str (str): Hour/Minute string to be parsed.

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

    def _parse_custom_time_format(self, *timevalues):
        """Parses the CR10X custom time format.

        Args:
            *timevalues (str): Time strings to be parsed.

        Returns:
            Parsed time format string representation and value.

        Raises:
            UnsupportedTimeFormatError: If more than three arguments is being passed.

        """
        time_values = list(timevalues)
        if len(time_values) > 3:
            raise UnsupportedTimeFormatError(
                "The CR10X time parser only supports Year, Day, Hour/Minute, got {0} time values".format(len(time_values)))
        found_time_format_args = []
        parsing_info = namedtuple('ParsedTimeInfo', ['parsed_time_format', 'parsed_time'])

        for i, value in enumerate(time_values):
            found_time_format_args.append(self.time_format_args_library[i])
            if i == 2:     # Time string "Hour/Minute" reached
                parsed_time_format, parsed_time = self._parse_hourminute(value)
                found_time_format_args[2] = parsed_time_format
                time_values[2] = parsed_time

        time_format_str = ','.join(found_time_format_args)
        time_values_str = ','.join(time_values)

        return parsing_info(time_format_str, time_values_str)

    @staticmethod
    def _process_mixed_rows(infile_path, line_num=0, fix_floats=True):
        """Helper method for _read_data.

        Args:
            infile_path (str): Input file's absolute path.
            line_num (int): Line number to start at. NOTE: Zero-based numbering.
            fix_floats (bool): Correct leading zeros floating points since the CR10X does not output leading zeros.

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
            fix_floats (bool): Correct leading zeros floating points since the CR10X does not output leading zeros.

        Returns:
            Each processed row, one at a time.

        """
        for row in CR10XParser._process_mixed_rows(infile_path=infile_path, line_num=line_num, fix_floats=fix_floats):
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

        data_filtered = self.filter_data_by_array_ids(*array_ids_info.keys(), data=data)

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
    def filter_data_by_array_ids(*array_ids, data):
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
            for row in CampbellSCILoggerParser._data_generator(data):
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
            raise DataTypeError("Data collection of type {0} not supported. Valid collection types are dict and list.")

        return data_filtered

    @staticmethod
    def read_array_ids_data(infile_path, line_num=0, fix_floats=True, array_ids_info=None):
        """Parses data filtered by array id (each rows' first element) from a given file.

        Args:
            infile_path (str): Input file's absolute path.
            line_num (int): Line number to start at. NOTE: Zero-based numbering.
            fix_floats (bool): Correct leading zeros floating points since the CR10X does not output leading zeros.
            array_ids_info (dict): Lookup table for array id name translation.

        Returns:
            All data found from the given line number onwards, filtered by array id.

        """
        if not array_ids_info:
            array_ids_info = {}
        array_ids = [key for key in array_ids_info.keys()]
        data_mixed = CR10XParser.read_mixed_data(infile_path=infile_path, line_num=line_num, fix_floats=fix_floats)
        data_by_array_ids = CR10XParser.filter_data_by_array_ids(*array_ids, data=data_mixed)

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
            fix_floats (bool): Correct leading zeros floating points since the CR10X does not output leading zeros.

        Returns:
            All data found from the given line number onwards.

        """
        return [row for row in CR10XParser._read_mixed_data(infile_path=infile_path, line_num=line_num, fix_floats=fix_floats)]


class CR1000Parser(CampbellSCILoggerParser):
    """Parses and exports data files collected by Campbell Scientific CR1000 data loggers. """

    def __init__(self, time_zone='UTC', time_format_args_library=None):
        """Initializes the data logger parser with time arguments for the CR1000 model.

        Args:
            time_zone (str): Data pytz time zone, used for localization. See pytz docs for reference.
            time_format_args_library (list): List of expected time string representations.

        """
        if not time_format_args_library:
            time_format_args_library = ['%Y-%m-%d %H:%M:%S']

        super().__init__(time_zone, time_format_args_library)

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
