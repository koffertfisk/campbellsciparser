#!/usr/bin/env
# -*- coding: utf-8 -*-

"""Module for parsing and exporting data collected by Campbell Scientific data loggers. """

import csv
import os.path

from collections import namedtuple, OrderedDict
from datetime import datetime

import pytz


class DataTypeError(TypeError):
    pass


class TimeColumnValueError(ValueError):
    pass


class TimeConversionException(Exception):
    pass


class TimeParsingException(ValueError):
    pass


class TimeZoneAlreadySet(ValueError):
    pass


class UnknownPytzTimeZoneError(pytz.UnknownTimeZoneError):
    pass


class UnsupportedTimeFormatError(ValueError):
    pass


class CampbellSCIBaseParser(object):
    """Base class for parsing and exporting data collected by Campbell Scientific data loggers. """

    def __init__(self, time_zone='UTC', time_format_args_library=None):
        """Initializes the data logger parser with time arguments (needed for time parsing and time conversion).

        Args:
            time_zone (str): Data pytz time zone, used for localization. See pytz docs for reference.
            time_format_args_library (list): List of expected time string representations.

        """
        try:
            self.time_zone = pytz.timezone(time_zone)
        except pytz.UnknownTimeZoneError:
            msg = "{0} is not a valid pytz time zone! See pytz docs for valid time zones".format(time_zone)
            raise UnknownPytzTimeZoneError(msg)

        if not time_format_args_library:
            time_format_args_library = []

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
    def _datetime_to_string_repr(dt, include_time_zone=False):
        """Produces a string representation from a datetime object including or excluding time zone information.

        Args:
            dt (datetime): Datetime to process.

        Returns:
            String representation of the given datetime including or excluding the time zone.

        """
        if include_time_zone:
            return dt.strftime("%Y-%m-%d %H:%M:%S%z")

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

    def _parse_custom_time_format(self, *time_values):
        """Base method for parsing Campbell data logger specific time formats.

        Args:
            *time_values (str): Time strings to be parsed.

        Returns:
            Parsed time string format representation and its parsed value.

        """
        parsing_info = namedtuple('ParsedCustomTimeInfo', ['parsed_time_format', 'parsed_time'])
        found_time_format_args = []
        found_time_values = []

        for format_arg, time_arg in zip(self.time_format_args_library, time_values):
            found_time_format_args.append(format_arg)
            found_time_values.append(time_arg)

        time_format_str = ','.join(found_time_format_args)
        time_values_str = ','.join(found_time_values)

        return parsing_info(time_format_str, time_values_str)

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
            if isinstance(header_row, int) and header_row >= 0:
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
        for row in CampbellSCIBaseParser._process_rows(infile_path, headers=headers, header_row=header_row, line_num=line_num):
            yield row

    def _values_to_strings(self, row, include_time_zone=False):
        """Produces a list for the values in a row, converted to strings.

        Args:
            row (OrderedDict): List of values read from a row.
            include_time_zone (bool): Include time zone in string converted datetime objects.

        Returns:
            String representations for each value.

        """
        for key, value in row.items():
            if isinstance(value, datetime):
                row[key] = self._datetime_to_string_repr(value, include_time_zone=include_time_zone)
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
            for row in CampbellSCIBaseParser._data_generator(data):
                if export_headers:
                    headers = [str(key) for key in row.keys()]
                    f_out.write(",".join(headers) + "\n")
                    export_headers = False
                f_out.write(",".join(self._values_to_strings(row, include_time_zone)) + "\n")

    def parse_time_values(self, *time_values, **parsing_info):
        """Base method for converting Campbell data logger specific time representations to a datetime object.

        Args:
            *time_values (str): Time strings to be parsed.
            **parsing_info: Additional parsing information. If to_utc is present and true, convert parsed time to UTC.

        Returns:
            Timestamp converted time.

        Raises:
            TimeParsingException: If time string format and time values parsing error is not ignored.

        """
        parsed_time_format, parsed_time = self._parse_custom_time_format(*time_values)

        try:
            dt = datetime.strptime(parsed_time, parsed_time_format)

        except ValueError:
            msg = "Could not parse time string {0} using the format {1}".format(parsed_time, parsed_time_format)
            print(msg)
            ignore_parsing_error = parsing_info.get('ignore_parsing_error', False)

            if ignore_parsing_error:
                loc_dt = datetime.fromtimestamp(0, self.time_zone)
                print("Setting time value to epoch time ({0})".format(str(loc_dt)))
            else:
                raise TimeParsingException(msg)

        else:
            try:
                loc_dt = self.time_zone.localize(dt)
            except ValueError:
                #print("Datetime already localized.")
                loc_dt = dt

        parsed_dt = loc_dt

        if parsing_info.get('to_utc', False):
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
    def update_headers(data, headers, match_row_lengths=True, output_mismatched_rows=False):
        data_headers_updated = []
        data_headers_length_mismatched = []

        for row in CampbellSCIBaseParser._data_generator(data):
            if match_row_lengths:
                if len(headers) == len(row):
                    data_headers_updated.append(OrderedDict([(name, value) for name, value in zip(headers, row.values())]))
                else:
                    if output_mismatched_rows:
                        data_headers_length_mismatched.append(row)
            else:
                data_headers_updated.append(OrderedDict([(name, value) for name, value in zip(headers, row.values())]))
                if output_mismatched_rows:
                    data_headers_length_mismatched.append(row)

        if match_row_lengths and output_mismatched_rows:
            return data_headers_updated, data_headers_length_mismatched

        return data_headers_updated