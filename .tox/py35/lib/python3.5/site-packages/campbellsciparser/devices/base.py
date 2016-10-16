# -*- coding: utf-8 -*-

"""
CampbellSCIBaseParser
---------------------
Base utility for parsing and exporting data collected by Campbell Scientific dataloggers.

"""
import csv
import os.path

from collections import namedtuple, OrderedDict
from datetime import datetime

import pytz


class TimeColumnValueError(ValueError):
    pass


class TimeParsingError(ValueError):
    pass


class UnknownPytzTimeZoneError(pytz.UnknownTimeZoneError):
    pass


class CampbellSCIBaseParser(object):
    """Generic parser that provides basic utility for most CR-type models.

    All parsed data is represented as a sequence (rows) of order-preserving
    key/value pairs (columns). The keys represent column names if headers are given,
    otherwise they will hold their column indices. The values hold each respective
    column's data value, represented as a string (by default).

    Args
    ----
    pytz_time_zone (str): String representation of a valid pytz time zone. (See pytz docs
        for more information). The time zone refers to collected data's time zone, which
        defaults to UTC and is used for localization and time conversion.
    time_format_args_library (list): List of the maximum expected string format columns
        sequence to match against when parsing time values. Defaults to empty library.

            Example configurations
            ----------------------

            1. Consider the time values ['2016', '1', '1', '22', '30', '00', '+0100'].

            The library ['%Y'] would parse only the year.
            The library ['%Y', '%m'] would parse year and month.
            The library ['%Y', '%m', '%d'] would parse year, month and day.
            The library ['%Y', '%m', '%d', '%H', '%M', '%S'] would parse year, month, day,
            hour, minute and seconds.
            The library ['%Y', '%m', '%d', '%H', '%M', '%S', '%z'] would parse year,
            month, day, hour, minute, seconds and time zone, overriding the specified
            parser time zone.

            2. Consider the time values ['2016', '1', '1'].

            The library ['%Y', '%m', '%d', '%H', '%M', '%S'] would parse year, month
            and day.

            Consider the time values ['2016-01-01 22:30:00']

            3. The library ['%Y-%d-%m %H:%M:%S'] would parse year, month, day, hour,
            minute and seconds.

        See the section "strftime() and strptime() Behavior" (as of October 2016) in the
        Python docs for valid time string formats.

    Raises
    ------
    UnknownPytzTimeZoneError: If the given time zone is not a valid pytz time zone.

    """
    def __init__(self, pytz_time_zone='UTC', time_format_args_library=None):

        try:
            self.time_zone = pytz.timezone(pytz_time_zone)
        except pytz.UnknownTimeZoneError:
            msg = "{0} is not a valid pytz time zone! "
            msg += "See pytz docs for valid time zones".format(pytz_time_zone)
            raise UnknownPytzTimeZoneError(msg)

        if not time_format_args_library:
            time_format_args_library = []

        self.time_format_args_library = time_format_args_library

    @staticmethod
    def _data_generator(data):
        """
        Iterate over the rows of a data set (list of ordered dictionaries, i.e. rows
        and columns).

        Args
        ----
        data (list(OrderedDict)): Data set to iterate.

        Returns
        -------
            Each row, one at a time.

        """
        for row in data:
            yield row

    @staticmethod
    def _datetime_to_string(dt, include_time_zone=False):
        """
        Returns string formatted representation of a datetime object, including or
        excluding its time zone.

        Args
        ----
        dt (datetime): Datetime to process.

        Returns
        -------
        String formatted representation of a datetime object, including or
        excluding its time zone.

        """
        if include_time_zone:
            return dt.strftime("%Y-%m-%d %H:%M:%S%z")

        return dt.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _find_first_time_column_key(headers, time_columns):
        """Search for the first time column name or index within the headers.

        Args
        ----
        headers (list): List of header names or indices.
        time_columns (list): List of columns, or indices representing time values.

        Returns
        -------
        The name or index of which the first time column in a row.

        Raises
        ------
        TimeColumnValueError: If the first time column is not present within the headers.

        """
        for key in headers:
            if key == time_columns[0]:
                return key
        else:
            msg = "First time column '{0}' not found in headers!".format(time_columns[0])
            raise TimeColumnValueError(msg)

    def _parse_custom_time_format(self, *time_values):
        """
        Parses datalogger model specific time representations based on its time format
        args library.

        Args
        ----
        time_values (str): Time strings to be matched against the datalogger parser time
            format library.

        Returns
        -------
        Two comma separated strings in a namedtuple; one for the time string formats and
        one for the time values.

        """
        parsing_info = namedtuple('ParsedCustomTimeInfo',
                                  ['parsed_time_format', 'parsed_time'])
        found_time_format_args = []
        found_time_values = []

        for format_arg, time_arg in zip(self.time_format_args_library, time_values):
            found_time_format_args.append(format_arg)
            found_time_values.append(time_arg)

        time_format_str = ','.join(found_time_format_args)
        time_values_str = ','.join(found_time_values)

        return parsing_info(time_format_str, time_values_str)

    def _parse_time_values(self, *time_values, **parsing_info):
        """Converts datalogger model specific time representations into a datetime object.

        Args
        ----
        time_values (str): Time strings to be parsed.
        parsing_info: Additional parsing information. If to_utc is given and true,
            the parsed time will be converted to UTC. If ignore_parsing_error is present
            and true, set all failed datetimes to epoch time and continue.

        Returns
        -------
        Time converted to datetime.

        Raises
        ------
        TimeParsingError: If the time string format and time values could not be
        parsed to a datetime object and parsing errors are not ignored.

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
                raise TimeParsingError(msg)

        else:
            try:
                loc_dt = self.time_zone.localize(dt)
            except ValueError:
                print("Datetime already localized.")
                loc_dt = dt

        parsed_dt = loc_dt

        if parsing_info.get('to_utc', False):
            utc_dt = loc_dt.astimezone(pytz.utc)
            parsed_dt = utc_dt

        return parsed_dt

    @staticmethod
    def _process_rows(infile_path, headers=None, header_row=None, line_num=0):
        """Iterator for _read_data.

        Args
        ----
        infile_path (str): Input file's absolute path.
        headers (list): Names to map to each rows' values.
        header_row (int): Input file's header row to map to each rows' values.
        line_num (int): Line number to start at. NOTE: Zero-based numbering.

        Returns
        -------
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
                        yield OrderedDict(
                            [(header, value) for header, value in zip(headers, row)])
            else:
                for row in rows:
                    if line_num <= (rows.line_num - 1):
                        yield OrderedDict([(i, value) for i, value in enumerate(row)])

    @staticmethod
    def _read_data(infile_path, headers=None, header_row=None, line_num=0):
        """Iterate over data read from given file starting at a given line number.

        Args
        ----
        infile_path (str): Input file's absolute path.
        headers (list): Names to map to each rows' values.
        header_row (int): Input file's header row to map to each rows' values.
        line_num (int): Line number to start at. NOTE: Zero-based numbering.

        Returns
        -------
        Each processed row, one at a time.

        """
        for row in CampbellSCIBaseParser._process_rows(
                infile_path, headers=headers, header_row=header_row, line_num=line_num):
            yield row

    def _values_to_strings(self, row, include_time_zone=False):
        """Returns a list of the values in a row, converted to strings.

        Args
        ----
        row (OrderedDict): Row's columns (name, value).
        include_time_zone (bool): Include time zone for string converted datetime objects.

        Returns
        -------
        A list of the row's values, converted to strings.

        """
        for key, value in row.items():
            if isinstance(value, datetime):
                row[key] = self._datetime_to_string(value, include_time_zone=include_time_zone)
            else:
                row[key] = str(value)

        return row.values()

    def convert_time(self, data, time_parsed_column=None, time_columns=None, to_utc=False):
        """
        Converts specific time columns from a data set into a single column with the
        time converted to a datetime object.

        Args
        ----
        data (list(OrderedDict)): Data set to convert.
        time_parsed_column (str): Converted time column name. If not given, use the
            name of the first time column.
        time_columns (list): Column(s) (names or indices) to use for time conversion.
        to_utc (bool): Convert time to UTC.

        Returns
        -------
        Time converted data set.

        Raises
        ------
        TimeColumnValueError: If not at least one time column is given.

        """
        if not time_columns:
            raise TimeColumnValueError("At least one time column is required!")

        data_converted = []

        for row in self._data_generator(data):
            first_time_column_key = (
                self._find_first_time_column_key(
                    list(row.keys()), time_columns)
            )

            row_time_column_values = [value for key, value in row.items()
                                      if key in time_columns]
            row_time_converted = (
                self._parse_time_values(*row_time_column_values, to_utc=to_utc)
            )

            old_key = first_time_column_key
            new_key = old_key
            if time_parsed_column:
                new_key = time_parsed_column

            row_converted = OrderedDict(
                (new_key if key == old_key else key, value) for key, value in row.items())
            row_converted[new_key] = row_time_converted

            for time_column in time_columns:
                if time_column in row_converted and time_column != new_key:
                    del row_converted[time_column]

            data_converted.append(row_converted)

        return data_converted

    def export_to_csv(self, data, outfile_path, export_headers=False, mode='a',
                      include_time_zone=False):
        """Write data to a CSV file.

        Args
        ----
        data (list(OrderedDict)): Data set to export.
        outfile_path (str): Output file's absolute path.
        export_headers (bool): Write file headers at the top of the output file.
        mode (str): Output file open mode, defaults to append. See Python Docs for other
            mode options.
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
                f_out.write(
                    ",".join(self._values_to_strings(row, include_time_zone)) + "\n"
                )

    def read_data(self, infile_path, headers=None, header_row=None, line_num=0,
                  convert_time=False, time_parsed_column=None, time_columns=None,
                  to_utc=False):
        """
        Reads data from a file and stores it in the parser's data structure format
        (see class documentation for details).

        Args
        ----
        infile_path (str): Input file's absolute path.
        headers (list): Names to map to each rows' values.
        header_row (int): Input file's header row fieldnames to map to each rows' values.
        line_num (int): Line number to start at. NOTE: Zero-based numbering.
        convert_time (bool): Convert datalogger specific time string representations
            to datetime objects.
        time_parsed_column (str): Converted time column name. If not given, use the
            name of the first time column.
        time_columns (list): Column(s) (names or indices) to use for time conversion.
        to_utc (bool): Convert time to UTC.

        Returns
        -------
        All data found from the given line number onwards.

        """
        data = [row for row
                in self._read_data(
                    infile_path=infile_path,
                    headers=headers,
                    header_row=header_row,
                    line_num=line_num)]

        if convert_time:
            data = self.convert_time(data=data, time_parsed_column=time_parsed_column,
                                     time_columns=time_columns, to_utc=to_utc)
        return data

    @staticmethod
    def update_column_names(data, headers, match_row_lengths=True,
                            output_mismatched_rows=False):
        """Updates a data set's column names.

        Args
        ----
        data (list(OrderedDict)): Data set to process.
        headers (list): Names to map to each rows' values.
        match_row_lengths (bool): Filter out rows whose number of columns does not match
            the number of names within the headers.
        output_mismatched_rows: Return a list of rows that did not pass the
            "match row lengths" test.

        Returns
        -------
        Data set with updated column names.

        """
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
