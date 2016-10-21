# -*- coding: utf-8 -*-

"""
CRGeneric
---------------------
Generic base parser that provides common utilities for parsing and exporting data
outputted by Campbell Scientific CR-type dataloggers.

"""
import csv
import os.path

from collections import namedtuple, OrderedDict
from datetime import datetime

import pytz


class TimeColumnValueError(ValueError):
    """Raised whenever there are problems parsing specific time columns. """
    pass


class TimeParsingError(ValueError):
    """
    Raised whenever there's an error in creating a datetime object and parsing errors
    are not ignored.

    """
    pass


class UnknownPytzTimeZoneError(pytz.UnknownTimeZoneError):
    """
    Raised whenever the time zone used when instantiating a parser object in not a
    valid pytz time zone.

    """
    pass


class CRGeneric(object):
    """Generic base parser that provides basic utilities for most CR-type models.

    Data is parsed from CSV (comma separated values) files outputted by CR-type dataloggers.
    All parsed data is represented as a list (rows) of order-preserving key/value pairs
    (column names and values), where the column names are either: derived from a file
    header row, passed explicitly by the user, or assigned by each columns' index.
    The column values hold each respective column's data value, represented as a string
    (by default).

    Attributes
    ----------
    time_zone : str
        String representation of a valid pytz time zone. (See pytz docs
        for a list of valid time zones). The time zone refers to collected data's
        time zone, which defaults to UTC and is used for localization and time conversion.
    time_format_args_library : list of str
        List of the maximum expected string
        format columns sequence to match against when parsing time values. Defaults to
        empty library.

            Examples of time format args library configurations
            ---------------------------------------------------

            1. Consider the time values ['2016', '1', '1', '22', '30', '00', '+0100'].

            The library ['%Y'] would be able to parse the year.
            The library ['%Y', '%m'] would be able to parse year and month.
            The library ['%Y', '%m', '%d'] would be able to parse year, month and day.
            The library ['%Y', '%m', '%d', '%H', '%M', '%S'] would be able to parse
            year, month, day, hour, minute and seconds.
            The library ['%Y', '%m', '%d', '%H', '%M', '%S', '%z'] would be able to parse
            year, month, day, hour, minute, seconds and time zone, overriding the
            specified parser time zone (pytz_time_zone).

            2. Consider the time values ['2016', '1', '1'].

            The library ['%Y', '%m', '%d', '%H', '%M', '%S'] would be able to parse
            year, month and day.

            3. Consider the time values ['2016-01-01 22:30:00']

            The library ['%Y-%d-%m %H:%M:%S'] would be able to parse year, month, day,
            hour, minute and seconds.

    Notes
    ----
    See the section "strftime() and strptime() Behavior" (as of October 2016)
    in the Python docs for valid time string formats.

    """
    def __init__(self, pytz_time_zone='UTC', time_format_args_library=None):
        """CRGeneric parser initialization.

        Parameters
        ----------
        pytz_time_zone : str
            String representation of a valid pytz time zone. (See pytz docs
            for a list of valid time zones). The time zone refers to collected data's
            time zone, which defaults to UTC and is used for localization and time
            conversion.
        time_format_args_library : list of str, optional
            List of the maximum expected string format columns sequence to match against
            when parsing time values. Defaults to empty library.

        Examples
        --------
        >>> cr = CRGeneric(time_format_args_library=['%Y', '%m', '%d'])
        >>> for key, val in cr.__dict__.items():
        ...     print(key, ":", val)
        ...
        time_format_args_library : ['%Y', '%m', '%d']
        time_zone : UTC

        >>> cr = CRGeneric(pytz_time_zone='UTC')
        >>> for key, val in cr.__dict__.items():
        ...     print(key, ":", val)
        ...
        time_format_args_library : []
        time_zone : UTC

        >>> cr = CRGeneric('Etc/GMT-1', ['%Y-%d-%m %H:%M:%S'])
        >>> for key, val in cr.__dict__.items():
        ...     print(key, ":", val)
        ...
        time_format_args_library : ['%Y-%d-%m %H:%M:%S']
        time_zone : Etc/GMT-1

        Raises
        ------
        UnknownPytzTimeZoneError: If the given time zone is not a valid pytz time zone.

        """
        try:
            self.time_zone = pytz.timezone(pytz_time_zone)
        except pytz.UnknownTimeZoneError:
            msg = "{time_zone} is not a valid pytz time zone! "
            msg += "See pytz docs for valid time zones".format(time_zone=pytz_time_zone)
            raise UnknownPytzTimeZoneError(msg)

        if not time_format_args_library:
            time_format_args_library = []

        self.time_format_args_library = time_format_args_library

    @staticmethod
    def _data_generator(data):
        """
        Iterate over the rows of a data set (list of ordered dictionaries, i.e. rows
        and columns).

        Parameters
        ----------
        data : list of OrderedDict
            Data set to iterate.

        Yields
        ------
        OrderedDict
            The data set's next row.

        """
        for row in data:
            yield row

    @staticmethod
    def _datetime_to_string(dt, include_time_zone=False):
        """
        Returns a string formatted representation of a datetime object, including or
        excluding its time zone.

        Parameters
        ----------
        dt : datetime
            Datetime to process.
        include_time_zone : bool, optional
            Include time zone in string representation.

        Returns
        -------
        str
            String formatted representation of a datetime object, including or
            excluding its time zone.

        """
        if include_time_zone:
            return dt.strftime("%Y-%m-%d %H:%M:%S%z")

        return dt.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _find_first_time_column_key(column_names, time_columns):
        """Search for the column name which holds the first time column value.

        Parameters
        ----------
        column_names : list of str or int
            List of column names (or indices).
        time_columns : list of str or int
            List of column names (or indices) that represent time values.

        Returns
        -------
        str or int
            The column name or index of the first time column.

        Raises
        ------
        TimeColumnValueError: If the first time column is not present in the list of
            column names.

        """
        for name in column_names:
            if name == time_columns[0]:
                return name
        else:
            msg = "First time column '{first_time_column}' not found in header!"
            msg = msg.format(first_time_column=time_columns[0])
            raise TimeColumnValueError(msg)

    def _parse_custom_time_format(self, *time_values):
        """
        Parses datalogger model specific time representations based on its time format
        args library.

        Parameters
        ----------
        *time_values
            Time strings to be matched against the datalogger parser time format library.

        Returns
        -------
        namedtuple
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

        time_format_string = ','.join(found_time_format_args)
        time_values_string = ','.join(found_time_values)

        return parsing_info(time_format_string, time_values_string)

    def _parse_time_values(self, *time_values, **parsing_info):
        """Converts datalogger model specific time representations into a datetime object.

        Parameters
        ----------
        *time_values
            Time strings to be parsed.
        **parsing_info
            Additional parsing information. If to_utc is given and true, the parsed time
            will be converted to UTC. If ignore_parsing_error is present and true, set all
            failed datetimes to epoch time and continue.

        Returns
        -------
        datetime
            Converted time.

        Raises
        ------
        TimeParsingError: If the time string format and time values could not be
            parsed to a datetime object and parsing errors are not ignored.

        """
        parsed_time_format, parsed_time = self._parse_custom_time_format(*time_values)

        try:
            dt = datetime.strptime(parsed_time, parsed_time_format)
        except ValueError:
            msg = "Could not parse time string {0} using the format {1}".format(
                parsed_time, parsed_time_format)
            print(msg)
            ignore_parsing_error = parsing_info.get('ignore_parsing_error', False)

            if ignore_parsing_error:
                local_dt = datetime.fromtimestamp(0, self.time_zone)
                print("Setting time value to epoch time ({0})".format(str(loc_dt)))
            else:
                raise TimeParsingError(msg)
        else:
            try:
                local_dt = self.time_zone.localize(dt)
            except ValueError:
                print("Datetime already localized.")
                local_dt = dt

        parsed_dt = local_dt

        if parsing_info.get('to_utc', False):
            utc_dt = local_dt.astimezone(pytz.utc)
            parsed_dt = utc_dt

        return parsed_dt

    @staticmethod
    def _process_rows(infile_path, header=None, header_row=None, line_num=0):
        """Iterator for _read_data.

        Parameters
        ----------
        infile_path : str
            Input file's absolute path.
        header : list of str, optional
            Column names to map to each rows' values.
        header_row : int, optional
            Input file's header row to map to each rows' values.
        line_num : int, optional
            Line number to start at. NOTE: Zero-based numbering.

        Yields
        ------
        OrderedDict
            The next row read and processed from an input CSV file.

        """
        with open(infile_path, 'r') as f:
            rows = csv.reader(f)
            if header:
                pass
            if isinstance(header_row, int) and header_row >= 0:
                header = rows.__next__()
                for i in range(header_row):
                    header = rows.__next__()
            if header:
                for row in rows:
                    if line_num <= (rows.line_num - 1):
                        yield OrderedDict(
                            [(header, value) for header, value in zip(header, row)])
            else:
                for row in rows:
                    if line_num <= (rows.line_num - 1):
                        yield OrderedDict([(i, value) for i, value in enumerate(row)])

    @staticmethod
    def _read_data(infile_path, header=None, header_row=None, line_num=0):
        """Iterate over data read from given file starting at a given line number.

        Parameters
        ----------
        infile_path : str
            Input file's absolute path.
        header : list of str, optional
            Column names to map to each rows' values.
        header_row : int, optional
            Input file's header row to map to each rows' values.
        line_num : int
            Line number to start at. NOTE: Zero-based numbering.

        Yields
        ------
        OrderedDict
            The next row read and processed from an input CSV file.

        """
        for row in CRGeneric._process_rows(
                infile_path, header=header, header_row=header_row, line_num=line_num):
            yield row

    def _values_to_strings(self, row, include_time_zone=False):
        """Returns a list of the values in a row, converted to strings.

        Parameters
        ----------
        row : OrderedDict
            Row's columns (names and values as key/value pairs).
        include_time_zone : bool, optional
            Include time zone for string converted datetime objects.

        Returns
        -------
        list of str
            A list of the row's values, converted to strings.

        """
        for name, value in row.items():
            if isinstance(value, datetime):
                row[name] = self._datetime_to_string(
                    value, include_time_zone=include_time_zone)
            else:
                row[name] = str(value)

        return row.values()

    def convert_time(self, data, time_parsed_column=None, time_columns=None,
                     replace_time_column=None, to_utc=False):
        """
        Converts specific time columns from a data set into a single column with the
        time converted to a datetime object.

        Parameters
        ----------
        data : list of OrderedDict
            Data set to convert.
        time_parsed_column : str, optional
            Converted time column name. If not given, use the name of the first
            time column.
        time_columns : list of str or int, optional
            Column(s) (names or indices) to use for time conversion.
        replace_time_column : str or int, optional
            Column (name or index) to place the parsed datetime object at. If not given,
            insert at the first time column index.
        to_utc : bool, optional
            Convert time to UTC.

        Returns
        -------
        list of OrderedDict
            Time converted data set.

        Examples
        --------
        >>> cr = CRGeneric('Etc/GMT-1', ['%Y', '%j', '%H%M'])
        >>> data = [
        ...     OrderedDict([
        ...         (0, 'some_value'),
        ...         (1, '2016'),
        ...         (2, '123'),
        ...         (3, '1234'),
        ...         (4, 'some_other_value')
        ...     ])
        ... ]
        >>> cr.convert_time(data, time_columns=[1, 2, 3])
        [OrderedDict([(0, 'some_value'), (1, datetime.datetime(2016, 5, 2, 12, 34,
        tzinfo=<StaticTzInfo 'Etc/GMT-1'>)), (4, 'some_other_value')])]

        >>> cr = CRGeneric('Etc/GMT-1', ['%Y-%d-%m %H:%M:%S'])
        >>> data = [
        ...     OrderedDict([
        ...         ('Label_1', 'some_value'),
        ...         ('Label_2', '2016-05-02 12:34:15'),
        ...         ('Label_3', 'some_other_value')
        ...     ])
        ... ]
        >>> cr.convert_time(
        ...     data, time_parsed_column='TIMESTAMP', time_columns=['Label_2'], to_utc=True)
        [OrderedDict([('Label_1', 'some_value'), ('TIMESTAMP', datetime.datetime(2016, 2,
        5, 11, 34, 15, tzinfo=<UTC>)), ('Label_3', 'some_other_value')])]

        Raises
        ------
        TimeColumnValueError: If not at least one time column is given or if the specified
            time column to replace is not found.

        """
        if not time_columns:
            raise TimeColumnValueError("At least one time column is required!")

        data_converted = []

        for row in self._data_generator(data):
            if not replace_time_column:
                replace_time_column_key = (
                    self._find_first_time_column_key(
                        list(row.keys()), time_columns)
                )
            else:
                if replace_time_column not in list(row.keys()):
                    msg = "{0} not found in column names!".format(replace_time_column)
                    raise TimeColumnValueError(msg)

                replace_time_column_key = replace_time_column

            row_time_column_values = [value for key, value in row.items()
                                      if key in time_columns]
            row_time_converted = (
                self._parse_time_values(*row_time_column_values, to_utc=to_utc)
            )

            old_key = replace_time_column_key
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

    def export_to_csv(self, data, outfile_path, export_header=False, mode='a+',
                      include_time_zone=False):
        """Write data set to a CSV file.

        Parameters
        ----------
        data : list of OrderedDict
            Data set to export.
        outfile_path : str
            Output file's absolute path.
        export_header : bool, optional
            Write file header at the top of the output file.
        mode : str, optional
            Output file open mode, defaults to a+. See Python Docs for other
            mode options.
        include_time_zone : bool, optional
            Include time zone in string converted datetime values.

        Examples
        --------
        >>> import pytz
        >>> import shutil
        >>> import tempfile
        >>> temp_dir = tempfile.mkdtemp()
        >>> temp_outfile = os.path.join(temp_dir, 'temp_outfile.dat')

        >>> cr = CRGeneric()
        >>> data = [
        ...     OrderedDict([
        ...         ('Label_1', 'some_value'),
        ...         ('Label_2', datetime(2016, 5, 2, 12, 34, 15, tzinfo=pytz.UTC)),
        ...         ('Label_3', 'some_other_value')
        ...     ])
        ... ]
        >>> cr.export_to_csv(data, temp_outfile, export_header=True)
        >>> exported_data = cr.read_data(temp_outfile, header_row=0)
        >>> exported_data
        [OrderedDict([('Label_1', 'some_value'), ('Label_2', '2016-05-02 12:34:15'),
        ('Label_3', 'some_other_value')])]

        >>> cr.export_to_csv(data, temp_outfile, include_time_zone=True)
        >>> exported_data = cr.read_data(temp_outfile, header_row=0)
        >>> exported_data
        [OrderedDict([('Label_1', 'some_value'), ('Label_2', '2016-05-02 12:34:15'),
        ('Label_3', 'some_other_value')]), OrderedDict([('Label_1', 'some_value'),
        ('Label_2', '2016-05-02 12:34:15+0000'), ('Label_3', 'some_other_value')])]
        
        >>> shutil.rmtree(temp_dir)

        """
        data_to_export = [
            OrderedDict([(name, value) for name, value in row.items()]) for row in data]

        os.makedirs(os.path.dirname(outfile_path), exist_ok=True)

        if os.path.exists(outfile_path) and export_header:
            with open(outfile_path, 'r') as f:
                f_list = [line.strip() for line in f]
            if len(f_list) > 0:
                export_header = False

        with open(outfile_path, mode) as f_out:
            for row in CRGeneric._data_generator(data_to_export):
                if export_header:
                    header = [str(key) for key in row.keys()]
                    f_out.write(",".join(header) + "\n")
                    export_header = False
                f_out.write(
                    ",".join(self._values_to_strings(row, include_time_zone)) + "\n"
                )

    def read_data(self, infile_path, header=None, header_row=None, line_num=0,
                  convert_time=False, time_parsed_column=None, time_columns=None,
                  to_utc=False):
        """
        Reads data from a file and stores it in the parser's data structure format
        (see class documentation for details).

        Parameters
        ----------
        infile_path : str
            Input file's absolute path.
        header : list of str, optional
            Column names to map to each rows' values.
        header_row : int, optional
            Input file's header row fieldnames to map to each rows' values.
        line_num : int, optional
            Line number to start at. NOTE: Zero-based numbering.
        convert_time : bool, optional
            Convert datalogger specific time string representations to datetime objects.
        time_parsed_column : str, optional
            Converted time column name. If not given, use the name of the first
            time column.
        time_columns : list of str or int, optional
            Column(s) (names or indices) to use for time conversion.
        to_utc : bool, optional
            Convert time to UTC.

        Returns
        -------
        list of OrderedDict
            All data found from the given line number onwards.

        """
        data = [row for row in self._read_data(
            infile_path=infile_path,
            header=header,
            header_row=header_row,
            line_num=line_num
        )]

        if convert_time:
            data = self.convert_time(
                data=data,
                time_parsed_column=time_parsed_column,
                time_columns=time_columns,
                to_utc=to_utc
            )

        return data

    @staticmethod
    def update_column_names(data, column_names, match_row_lengths=True,
                            get_mismatched_row_lengths=False):
        """Updates a data set's column names.

        Parameters
        ----------
        data : list of OrderedDict
            Data set to process.
        column_names : list of str
            Names to map to each rows' values.
        match_row_lengths : bool, optional
            Filter out rows whose number of columns does not match the number of
            column names.
        get_mismatched_row_lengths : bool, optional
            Return a list of rows that did not pass the "match row lengths" test.

        Returns
        -------
        namedtuple
            Data set with updated column names. If get_mismatched_row_lengths is true,
            return a list of rows that did not pass the "match row lengths" test.

        """
        data_updated_column_names = []
        data_mismatched_row_lengths = []
        updated_column_names_result = namedtuple(
            'UpdatedColumnNamesResult',
            ['data_updated_column_names', 'data_mismatched_row_lengths']
        )

        for row in CRGeneric._data_generator(data):
            if match_row_lengths:
                if len(column_names) == len(row):
                    data_updated_column_names.append(OrderedDict(
                        [(name, value) for name, value in zip(column_names, row.values())]
                    ))
                else:
                    if get_mismatched_row_lengths:
                        data_mismatched_row_lengths.append(row)
            else:
                data_updated_column_names.append(OrderedDict(
                    [(name, value) for name, value in zip(column_names, row.values())]))
                if get_mismatched_row_lengths:
                    data_mismatched_row_lengths.append(row)

        if match_row_lengths and get_mismatched_row_lengths:
            return updated_column_names_result(
                data_updated_column_names, data_mismatched_row_lengths)

        return updated_column_names_result(data_updated_column_names, None)
