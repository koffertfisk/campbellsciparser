#!/usr/bin/env
# -*- coding: utf-8 -*-

"""
cr
--
Utility for parsing and exporting data outputted by Campbell Scientific, Inc.
CR-type dataloggers.

"""

import csv
import os

from collections import defaultdict, namedtuple
from datetime import datetime

from campbellsciparser.dataset import DataSet
from campbellsciparser.dataset import Row

import pytz


class ArrayIdsInfoValueError(Exception):
    """Raised whenever provided array ids info is insufficient. """
    pass


class ArrayIdsExportInfoError(Exception):
    """Raised whenever information provided for mixed array exporting is insufficient. """
    pass


class DataSetTypeError(Exception):
    """Raised whenever an unsupported data set format is provided. """
    pass


class TimeColumnValueError(Exception):
    """Raised whenever there are problems parsing specific time columns. """
    pass


class TimeParsingError(Exception):
    """
    Raised whenever there's an error when creating a datetime object (and parsing errors
    are not ignored).

    """
    pass


class UnknownPytzTimeZoneError(Exception):
    """Raised whenever a provided time zone is not a valid pytz time zone. """
    pass


def _data_generator(data):
    """
    Iterate over the rows of a data set (list of ordered dictionaries, i.e. rows
    and columns).

    Parameters
    ----------
    data : DataSet
        Data set to iterate.

    Yields
    ------
    Row
        The data set's next row.

    """
    for row in data:
        yield row


def _convert_time_zone(dt, to_time_zone):
    """Converts datetime from one time zone to another.

    Parameters
    ----------
    dt : datetime
        Datetime to time zone convert.
    to_time_zone : pytz time zone.
        Time zone to convert to.

    Returns
    -------
    datetime
        Time zone converted datetime object.

    """

    return dt.astimezone(to_time_zone)


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


def _extract_columns_data_generator(data, *column_names, **time_range):
    """Iterator for extract_column_data

    Parameters
    ----------
    data : DataSet
        Data set to extract from.
    *column_names : Column(s) to extract.
    **time_range : Extract data from, to or between timestamps. If this mode is used, the
        data set must be time converted.

    Yields
    ------
    Row
        The next row in a data set where specific columns have been extracted.

    Raises
    ------
        TimeColumnValueError: If the keyword argument time_column is not provided or if
            the given time column does not exist in a row.
    """

    if time_range:
        time_column = time_range.get('time_column')

        if not time_column:
            raise TimeColumnValueError('No time column provided!')

        from_timestamp = time_range.get('from_timestamp', datetime.fromtimestamp(0, pytz.UTC))
        to_timestamp = time_range.get('to_timestamp')

        if not to_timestamp:
            to_timestamp = datetime.utcnow()
            to_timestamp = to_timestamp.replace(tzinfo=pytz.utc)

    for row in _data_generator(data):
        if time_range:
            if time_column not in row:
                raise TimeColumnValueError("Invalid time column")
            if to_timestamp >= row.get(time_column) >= from_timestamp:
                if column_names:
                    yield Row([(name, value) for name, value in row.items() if name
                                       in column_names])
                else:
                    yield Row([(name, value) for name, value in row.items()])
        else:
            if column_names:
                yield Row([(name, value) for name, value in row.items() if name
                                   in column_names])
            else:
                yield Row([(name, value) for name, value in row.items()])


def _find_first_time_column_name(column_names, time_columns):
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
        msg = "First time column '{first_time_column}' not found in column names!"
        msg = msg.format(first_time_column=time_columns[0])
        raise TimeColumnValueError(msg)


def _parse_custom_time_formats(time_format_args_library, *time_values):
    """
    Parses CR-type datalogger specific time representations that are not supported
    by the Python standard datetime module.

    Parameters
    ----------
    time_format_args_library : list of str
        List of the maximum expected string format columns sequence to match against
        when parsing time values.
    *time_values
        Time strings to be matched against the datalogger parser time format library.

    Returns
    -------
    namedtuple
        Two comma separated strings in a namedtuple; one for the time string formats and
        one for the time values.

    """
    parsing_info = namedtuple('ParsedCustomTimeInfo', ['parsed_time_format', 'parsed_time'])
    found_time_format_args = []
    found_time_values = []

    for format_arg, time_arg in zip(time_format_args_library, time_values):
        # Handle custom time formats here
        if format_arg == '%H%M':
            time_arg = _parse_hourminute(time_arg)
        found_time_format_args.append(format_arg)
        found_time_values.append(time_arg)

    time_format_string = ','.join(found_time_format_args)
    time_values_string = ','.join(found_time_values)

    return parsing_info(time_format_string, time_values_string)


def _parse_hourminute(hour_minute_str):
    """
    Parses the custom time format column 'Hour/Minute'. The time in the format HHMM is
    determined by the length of the given time string since CR-type dataloggers using this
    format does not  output hours and minutes with leading zeros (except for  midnight,
    which is output as 0).

    Examples of the 'Hour/Minute' pattern and how they would be parsed:

    CR time string: '5'
    HHMM time string: '0005'

    CR time string: '35'
    HHMM time string: '0035'

    CR time string: '159'
    HHMM time string: '0159'

    CR time string: '2345'
    HHMM time string: '2345'

    Parameters
    ----------
    hour_minute_str : str
        Hour/Minute string to be parsed.

    Returns
    -------
    The time parsed in the format HHMM.

    Raises
    ------
    TimeColumnValueError: If the given time column value could not be parsed.

    """
    if len(hour_minute_str) == 1:  # 0 - 9
        minute = hour_minute_str
        parsed_time = "000" + minute
    elif len(hour_minute_str) == 2:  # 10 - 59
        minute = hour_minute_str[-2:]
        parsed_time = "00" + minute
    elif len(hour_minute_str) == 3:  # 100 - 959
        hour = hour_minute_str[:1]
        minute = hour_minute_str[-2:]
        parsed_time = "0" + hour + minute
    elif len(hour_minute_str) == 4:  # 1000 - 2359
        hour = hour_minute_str[:2]
        minute = hour_minute_str[-2:]
        parsed_time = hour + minute
    else:
        msg = "Hour/Minute {hour_minute} could not be parsed!".format(
            hour_minute=hour_minute_str)
        raise TimeColumnValueError(msg)

    return parsed_time


def _parse_time_values(pytz_time_zone, time_format_args_library, *time_values, **parsing_info):
    """Converts datalogger model specific time representations into a datetime object.

    Parameters
    ----------
    pytz_time_zone : pytz time zone
        Collected data's pytz time zone, used for localization and time conversion.
    time_format_args_library : list of str
        List of the maximum expected string format columns sequence to match against
        when parsing time values.
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
    parsed_time_format, parsed_time = _parse_custom_time_formats(
        time_format_args_library, *time_values)

    try:
        dt = datetime.strptime(parsed_time, parsed_time_format)
    except ValueError:
        msg = "Could not parse time string {parsed_time} using the format {parsed_time_format}"
        msg = msg.format(parsed_time=parsed_time, parsed_time_format=parsed_time_format)
        print(msg)
        ignore_parsing_error = parsing_info.get('ignore_parsing_error', False)

        if ignore_parsing_error:
            local_dt = datetime.fromtimestamp(0, pytz_time_zone)
            msg = "Setting time value to epoch time ({epoch_dt})"
            msg = msg.format(epoch_dt=str(local_dt))
            print(msg)
        else:
            raise TimeParsingError(msg)
    else:
        try:
            local_dt = pytz_time_zone.localize(dt)
        except ValueError:
            print("Datetime already localized.")
            local_dt = dt

    parsed_dt = local_dt

    if parsing_info.get('to_utc', False):
        utc_dt = local_dt.astimezone(pytz.utc)
        parsed_dt = utc_dt

    return parsed_dt


def _process_mixed_array_rows(infile_path, first_line_num=0, last_line_num=None, fix_floats=True):
    """Iterator for _read_mixed_array_data.

    Parameters
    ----------
    infile_path : str
        Input file's absolute path.
    first_line_num : int, optional
        First line number to read. NOTE: Zero-based numbering.
    last_line_num : int, optional
        Last line number to read. NOTE: Zero-based numbering.
    fix_floats : bool
        Correct leading zeros for floating points values since many older CR-type
        dataloggers strips leading zeros.

    Yields
    ------
    Row
        The next row read and processed from a mixed array format CSV file.

    """
    with open(infile_path, 'r') as f:
        rows = csv.reader(f)
        replacements = {'.': '0.', '-.': '-0.'}  # Patterns to look for
        for row in rows:
            # Correct reader for zero-based numbering
            if first_line_num <= (rows.line_num - 1):
                if isinstance(last_line_num, int) and last_line_num < (rows.line_num - 1):
                    break
                if fix_floats:
                    for i, value in enumerate(row):
                        for source, replacement in replacements.items():
                            if value.startswith(source):
                                row[i] = value.replace(source, replacement)

                yield Row([(i, value) for i, value in enumerate(row)])


def _process_table_rows(infile_path, header=None, header_row=None, first_line_num=0,
                        last_line_num=None):
    """Iterator for _read_table_data.

    Parameters
    ----------
    infile_path : str
        Input file's absolute path.
    header : list of str, optional
        Column names to map to each rows' values.
    header_row : int, optional
        Input file's header row to map to each rows' values.
    first_line_num : int, optional
        First line number to read. NOTE: Zero-based numbering.
    last_line_num : int, optional
        Last line number to read. NOTE: Zero-based numbering.

    Yields
    ------
    Row
        The next row read and processed from a table format CSV file.

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
                if first_line_num <= (rows.line_num - 1):
                    if isinstance(last_line_num, int) and last_line_num < (rows.line_num - 1):
                        break
                    yield Row(
                        [(header, value) for header, value in zip(header, row)])
        else:
            for row in rows:
                if first_line_num <= (rows.line_num - 1):
                    if isinstance(last_line_num, int) and last_line_num < (rows.line_num - 1):
                        break
                    yield Row([(i, value) for i, value in enumerate(row)])


def _read_table_data(infile_path, header=None, header_row=None, first_line_num=0,
                     last_line_num=None):
    """Iterate over data read from a CSV file starting at a given line number.

    Parameters
    ----------
    infile_path : str
        Input file's absolute path.
    header : list of str, optional
        Column names to map to each rows' values.
    header_row : int, optional
        Input file's header row to map to each rows' values.
    first_line_num : int, optional
        First line number to read. NOTE: Zero-based numbering.
    last_line_num : int, optional
        Last line number to read. NOTE: Zero-based numbering.

    Yields
    ------
    Row
        The next row read and processed from an input CSV file.

    """
    for row in _process_table_rows(
            infile_path, header=header, header_row=header_row, first_line_num=first_line_num,
            last_line_num=last_line_num):
        yield row


def _read_mixed_array_data(infile_path, first_line_num=0, last_line_num=None, fix_floats=True):
    """Iterate over mixed data read from given a CSV file starting at a given line number.

    Parameters
    ----------
    infile_path : str
        Input file's absolute path.
    first_line_num : int, optional
        First line number to read. NOTE: Zero-based numbering.
    last_line_num : int, optional
        Last line number to read. NOTE: Zero-based numbering.
    fix_floats : bool
        Correct leading zeros for floating points values since many older CR-type
        dataloggers strips leading zeros.

    Returns
    -------
    Each processed row, one at a time.

    """
    for row in _process_mixed_array_rows(
            infile_path=infile_path, first_line_num=first_line_num,
            last_line_num=last_line_num, fix_floats=fix_floats):
        yield row


def _values_to_strings(row, include_time_zone=False):
    """Returns a list of the values in a row, converted to strings.

    Parameters
    ----------
    row : Row
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
            row[name] = _datetime_to_string(value, include_time_zone=include_time_zone)
        else:
            row[name] = str(value)

    return row.values()


def convert_time_zone(data, time_column, to_time_zone):
    """Converts a data set's time zone.

    Parameters
    ----------
    data : DataSet
        Data set to time zone convert.
    time_column : str or int
        Time column name (or index) to convert.
    to_time_zone : String representation of a valid pytz time zone.
        Time zone to convert all rows' timestamp to.

    Returns
    -------
    list of Row
        Time zone converted data set.

    Examples
    --------
    >>> import pytz

    >>> data = DataSet([
    ...     Row([
    ...         ('Label_1', 'some_value_1'),
    ...         ('Label_2', datetime(2016, 5, 2, 12, 34, 15,
    ...             tzinfo=pytz.timezone('Europe/Stockholm'))),
    ...         ('Label_3', 'some_other_value_1')
    ...     ]),
    ...     Row([
    ...         ('Label_1', 'some_value_2'),
    ...         ('Label_2', datetime(2016, 5, 2, 13, 34, 15,
    ...             tzinfo=pytz.timezone('Europe/Stockholm'))),
    ...         ('Label_3', 'some_other_value_2')
    ...     ]),
    ...     Row([
    ...         ('Label_1', 'some_value_3'),
    ...         ('Label_2', datetime(2016, 5, 2, 14, 34, 15,
    ...             tzinfo=pytz.timezone('Europe/Stockholm'))),
    ...         ('Label_3', 'some_other_value_2')
    ...     ])
    ... ])

    >>> convert_time_zone(data, 'Label_2', 'Asia/Hong_Kong')
    DataSet([Row([('Label_1', 'some_value_1'), ('Label_2', datetime.datetime(2016, 5, 2, \
19, 34, 15, tzinfo=<DstTzInfo 'Asia/Hong_Kong' HKT+8:00:00 STD>)), ('Label_3', \
'some_other_value_1')]), Row([('Label_1', 'some_value_2'), ('Label_2', \
datetime.datetime(2016, 5, 2, 20, 34, 15, \
tzinfo=<DstTzInfo 'Asia/Hong_Kong' HKT+8:00:00 STD>)), ('Label_3', 'some_other_value_2')]), \
Row([('Label_1', 'some_value_3'), ('Label_2', \
datetime.datetime(2016, 5, 2, 21, 34, 15, tzinfo=<DstTzInfo 'Asia/Hong_Kong' HKT+8:00:00 STD>)), \
('Label_3', 'some_other_value_2')])])

    Raises
    ------
    UnknownPytzTimeZoneError: If the provided time zone is not a valid pytz time zone.

    """
    try:
        pytz_to_time_zone = pytz.timezone(to_time_zone)
    except pytz.UnknownTimeZoneError:
        msg = "{time_zone} is not a valid pytz time zone! "
        msg += "See pytz docs for valid time zones".format(time_zone=to_time_zone)
        raise UnknownPytzTimeZoneError(msg)

    data_time_zone_converted = DataSet([])

    for row in _data_generator(data):
        row[time_column] = _convert_time_zone(row.get(time_column), pytz_to_time_zone)
        data_time_zone_converted.append(row)

    return data_time_zone_converted


def export_array_ids_to_csv(data, array_ids_info, export_header=False,
                            mode='a+', include_time_zone=False):
    """Write array id separated data to a CSV file.

    Parameters
    ----------
    data : dict
        Data set to export.
    array_ids_info : dict
        Array ids to export. Contains output file paths.
    export_header : bool, optional
        Write file header at the top of the output file.
    mode : str, optional
        Output file open mode, defaults to a+. See Python Docs for other
        mode options.
    include_time_zone : bool, optional
        Include time zone in string converted datetime values.

    Examples
    --------
    >>> import shutil
    >>> import tempfile
    >>> temp_dir = tempfile.mkdtemp()
    >>> temp_outfile_100 = os.path.join(temp_dir, 'temp_outfile_100.dat')
    >>> temp_outfile_101 = os.path.join(temp_dir, 'temp_outfile_101.dat')

    >>> data = {
    ...     '100': DataSet([Row([('ID', '100'), ('Year', '2016'), ('Julian Day', '123'), ('Data', '54.2')])]),
    ...     '101': DataSet([Row([('ID', '101'), ('Year', '2016'), ('Julian Day', '123'),
    ...         ('Hour/Minute', '1245'), ('Data', '44.2')])])
    ... }
    >>> array_ids_info = {'100': {'file_path': temp_outfile_100}, '101': {'file_path': temp_outfile_101}}
    >>> export_array_ids_to_csv(data, array_ids_info, export_header=True)

    >>> exported_data_100 = read_table_data(temp_outfile_100, header_row=0)
    >>> exported_data_101 = read_table_data(temp_outfile_101, header_row=0)
    >>> exported_data_100
    DataSet([Row([('ID', '100'), ('Year', '2016'), ('Julian Day', '123'), ('Data', '54.2')])])
    >>> exported_data_101
    DataSet([Row([('ID', '101'), ('Year', '2016'), ('Julian Day', '123'), ('Hour/Minute', \
'1245'), ('Data', '44.2')])])

    >>> shutil.rmtree(temp_dir)

    Raises
    ------
    ArrayIdsInfoValueError: If not at least one array id in array_ids_info is found.
    ArrayIdsExportInfoError: If no information for a certain array id is found.

    """
    if len(array_ids_info) < 1:
        raise ArrayIdsInfoValueError("At least one array id must be given!")

    data_filtered = filter_mixed_array_data(data, *array_ids_info.keys())

    for array_id, array_id_data in data_filtered.items():
        export_info = array_ids_info.get(array_id)
        if not export_info:
            msg = "No information was found for array id {0}".format(array_id)
            raise ArrayIdsExportInfoError(msg)
        file_path = export_info.get('file_path')
        if not file_path:
            msg = "Not file path was found for array id {0}".format(array_id)
            raise ArrayIdsExportInfoError(msg)

        export_to_csv(
            data=array_id_data,
            outfile_path=file_path,
            export_header=export_header,
            mode=mode,
            include_time_zone=include_time_zone)


def export_to_csv(data, outfile_path, export_header=False, mode='a+',
                  include_time_zone=False):
    """Write data set to a CSV file.

    Parameters
    ----------
    data : DataSet
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
    >>> import shutil
    >>> import tempfile
    >>> temp_dir = tempfile.mkdtemp()
    >>> temp_outfile = os.path.join(temp_dir, 'temp_outfile.dat')

    >>> data = DataSet([
    ...     Row([
    ...         ('Label_1', 'some_value'),
    ...         ('Label_2', datetime(2016, 5, 2, 12, 34, 15, tzinfo=pytz.UTC)),
    ...         ('Label_3', 'some_other_value')
    ...     ])
    ... ])

    >>> export_to_csv(data, temp_outfile, export_header=True)
    >>> exported_data = read_table_data(temp_outfile, header_row=0)
    >>> exported_data
    DataSet([Row([('Label_1', 'some_value'), ('Label_2', '2016-05-02 12:34:15'), \
('Label_3', 'some_other_value')])])

    >>> export_to_csv(data, temp_outfile, include_time_zone=True)
    >>> exported_data = read_table_data(temp_outfile, header_row=0)
    >>> exported_data
    DataSet([Row([('Label_1', 'some_value'), ('Label_2', '2016-05-02 12:34:15'), \
('Label_3', 'some_other_value')]), Row([('Label_1', 'some_value'), \
('Label_2', '2016-05-02 12:34:15+0000'), ('Label_3', 'some_other_value')])])

    >>> shutil.rmtree(temp_dir)

    """
    data_to_export = DataSet([
        Row([(name, value) for name, value in row.items()]) for row in data])

    os.makedirs(os.path.dirname(outfile_path), exist_ok=True)

    if os.path.exists(outfile_path) and export_header:
        with open(outfile_path, 'r') as f:
            f_list = [line.strip() for line in f]
        if len(f_list) > 0:
            export_header = False

    with open(outfile_path, mode) as f_out:
        for row in _data_generator(data_to_export):
            if export_header:
                header = [str(key) for key in row.keys()]
                f_out.write(",".join(header) + "\n")
                export_header = False

            f_out.write(",".join(_values_to_strings(row, include_time_zone)) + "\n")


def extract_columns_data(data, *column_names, **time_range):
    """Extract data from specific column(s).

    Parameters
    ----------
    data : DataSet
        Data set to extract from.
    *column_names : Column(s) to extract.
    **time_range : Extract data from, to or between timestamps. If this mode is used, the
        data set must be time converted.

    Returns
    -------
    DataSet
        Data extracted from one or more columns

    Raises
    ------
        TimeColumnValueError: If the keyword argument time_column is not provided or if
            the given time column does not exist in a row.

    Example
    -------
    >>> data = DataSet([
    ...     Row([
    ...         ('Label_1', 'some_value_1'),
    ...         ('Label_2', datetime(2016, 5, 2, 12, 34, 15, tzinfo=pytz.UTC)),
    ...         ('Label_3', 'some_other_value_1')
    ...     ]),
    ...     Row([
    ...         ('Label_1', 'some_value_2'),
    ...         ('Label_2', datetime(2016, 5, 2, 13, 34, 15, tzinfo=pytz.UTC)),
    ...         ('Label_3', 'some_other_value_2')
    ...     ]),
    ...     Row([
    ...         ('Label_1', 'some_value_3'),
    ...         ('Label_2', datetime(2016, 5, 2, 14, 34, 15, tzinfo=pytz.UTC)),
    ...         ('Label_3', 'some_other_value_3')
    ...     ])
    ... ])

    >>> extract_columns_data(data, 'Label_3')
    DataSet([Row([('Label_3', 'some_other_value_1')]), Row([('Label_3', \
'some_other_value_2')]), Row([('Label_3', 'some_other_value_3')])])


    >>> extract_columns_data(data, 'Label_2', 'Label_3', time_column='Label_2',
    ... from_timestamp=datetime(2016, 5, 2, 13, 0, 0, tzinfo=pytz.UTC))
    DataSet([Row([('Label_2', datetime.datetime(2016, 5, 2, 13, 34, 15, tzinfo=<UTC>)), \
('Label_3', 'some_other_value_2')]), Row([('Label_2', datetime.datetime(2016, \
5, 2, 14, 34, 15, tzinfo=<UTC>)), ('Label_3', 'some_other_value_3')])])


    >>> extract_columns_data(data, 'Label_2', 'Label_3', time_column='Label_2',
    ... to_timestamp=datetime(2016, 5, 2, 14, 0, 0, tzinfo=pytz.UTC))
    DataSet([Row([('Label_2', datetime.datetime(2016, 5, 2, 12, 34, 15, tzinfo=<UTC>)), \
('Label_3', 'some_other_value_1')]), Row([('Label_2', datetime.datetime(2016, \
5, 2, 13, 34, 15, tzinfo=<UTC>)), ('Label_3', 'some_other_value_2')])])

    >>> extract_columns_data(data, 'Label_2', 'Label_3', time_column='Label_2',
    ... from_timestamp=datetime(2016, 5, 2, 13, 0, 0, tzinfo=pytz.UTC),
    ... to_timestamp = datetime(2016, 5, 2, 14, 0, 0, tzinfo=pytz.UTC))
    DataSet([Row([('Label_2', datetime.datetime(2016, 5, 2, 13, 34, 15, tzinfo=<UTC>)), \
('Label_3', 'some_other_value_2')])])


    """
    extracted_columns_data = DataSet([row for row in _extract_columns_data_generator(
        data, *column_names, **time_range)])

    return extracted_columns_data


def filter_mixed_array_data(data, *array_ids):
    """Filter mixed array data set by array ids.

    Parameters
    ----------
    data : Either dict of DataSet or DataSet
        Array id separated (dict of list of Row) or mixed array data (list).
    *array_ids: Array ids to filter by. If no arguments are given, return unfiltered
        data set.

    Returns
    -------
    DataSet
        Filtered data set if array ids are given, unfiltered otherwise. If a mixed
        data set is given, return unfiltered data set split by its array ids.

    Examples
    --------
    >>> data = DataSet([
    ...     Row([('ID', '100'), ('Year', '2016'), ('Julian Day', '123'), ('Data', '54.2')]),
    ...     Row([('ID', '101'), ('Year', '2016'), ('Julian Day', '123'),
    ...     ('Hour/Minute', '1245'), ('Data', '44.2')])
    ... ])
    >>> filter_mixed_array_data(data, '100')
    defaultdict(<class 'campbellsciparser.dataset.DataSet'>, {'100': DataSet([Row([('ID', \
'100'), ('Year', '2016'), ('Julian Day', '123'), ('Data', '54.2')])])})

    >>> filter_mixed_array_data(data, '101')
    defaultdict(<class 'campbellsciparser.dataset.DataSet'>, {'101': DataSet([Row([('ID', \
'101'), ('Year', '2016'), ('Julian Day', '123'), ('Hour/Minute', '1245'), ('Data', '44.2')])])})
    >>> data_filtered = filter_mixed_array_data(data, '100', '101')
    >>> data_filtered.get('100')
    DataSet([Row([('ID', '100'), ('Year', '2016'), ('Julian Day', '123'), ('Data', '54.2')])])
    >>> data_filtered.get('101')
    DataSet([Row([('ID', '101'), ('Year', '2016'), ('Julian Day', '123'), ('Hour/Minute', \
'1245'), ('Data', '44.2')])])

    Raises:
        DataTypeError: if anything else than list or dictionary data is given.

    """
    data_filtered = defaultdict(DataSet)

    if isinstance(data, dict):
        if not array_ids:
            return data
        for array_id, array_id_data in data.items():
            if array_id in array_ids:
                data_filtered[array_id] = array_id_data
    elif isinstance(data, DataSet):
        for row in _data_generator(data):
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
        msg = "Data collection of type {data_type} not supported. "
        msg += "Valid collection types are dict and list.".format(data_type=type(data))
        raise DataSetTypeError(msg)

    return data_filtered


def parse_time(data, time_zone, time_format_args_library, time_columns,
               time_parsed_column=None, replace_time_column=None, to_utc=False):
    """
    Parses specific time columns from a data set into a datetime object.

    Parameters
    ----------
    data : DataSet
        Data set to convert.
    time_zone : str
        String representation of a valid pytz time zone. (See pytz docs
        for a list of valid time zones). The time zone refers to collected data's
        time zone, which defaults to UTC and is used for localization and time conversion.
    time_format_args_library : list of str
        List of the maximum expected string
        format columns sequence to match against when parsing time values. Defaults to
        empty library.
    time_columns : list of str or int
        Column(s) (names or indices) to use for time conversion.
    time_parsed_column : str, optional
        Converted time column name. If not given, use the name of the first
        time column.
    replace_time_column : str or int, optional
        Column (name or index) to place the parsed datetime object at. If not given,
        insert at the first time column index.
    to_utc : bool, optional
        Convert time to UTC.

    Returns
    -------
    DataSet
        Time converted data set.

    Examples
    --------
    >>> data = DataSet([
    ...     Row([
    ...         (0, 'some_value'),
    ...         (1, '2016'),
    ...         (2, '123'),
    ...         (3, '1234'),
    ...         (4, 'some_other_value')
    ...     ])
    ... ])
    >>> parse_time(data=data, time_zone='Etc/GMT-1',
    ...     time_format_args_library=['%Y', '%j', '%H%M'], time_columns=[1, 2, 3])
    DataSet([Row([(0, 'some_value'), \
(1, datetime.datetime(2016, 5, 2, 12, 34, tzinfo=<StaticTzInfo 'Etc/GMT-1'>)), \
(4, 'some_other_value')])])

    >>> data = DataSet([
    ...     Row([
    ...         ('Label_1', 'some_value'),
    ...         ('Label_2', '2016-05-02 12:34:15'),
    ...         ('Label_3', 'some_other_value')
    ...     ])
    ... ])
    >>> parse_time(
    ...     data, time_zone='Etc/GMT-1', time_format_args_library=['%Y-%m-%d %H:%M:%S'],
    ...     time_parsed_column='TIMESTAMP', time_columns=['Label_2'], to_utc=True)
    DataSet([Row([('Label_1', 'some_value'), ('TIMESTAMP', datetime.datetime(2016, 5, 2, \
11, 34, 15, tzinfo=<UTC>)), ('Label_3', 'some_other_value')])])

    Raises
    ------
    TimeColumnValueError: If not at least one time column is given or if the specified
        time column to replace is not found.
    UnknownPytzTimeZoneError: If the provided time zone is not a valid pytz time zone.

    """
    try:
        pytz_time_zone = pytz.timezone(time_zone)
    except pytz.UnknownTimeZoneError:
        msg = "{time_zone} is not a valid pytz time zone! "
        msg += "See pytz docs for valid time zones".format(time_zone=time_zone)
        raise UnknownPytzTimeZoneError(msg)

    if not time_format_args_library:
        time_format_args_library = []
    if not time_columns:
        raise TimeColumnValueError("At least one time column is required!")

    data_converted = DataSet([])

    for row in _data_generator(data):
        if not replace_time_column:
            replace_time_column_name = (
                _find_first_time_column_name(
                    list(row.keys()), time_columns)
            )
        else:
            if replace_time_column not in list(row.keys()):
                msg = "{0} not found in column names!".format(replace_time_column)
                raise TimeColumnValueError(msg)

            replace_time_column_name = replace_time_column

        row_time_column_values = [value for name, value in row.items()
                                  if name in time_columns]
        row_time_converted = (
            _parse_time_values(
                pytz_time_zone, time_format_args_library,
                *row_time_column_values, to_utc=to_utc)
        )

        old_name = replace_time_column_name
        new_name = old_name
        if time_parsed_column:
            new_name = time_parsed_column

        row_converted = Row(
            (new_name if name == old_name else name, value) for name, value in row.items())
        row_converted[new_name] = row_time_converted

        for time_column in time_columns:
            if time_column in row_converted and time_column != new_name:
                del row_converted[time_column]

        data_converted.append(row_converted)

    return data_converted


def read_array_ids_data(infile_path, first_line_num=0, last_line_num=None, fix_floats=True,
                        array_id_names=None):
    """Parses data filtered by array id (each rows' first element) read from a file.

    Parameters
    ----------
    infile_path : str
        Input file's absolute path.
    first_line_num : int, optional
        First line number to read. NOTE: Zero-based numbering.
    last_line_num : int, optional
        Last line number to read. NOTE: Zero-based numbering.
    fix_floats : bool
        Correct leading zeros for floating points values since many older CR-type
        dataloggers strips leading zeros.
    array_id_names : dict
        Lookup table for array id name translation.

    Returns
    -------
    dict of DataSet
        All data found from the given line number onwards, filtered by array id.

    Examples
    --------
    >>> import shutil
    >>> import tempfile
    >>> temp_dir = tempfile.mkdtemp()
    >>> temp_outfile = os.path.join(temp_dir, 'temp_outfile.dat')

    >>> data = DataSet([
    ...     Row([('ID', '100'), ('Year', '2016'), ('Julian Day', '123'), ('Data', '54.2')]),
    ...     Row([('ID', '101'), ('Year', '2016'), ('Julian Day', '123'),
    ...     ('Hour/Minute', '1245'), ('Data', '44.2')])
    ... ])
    >>> export_to_csv(data, temp_outfile)

    >>> array_id_names = {'100': 'Daily', '101': 'Hourly'}
    >>> exported_data = read_array_ids_data(temp_outfile, array_id_names=array_id_names)
    >>> exported_data.get('Hourly')
    DataSet([Row([(0, '101'), (1, '2016'), (2, '123'), (3, '1245'), (4, '44.2')])])
    >>> exported_data.get('Daily')
    DataSet([Row([(0, '100'), (1, '2016'), (2, '123'), (3, '54.2')])])

    >>> shutil.rmtree(temp_dir)

    """
    if not array_id_names:
        array_id_names = {}

    array_ids = [array_id for array_id in array_id_names]

    data_mixed = read_mixed_array_data(
        infile_path=infile_path,
        first_line_num=first_line_num,
        last_line_num=last_line_num,
        fix_floats=fix_floats)

    data_by_array_ids = filter_mixed_array_data(data_mixed, *array_ids)

    for array_id, array_name in array_id_names.items():
        if array_id in data_by_array_ids:
            if array_name:
                data_by_array_ids[array_name] = data_by_array_ids.pop(array_id)

    return data_by_array_ids


def read_mixed_array_data(infile_path, first_line_num=0, last_line_num=None, fix_floats=True):
    """
    Reads mixed array data from a file (without array ids filtering) and stores it
    in the CR module's data structure format (see module documentation for details).

    Parameters
    ----------
    infile_path : str
        Input file's absolute path.
    first_line_num : int, optional
        First line number to read. NOTE: Zero-based numbering.
    last_line_num : int, optional
        Last line number to read. NOTE: Zero-based numbering.
    fix_floats : bool
        Correct leading zeros for floating points values since many older CR-type
        dataloggers strips leading zeros.

    Returns
    -------
    DataSet
        All data found from the given line number onwards.

    Example
    -------
    >>> import shutil
    >>> import tempfile
    >>> temp_dir = tempfile.mkdtemp()
    >>> temp_outfile = os.path.join(temp_dir, 'temp_outfile.dat')

    >>> data = DataSet([
    ...     Row([('ID', '100'), ('Year', '2016'), ('Julian Day', '123'), ('Data', '54.2')]),
    ...     Row([('ID', '101'), ('Year', '2016'), ('Julian Day', '123'),
    ...     ('Hour/Minute', '1245'), ('Data', '44.2')])
    ... ])
    >>> export_to_csv(data, temp_outfile)

    >>> exported_data = read_mixed_array_data(temp_outfile)
    >>> exported_data
    DataSet([Row([(0, '100'), (1, '2016'), (2, '123'), (3, '54.2')]), Row([(0, '101'), \
(1, '2016'), (2, '123'), (3, '1245'), (4, '44.2')])])

    >>> shutil.rmtree(temp_dir)

    """
    return DataSet([row for row in _read_mixed_array_data(
        infile_path=infile_path, first_line_num=first_line_num,
        last_line_num=last_line_num, fix_floats=fix_floats)])


def read_table_data(infile_path, header=None, header_row=None, first_line_num=0,
                    last_line_num=None, parse_time_columns=False, time_zone='UTC',
                    time_format_args_library=None, time_parsed_column=None,
                    time_columns=None, to_utc=False):
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
    first_line_num : int, optional
        First line number to read. NOTE: Zero-based numbering.
    last_line_num : int, optional
        Last line number to read. NOTE: Zero-based numbering.
    parse_time_columns : bool, optional
        Convert datalogger specific time string representations to datetime objects.
    time_zone : str
        String representation of a valid pytz time zone. (See pytz docs
        for a list of valid time zones). The time zone refers to collected data's
        time zone, which defaults to UTC and is used for localization and time conversion.
    time_format_args_library : list of str
        List of the maximum expected string
        format columns sequence to match against when parsing time values. Defaults to
        empty library.
    time_parsed_column : str, optional
        Converted time column name. If not given, use the name of the first
        time column.
    time_columns : list of str or int, optional
        Column(s) (names or indices) to use for time conversion.
    to_utc : bool, optional
        Convert time to UTC.

    Returns
    -------
    DataSet
        All data found from the given line number onwards.

    Examples
    --------
    >>> import pytz
    >>> import shutil
    >>> import tempfile
    >>> temp_dir = tempfile.mkdtemp()
    >>> temp_outfile = os.path.join(temp_dir, 'temp_outfile.dat')

    >>> data = DataSet([
    ...     Row([
    ...         ('Label_1', 'some_value'),
    ...         ('Label_2', datetime(2016, 5, 2, i, 34, 15, tzinfo=pytz.UTC)),
    ...         ('Label_3', 'some_other_value')])
    ...     for i in range(20)
    ... ])
    >>> export_to_csv(data, temp_outfile, export_header=True)

    >>> exported_data = read_table_data(temp_outfile, header_row=0)
    >>> exported_data.data[:3]
    [Row([('Label_1', 'some_value'), ('Label_2', '2016-05-02 00:34:15'), \
('Label_3', 'some_other_value')]), Row([('Label_1', 'some_value'), \
('Label_2', '2016-05-02 01:34:15'), ('Label_3', 'some_other_value')]), \
Row([('Label_1', 'some_value'), ('Label_2', '2016-05-02 02:34:15'), \
('Label_3', 'some_other_value')])]

    >>> new_column_names = ['New_Label_1', 'New_Label_2', 'New_Label_3']
    >>> exported_data = read_table_data(temp_outfile, header=new_column_names, first_line_num=1)
    >>> exported_data.data[:3]
    [Row([('New_Label_1', 'some_value'), ('New_Label_2', '2016-05-02 00:34:15'), \
('New_Label_3', 'some_other_value')]), Row([('New_Label_1', 'some_value'), \
('New_Label_2', '2016-05-02 01:34:15'), ('New_Label_3', 'some_other_value')]), \
Row([('New_Label_1', 'some_value'), ('New_Label_2', '2016-05-02 02:34:15'), \
('New_Label_3', 'some_other_value')])]

    >>> exported_data = read_table_data(temp_outfile, header_row=0, first_line_num=18)
    >>> exported_data.data
    [Row([('Label_1', 'some_value'), ('Label_2', '2016-05-02 17:34:15'), \
('Label_3', 'some_other_value')]), Row([('Label_1', 'some_value'), \
('Label_2', '2016-05-02 18:34:15'), ('Label_3', 'some_other_value')]), \
Row([('Label_1', 'some_value'), ('Label_2', '2016-05-02 19:34:15'), \
('Label_3', 'some_other_value')])]

    >>> exported_data = read_table_data(
    ...     temp_outfile,
    ...     header_row=0,
    ...     parse_time_columns=True,
    ...     time_zone='UTC',
    ...     time_format_args_library=['%Y-%m-%d %H:%M:%S'],
    ...     time_parsed_column='TIMESTAMP',
    ...     time_columns=['Label_2']
    ... )
    >>> exported_data.data[:3]
    [Row([('Label_1', 'some_value'), ('TIMESTAMP', datetime.datetime(2016, 5, 2, \
0, 34, 15, tzinfo=<UTC>)), ('Label_3', 'some_other_value')]), Row([\
('Label_1', 'some_value'), ('TIMESTAMP', datetime.datetime(2016, 5, 2, 1, 34, 15, \
tzinfo=<UTC>)), ('Label_3', 'some_other_value')]), Row([('Label_1', \
'some_value'), ('TIMESTAMP', datetime.datetime(2016, 5, 2, 2, 34, 15, tzinfo=<UTC>)), \
('Label_3', 'some_other_value')])]

    >>> shutil.rmtree(temp_dir)

    """
    data = DataSet([row for row in _read_table_data(
        infile_path=infile_path,
        header=header,
        header_row=header_row,
        first_line_num=first_line_num,
        last_line_num=last_line_num
    )])

    if parse_time_columns:
        data = parse_time(
            data=data,
            time_zone=time_zone,
            time_format_args_library=time_format_args_library,
            time_parsed_column=time_parsed_column,
            time_columns=time_columns,
            to_utc=to_utc
        )

    return data


def update_column_names(data, column_names, match_row_lengths=True,
                        get_mismatched_row_lengths=False):
    """Updates a data set's column names.

    Parameters
    ----------
    data : DataSet
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
    DataSet or namedtuple
        Data set with updated column names. If get_mismatched_row_lengths is true,
        return also a list of rows that did not pass the "match row lengths" test.
        The results will in the latter case be packed in a namedtuple.

    Examples
    --------
    >>> data = DataSet([
    ...     Row([
    ...         ('Label_1', 'some_value'),
    ...         ('Label_2', datetime(2016, 5, 2, 12, 34, 15, tzinfo=pytz.UTC)),
    ...         ('Label_3', 'some_other_value')])
    ... ])

    >>> new_column_names = ['New_Label_1', 'New_Label_2', 'New_Label_3']
    >>> new_column_names_result = update_column_names(data, column_names=new_column_names)
    >>> new_column_names_result
    DataSet([Row([('New_Label_1', 'some_value'), ('New_Label_2', datetime.datetime(2016, \
5, 2, 12, 34, 15, tzinfo=<UTC>)), ('New_Label_3', 'some_other_value')])])

    >>> data.append(Row([
    ...     ('Label_1', 'some_value'), ('Label_2', 'some_other_value')]))
    >>> new_column_names_result = update_column_names(
    ... data, column_names=new_column_names, get_mismatched_row_lengths=True)
    >>> new_column_names_result
    UpdatedColumnNamesResult(data_updated_column_names=DataSet([Row([('New_Label_1', \
'some_value'), ('New_Label_2', datetime.datetime(2016, 5, 2, 12, 34, 15, tzinfo=<UTC>)), \
('New_Label_3', 'some_other_value')])]), data_mismatched_row_lengths=DataSet([Row([\
('Label_1', 'some_value'), ('Label_2', 'some_other_value')])]))

    >>> data_new_column_names, data_mismatched_rows = new_column_names_result
    >>> data_new_column_names
    DataSet([Row([('New_Label_1', 'some_value'), ('New_Label_2', datetime.datetime(2016, \
5, 2, 12, 34, 15, tzinfo=<UTC>)), ('New_Label_3', 'some_other_value')])])
    >>> data_mismatched_rows
    DataSet([Row([('Label_1', 'some_value'), ('Label_2', 'some_other_value')])])

    """
    data_updated_column_names = DataSet([])
    data_mismatched_row_lengths = DataSet([])
    updated_column_names_result = namedtuple(
        'UpdatedColumnNamesResult',
        ['data_updated_column_names', 'data_mismatched_row_lengths']
    )

    for row in _data_generator(data):
        if match_row_lengths:
            if len(column_names) == len(row):
                data_updated_column_names.append(Row(
                    [(name, value) for name, value in zip(column_names, row.values())]
                ))
            else:
                if get_mismatched_row_lengths:
                    data_mismatched_row_lengths.append(row)
        else:
            data_updated_column_names.append(Row(
                [(name, value) for name, value in zip(column_names, row.values())]))
            if get_mismatched_row_lengths:
                data_mismatched_row_lengths.append(row)

    if match_row_lengths and get_mismatched_row_lengths:
        return updated_column_names_result(
            data_updated_column_names, data_mismatched_row_lengths)

    return data_updated_column_names
