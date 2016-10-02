#!/usr/bin/env
# -*- coding: utf-8 -*-

"""Module for parsing and exporting comma-separated data collected by Campbell Scientific data loggers. """

import csv
import os

from collections import defaultdict, namedtuple
from datetime import datetime

from campbellsciparser import timeparser


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


class CR10X(object):
    """Parses and exports data files collected by Campbell Scientific CR10X data loggers. """

    @staticmethod
    def _data_generator(data):
        """Turns data set (list) into a generator.

        Args:
            data (list): Data set (list of rows) to process.

        Returns:
            Each row, one at a time.

        """
        for row in data:
            yield row

    @staticmethod
    def _datetime_to_str_no_time_zone(dt):
        """Produces a string representation from a datetime object, omitting time zone information.

        Args:
            dt (datetime): Datetime to process.

        Returns:
            String representation of the given datetime excluding the time zone.

        """
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _find_first_time_column_index(headers, time_columns):
        """
        Search for the first time representation column within the headers. Used for inserting the parsed time
        column on the found position.

        Args:
            headers (list): List of data file headers or indices.
            time_columns (list): List of columns, or indices representing time values.

        Returns:
            The index of which the first time column is expected.

        Raises:
            TimeColumnValueError: If the first time column is not present in the headers.

        """
        for i, name in enumerate(headers):
            if name == time_columns[0]:
                return i
        else:
            raise timeparser.TimeColumnValueError("First time column '{0}' not found in headers!".format(time_columns[0]))

    @staticmethod
    def _process_rows(infile_path, line_num=0, fix_floats=True):
        """Helper method for _read_data.

        Args:
            infile_path (str): Input file's absolute path.
            line_num (int): Line number to start at. NOTE: Zero-based numbering.
            fix_floats (bool): Correct leading zeros floating points since the CR10X does not output leading zeros.

        Returns:
            Each processed row, one at a time.

        """
        with open(infile_path, 'r') as f:
            rows = csv.reader(f)
            if fix_floats:
                replacements = {'.': '0.', '-.': '-0.'}    # Patterns to look for
                for row in rows:
                    if line_num <= (rows.line_num - 1):   # Correct reader for zero-based numbering
                        for i, value in enumerate(row):
                            for source, replacement in replacements.items():
                                if value.startswith(source):
                                    row[i] = value.replace(source, replacement)
                        yield row
            else:
                for row in rows:
                    yield row

    @staticmethod
    def _read_data(infile_path, line_num=0, fix_floats=True):
        """Produces a generator object of data read from given file input starting from a given line number.

        Args:
            infile_path (str): Input file's absolute path.
            line_num (int): Line number to start at. NOTE: Zero-based numbering.
            fix_floats (bool): Correct leading zeros floating points since the CR10X does not output leading zeros.

        Returns:
            Each processed row, one at a time.

        """
        for row in CR10X._process_rows(infile_path=infile_path, line_num=line_num, fix_floats=fix_floats):
            yield row

    @staticmethod
    def _row_str_conversion(row, include_time_zone=False):
        """Produces a generator object for the values in a row, converted to strings.

        Args:
            row (list): List of values read from a row.
            include_time_zone (bool): Include time zone in string converted datetime objects.

        Returns:
            String representation of each value.

        """
        for i, value in enumerate(row):
            if not include_time_zone and isinstance(value, datetime):
                row[i] = CR10X._datetime_to_str_no_time_zone(value)
            else:
                row[i] = str(value)

        return row

    @staticmethod
    def convert_time(data, headers=None, time_parsed_column="Timestamp", time_columns=None, data_time_zone='UTC',
                     to_utc=False):
        """Converts specific time columns from a data set into a single timestamp column.

        Args:
             data (list): Data set to convert.
             headers (list): Column headers to match time columns against. If not given, use each rows' index instead.
             time_parsed_column (str): Converted time column name.
             time_columns (list): Column(s) to use for time conversion.
             data_time_zone (str): Pytz time zone of the recorded data.
             to_utc (bool): Convert time to UTC.

        Returns:
            The updated headers and converted data set.

        Raises:
            TimeColumnValueError: If not at least one time column is given.

        """
        if not time_columns:
            raise timeparser.TimeColumnValueError("At least one time column is required!")

        time_parser = timeparser.CR10XTimeParser(time_zone=data_time_zone)
        headers_pre_conversion = headers
        headers_post_conversion = None
        data_post_conversion = namedtuple('ConversionResults', ['headers_updated', 'data_converted'])
        data_converted = []

        if headers_pre_conversion:
            first_time_column_index = CR10X._find_first_time_column_index(headers_pre_conversion, time_columns)

        for row in CR10X._data_generator(data):
            if not headers_pre_conversion:
                headers_pre_conversion = [i for i, value in enumerate(row)]   # Construct headers from the rows' length
                first_time_column_index = CR10X._find_first_time_column_index(headers_pre_conversion, time_columns)
            if not headers_post_conversion:
                headers_post_conversion = [name for name in headers_pre_conversion if name not in time_columns]
                headers_post_conversion.insert(first_time_column_index, time_parsed_column)

            row_time_column_values = [column for header, column in zip(headers_pre_conversion, row) if header in time_columns]
            row_converted = [value for name, value in zip(headers_pre_conversion, row) if name not in time_columns]
            row_time_converted = time_parser.parse_time(*row_time_column_values, to_utc=to_utc)
            row_converted.insert(first_time_column_index, row_time_converted)

            data_converted.append(row_converted)

        return data_post_conversion(headers_post_conversion, data_converted)

    @staticmethod
    def export_to_csv(data, outfile_path, headers=None, match_num_of_columns=True, output_mismatched_columns=False,
                      mode='a', include_time_zone=False):
        """Export data as a comma-separated values file.

        Args:
            data (list): Data set to export.
            outfile_path (str): Output file's absolute path.
            headers (list): File headers to write at the top of the output file.
            match_num_of_columns (bool): Match number of header columns to number of the rows' columns.
            output_mismatched_columns (bool): Output mismatched columns (with respect to header length).
            mode (str): Output file open mode, defaults to append. See Python Docs for other mode options.
            include_time_zone (bool): Include time zone in string converted datetime objects.

        Raises:
            DataTypeError: If data is not a list.

        """
        headers_to_export = headers
        mismatched_columns = []     # If row length matching is enabled, all mismatched columns will be stored in this list.

        if not isinstance(data, list):
            raise DataTypeError("Data must be of type list, got {0}".format(type(data)))

        os.makedirs(os.path.dirname(outfile_path), exist_ok=True)

        if os.path.exists(outfile_path) and headers:
            with open(outfile_path, 'r') as temp_f:
                f_list = [line.strip() for line in temp_f]
            if len(f_list) > 0:
                headers_to_export = None

        with open(outfile_path, mode) as f_out:
            if headers_to_export:
                f_out.write(",".join(headers_to_export) + "\n")
            for row in CR10X._data_generator(data):
                if headers and match_num_of_columns:
                    if len(headers) != len(row):
                        if output_mismatched_columns:
                            mismatched_columns.append(row)
                        continue
                f_out.write((",".join(CR10X._row_str_conversion(row, include_time_zone)) + "\n"))

        if mismatched_columns:
            output_dir, file = os.path.split(outfile_path)
            file_name, file_extension = os.path.splitext(file)
            mismatched_columns_file = os.path.join(output_dir, file_name + " Mismatched columns" + file_extension)

            with open(mismatched_columns_file, mode) as f_out:
                for row in CR10X._data_generator(mismatched_columns):
                    f_out.write((",".join(CR10X._row_str_conversion(row, include_time_zone)) + "\n"))

    @staticmethod
    def export_array_ids_to_csv(data, array_ids_info, match_num_of_columns=True, output_mismatched_columns=False,
                                mode='a', include_time_zone=False):
        """Export array ids data as separate comma-separated values files.

        Args:
            data (dict(list)): Array id separated data.
            array_ids_info (dict(dict)): Array ids to export. Contains output file paths and file headers.
            match_num_of_columns (bool): Match number of header columns to number of the rows' columns.
            output_mismatched_columns (bool): Output mismatched columns (with respect to header length).
            mode (str): Output file open mode, defaults to append. See Python Docs for other mode options.
            include_time_zone (bool): Include time zone in string converted datetime objects.

        Raises:
            ArrayIdsInfoError: If not at least one array id in array_ids_info is found.
            ArrayIdsExportInfoError: If no information for a certain array id is found.
            ArrayIdsFilePathError: If no output file path for a certain array id is found.

        """
        if len(array_ids_info) < 1:
            raise ArrayIdsInfoError("At least one array id must be given!")

        data_filtered = CR10X.filter_data_by_array_ids(*array_ids_info.keys(), data=data)

        for array_id, array_id_data in data_filtered.items():
            export_info = array_ids_info.get(array_id)
            if not export_info:
                raise ArrayIdsExportInfoError("No information was found for array id {0}".format(array_id))
            file_path = export_info.get('file_path')
            if not file_path:
                raise ArrayIdsFilePathError("Not file path was found for array id {0}".format(array_id))
            headers = export_info.get('headers')
            CR10X.export_to_csv(array_id_data, file_path, headers, match_num_of_columns=match_num_of_columns,
                                output_mismatched_columns=output_mismatched_columns, mode=mode,
                                include_time_zone=include_time_zone)

    @staticmethod
    def filter_data_by_array_ids(*array_ids, data):
        """Filter data set by array ids.

        Args:
            *array_ids: Array ids to filter by. If no arguments are given, return unfiltered data set.
            data (dict(list), list): Array id separated or mixed data.

        Returns:
            Filtered data set if array ids are given, unfiltered otherwise. If a mixed data set is given, return
            unfiltered data set split by its array ids.

        """
        data_filtered = defaultdict(list)

        if isinstance(data, dict):
            if not array_ids:
                return data     # Return unfiltered data set
            for array_id, array_id_data in data.items():
                if array_id in array_ids:
                    data_filtered[array_id] = array_id_data
        elif isinstance(data, list):
            for row in CR10X._data_generator(data):
                if not array_ids:
                    data_filtered[row[0]].append(row)   # Append to unfiltered data set, but split by array ids.
                else:
                    if row[0] in array_ids:
                        data_filtered[row[0]].append(row)
        else:
            raise TypeError("Data collection of type {0} not supported. Valid collection types are dict and list.")

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
        data_mixed = CR10X.read_mixed_data(infile_path=infile_path, line_num=line_num, fix_floats=fix_floats)
        data_by_array_ids = CR10X.filter_data_by_array_ids(*array_ids, data=data_mixed)
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
        return [row for row in CR10X._read_data(infile_path=infile_path, line_num=line_num, fix_floats=fix_floats)]
