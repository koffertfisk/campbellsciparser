#!/usr/bin/env
# -*- coding: utf-8 -*-

"""Module for parsing and exporting comma-separated data collected by Campbell Scientific data loggers. """

import csv

from collections import defaultdict, namedtuple, OrderedDict
from datetime import datetime

from campbellsciparser import timeparser


class CR10X(object):
    """Parses and exports data files collected by Campbell Scientific CR10X data loggers. """

    @classmethod
    def _process_rows(cls, file_path, line_num=0, fix_floats=True):
        with open(file_path, 'r') as f:
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

    @classmethod
    def _read_data(cls, file_path, line_num=0, fix_floats=True):
        for row in cls._process_rows(file_path=file_path, line_num=line_num, fix_floats=fix_floats):
            yield row

    @classmethod
    def read_mixed_data(cls, file_path, line_num=0, fix_floats=True):
        return [row for row in cls._read_data(file_path=file_path, line_num=line_num, fix_floats=fix_floats)]

    @classmethod
    def data_generator(cls, data):
        for row in data:
            yield row

    @classmethod
    def split_mixed_data(cls, data, array_ids_table=None):
        data_by_array_ids = defaultdict(list)

        for row in cls.data_generator(data):
            array_id = row[0]
            if array_ids_table:
                array_id = array_ids_table.get(array_id, array_id)
            data_by_array_ids[array_id].append(row)

        return data_by_array_ids

    @classmethod
    def read_array_ids_data(cls, file_path, line_num=0, fix_floats=True, array_ids_table=None):
        """Collects data separated by array id, i.e. each rows' first element.

        Args:
            file_path (string): Absolute path to file.
            line_num (int): Line number to start at. NOTE: Zero-based numbering.
            fix_floats (bool): Correct leading zeros floating points since the CR10X does not output leading zeros.
            array_ids_table (dict): Lookup table for translating array ids.

        Returns:
            All data found from the given line number onwards, separated by array id.
        """

        data_mixed = cls.read_mixed_data(file_path=file_path, line_num=line_num, fix_floats=fix_floats)
        data_by_array_ids = cls.split_mixed_data(data=data_mixed, array_ids_table=array_ids_table)

        return data_by_array_ids

    @classmethod
    def filter_array_ids(cls, data, *array_ids):
        """Filter dataset by given array ids.

        Args:
            data (dict(list)): Array id separated data.
            *array_ids (string): Array ids to filter.

        Returns:
            Filtered dataset.
        """

        data_filtered = defaultdict(list)

        if not array_ids:
            return data
        else:
            if isinstance(data, dict):
                for array_id, array_id_data in data.items():
                    if array_id in array_ids:
                        data_filtered[array_id] = array_id_data
            elif isinstance(data, list):
                for row in cls.data_generator(data):
                    if row[0] in array_ids:
                        data_filtered[row[0]].append(row)
            else:
                raise TypeError("Data collection of type {0} not supported. Valid collection types are dict and list.")

        return data_filtered

    @classmethod
    def _datetime_to_str_no_time_zone(cls, dt):
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    @classmethod
    def _row_str_conversion_generator(cls, row, include_time_zone=False):
        for value in row:
            if not include_time_zone and isinstance(value, datetime):
                value = cls._datetime_to_str_no_time_zone(value)
            else:
                value = str(value)
            yield value

    @classmethod
    def export_to_csv(cls, data, file_path, header=None, match_num_of_columns=True, output_mismatched_columns=False,
                      mode='a', include_time_zone=False):
        """Export data as a comma-separated values file.

        Args:
            data (list): Data to export.
            file_path (string): Absolute path to output file.
            header (list): File header to write at the top of the output file.
            match_num_of_columns (bool): Match number of header columns to number of the rows' columns.
            output_mismatched_columns (bool): Output mismatched columns (with respect to header length).
            mode (string): File open mode, defaults to append.

        """
        export_header = header
        mismatched_columns = []

        if not isinstance(data, list):
            raise TypeError("Data must be of type list, got {0}".format(type(data)))

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        if os.path.exists(file_path) and header:
            with open(file_path, 'r') as temp_f:
                f_list = [line.strip() for line in temp_f]
            if len(f_list) > 0:
                export_header = None

        with open(file_path, mode) as f_out:
            if export_header:
                f_out.write(",".join(export_header) + "\n")
            for row in cls.data_generator(data):
                if header and match_num_of_columns:
                    if len(header) != len(row):
                        if output_mismatched_columns:
                            mismatched_columns.append(row)
                        continue
                f_out.write((",".join(cls._row_str_conversion_generator(row, include_time_zone)) + "\n"))

        if mismatched_columns:
            output_dir, file = os.path.split(file_path)
            file_name, file_extension = os.path.splitext(file)
            mismatched_columns_file = os.path.join(output_dir, file_name + " Mismatched columns" + file_extension)

            with open(mismatched_columns_file, mode) as f_out:
                for row in cls.data_generator(mismatched_columns):
                    f_out.write((",".join(cls._row_str_conversion_generator(row, include_time_zone)) + "\n"))

    @classmethod
    def export_array_ids_to_csv(cls, data, array_ids_info, match_num_of_columns=True, output_mismatched_columns=False, mode='a'):
        """Export array ids data as separate comma-separated values files.

        Args:
            data (dict(list)): Array id separated data.
            array_ids_info (dict(dict)): Array ids to export. Contains output file paths and file headers.
            match_num_of_columns: Match number of header columns to number of the rows' columns.
            mode (string): File open mode.

        Returns:

        """
        if len(array_ids_info) < 1:
            raise ValueError("Keyword argument export_array_ids must hold at least one item!")

        data_filtered = cls.filter_array_ids(data, *array_ids_info.keys())

        for array_id, array_id_data in data_filtered.items():
            export_info = array_ids_info.get(array_id)
            if not export_info:
                raise ValueError("No information was found for array id {0}".format(array_id))
            file_path = export_info.get('file_path')
            if not file_path:
                raise ValueError("Not file path was found for array id {0}".format(array_id))
            header = export_info.get('header')
            cls.export_to_csv(array_id_data, file_path, header, match_num_of_columns=match_num_of_columns,
                              output_mismatched_columns=output_mismatched_columns, mode=mode)

    @classmethod
    def _find_first_time_column_index(cls, headers, time_columns):
        for i, name in enumerate(headers):
            if name == time_columns[0]:
                return i
        else:
            raise timeparser.TimeColumnValueError("First time column '{0}' not found in headers!".format(time_columns[0]))

    @classmethod
    def convert_time(cls, data, headers=None, time_parsed_column="Timestamp", time_columns=None, data_time_zone='UTC', to_utc=False):
        """Converts specific time columns from a data set to one timestamp column.

        Args:
             data (list): Data to convert.
             headers (list): Column headers to match time columns against. If not given, use each row's index.
             time_parsed_column (string): Name of converted time column.
             time_columns (list): Column(s) to use for time conversion.
             data_time_zone (string): Pytz time zone of the recorded data.
             to_utc (bool): Convert time to UTC.
        Returns:
            The updated headers and converted data set.
        """
        if not time_columns:
            raise timeparser.TimeColumnValueError("At least one time column is required!")

        time_parser = timeparser.CR10XTimeParser(time_zone=data_time_zone)
        headers_pre_conversion = headers
        headers_post_conversion = None
        data_post_conversion = namedtuple('ConversionResults', ['headers_updated', 'data_converted'])
        data_converted = []

        if headers_pre_conversion:
            first_time_column_index = cls._find_first_time_column_index(headers_pre_conversion, time_columns)

        for row in cls.data_generator(data):
            if not headers_pre_conversion:
                headers_pre_conversion = [i for i, value in enumerate(row)]   # Construct headers from the rows' length
                first_time_column_index = cls._find_first_time_column_index(headers_pre_conversion, time_columns)
            if not headers_post_conversion:
                headers_post_conversion = [name for name in headers_pre_conversion if name not in time_columns]
                headers_post_conversion.insert(first_time_column_index, time_parsed_column)

            row_time_column_values = [column for header, column in zip(headers_pre_conversion, row) if header in time_columns]
            row_converted = [value for name, value in zip(headers_pre_conversion, row) if name not in time_columns]
            row_time_converted = time_parser.parse_time(*row_time_column_values, to_utc=to_utc)
            row_converted.insert(first_time_column_index, row_time_converted)

            data_converted.append(row_converted)

        return data_post_conversion(headers_post_conversion, data_converted)
