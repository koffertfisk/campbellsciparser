#!/usr/bin/env
# -*- coding: utf-8 -*-

"""API for parsing and exporting comma-separated data collected by Campbell datalogger. """

import csv
import os

from collections import defaultdict


class CR10X(object):
    """Parses and exports data following the Campbell CR10X format. """
    @classmethod
    def get_data(cls, file_path, line_num=0, fix_floats=True):
        """Collects data from a given input file.

        Args:
            file_path (string): Absolute path to file.
            line_num (int): Line number to start at. NOTE: Zero-based numbering.
            fix_floats (bool): Correct leading zeros floating points since the CR10X does not output leading zeros.

        Returns:
            All data found from the given line number onwards.
        """
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            if fix_floats:
                replacements = {'.': '0.', '-.': '-0.'}    # Patterns to look for
                data = []
                for row in reader:
                    if line_num <= (reader.line_num - 1):   # Correct reader for zero-based numbering
                        for i, value in enumerate(row):
                            for source, replacement in replacements.items():
                                if value.startswith(source):
                                    row[i] = value.replace(source, replacement)
                        data.append(row)

            else:
                data = [row for row in reader if line_num <= (reader.line_num - 1)]

        return data

    @classmethod
    def get_array_ids_data(cls, file_path, line_num=0, fix_floats=True, array_ids_table=None):
        """Collects data separated by array id, i.e. each rows' first element.

        Args:
            file_path (string): Absolute path to file.
            line_num (int): Line number to start at. NOTE: Zero-based numbering.
            fix_floats (bool): Correct leading zeros floating points since the CR10X does not output leading zeros.
            array_ids_table (dict): Lookup table for translating array ids.

        Returns:
            All data found from the given line number onwards, separated by array id.
        """
        array_ids_dict = defaultdict(list)

        data = cls.get_data(file_path, line_num, fix_floats)

        for row in data:
            array_id = row[0]
            if array_ids_table:
                array_id = array_ids_table.get(array_id, array_id)

            array_ids_dict[array_id].append(row)

        return array_ids_dict

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
            data_filtered = data
        else:
            for array_id, array_id_data in data.items():
                if array_id in array_ids:
                    data_filtered[array_id] = array_id_data

        return data_filtered

    @classmethod
    def export_to_csv(cls, data, file_path, header=None, match_num_of_columns=True, mode='a'):
        """Export data as a comma-separated values file.

        Args:
            data (list): Data to export.
            file_path (string): Absolute path to output file.
            header (list): File header to write at the top of the output file.
            match_num_of_columns: Match number of header columns to number of the rows' columns.
            mode (string): File open mode.

        """
        export_header = header

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
            for row in data:
                if header and match_num_of_columns:
                    if len(header) != len(row):
                        continue
                f_out.write((",".join(row)) + "\n")

    @classmethod
    def export_array_ids_to_csv(cls, data, array_ids_info, match_num_of_columns=True, mode='a'):
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
            cls.export_to_csv(array_id_data, file_path, header, match_num_of_columns=match_num_of_columns, mode=mode)
