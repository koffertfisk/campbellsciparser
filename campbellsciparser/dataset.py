#!/usr/bin/env
# -*- coding: utf-8 -*-
"""
dataset
-------
Data structure representing the rows and columns read from a Campbell Scientific CR-type
output file.

"""

from collections import OrderedDict


class DataSet(object):
    """Container holding a sequence of rows read from a data collection.

    Parameters
    ----------
    data : list of Row, optional
        Sequence representing the rows.

    Attributes
    ----------
    _rows : list of Row, optional
        Sequence representing the rows.

    Example
    -------
    >>> rows = [Row([('Label_1', '123'), ('Label_2', '456')]),
    ... Row([('Label_1', '123'), ('Label_2', '789')])]
    >>> dataset = DataSet(rows)
    >>> dataset
    DataSet([Row([('Label_1', '123'), ('Label_2', '456')]), Row([('Label_1', '123'), \
    ('Label_2', '789')])])
    >>> dataset.rows
    [Row([('Label_1', '123'), ('Label_2', '456')]), Row([('Label_1', '123'), ('Label_2', '789')])]
    >>> for row in dataset:
    ...     print(row)
    ...
    Row([('Label_1', '123'), ('Label_2', '456')])
    Row([('Label_1', '123'), ('Label_2', '789')])

    """
    def __init__(self, rows=None):
        if rows:
            DataSet._validate_rows(rows)
            self._rows = list(rows)
        else:
            self._rows = list()

    @staticmethod
    def _validate_rows(rows):
        """Ensures that all items in a data set is of type 'Row'.

        Parameters
        ----------
        rows : list of Row
            List of rows to validate.

        """
        for i in rows:
            DataSet._validate_row(i)

    @staticmethod
    def _validate_row(row):
        """Ensures that an item is of type 'Row'.

        Parameters
        ----------
        row : Row
            item to validate.

        Raises
        ------
        TypeError: If the given item is not of type 'Row'.

        """
        if not isinstance(row, Row):
            raise TypeError(
                "Only row objects allowed, got {type}".format(type=type(row)))

    def append(self, row):
        """Stores row at the end of the data set.

        Parameters
        ----------
        row : Row
            row to append.

        """
        DataSet._validate_row(row)
        self._rows.append(row)

    @property
    def rows(self):
        """Returns data set. """
        return self._rows

    def __getitem__(self, index):
        return self._rows[index]

    def __iter__(self):
        for row in self._rows:
            yield row

    def __len__(self):
        return len(self._rows)

    def __repr__(self):
        return '{name}({rows})'.format(name=self.__class__.__name__, rows=self._rows)

    def __setitem__(self, index, row):
        DataSet._validate_row(row)
        self._rows[index] = row

    def __str__(self):
        return self.__repr__()


class Row(OrderedDict):
    """Container representing rows' column names and values as key/value pairs.

    Examples
    --------
    >>> Row([('Label_1', '123'), ('Label_2', '456')])
    Row([('Label_1', '123'), ('Label_2', '456')])
    >>> for name, value in row.items():
    ...     print("Column name:", name, "Column value:", value)
    ...
    Column name: Label_1 Column value: 123
    Column name: Label_2 Column value: 456

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
