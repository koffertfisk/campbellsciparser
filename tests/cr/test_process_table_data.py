#!/usr/bin/env
# -*- coding: utf-8 -*-

import os

from campbellsciparser import cr
from campbellsciparser.dataset import Row

TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')


def test_process_table_rows_generator_empty():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_empty.dat')
    assert tuple(cr._process_table_rows(infile_path=file)) == ()


def test_process_table_rows_generator_exceeding_line_num():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_empty.dat')
    assert tuple(cr._process_table_rows(infile_path=file, first_line_num=1)) == ()


def test_process_table_rows_generator_three_rows():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_3_rows.dat')
    row_1 = Row([(0, '1')])
    row_2 = Row([(0, '1'), (1, '2')])
    row_3 = Row([(0, '1'), (1, '2'), (2, '3')])

    assert tuple(cr._process_table_rows(infile_path=file)) == (row_1, row_2, row_3)


def test_process_table_rows_generator_three_rows_slice():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_3_rows.dat')
    row_1 = Row([(0, '1')])
    row_2 = Row([(0, '1'), (1, '2')])
    row_3 = Row([(0, '1'), (1, '2'), (2, '3')])

    assert tuple(cr._process_table_rows(infile_path=file, first_line_num=1)) == (row_2, row_3, )
    assert tuple(cr._process_table_rows(
        infile_path=file, first_line_num=1, last_line_num=1)) == (row_2, )
    assert tuple(cr._process_table_rows(infile_path=file, last_line_num=2)) == (
        row_1, row_2, row_3)


def test_process_table_rows_generator_three_rows_header():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_3_rows.dat')
    header = ['Label_' + str(i) for i in range(3)]

    row_1 = Row([('Label_0', '1')])
    row_2 = Row([('Label_0', '1'), ('Label_1', '2')])
    row_3 = Row([('Label_0', '1'), ('Label_1', '2'), ('Label_2', '3')])

    assert tuple(cr._process_table_rows(
        infile_path=file,
        header=header,
        first_line_num=0)
    ) == (row_1, row_2, row_3)


def test_process_table_rows_generator_three_rows_indices():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_3_rows.dat')

    row_1 = Row([(0, '1')])
    row_2 = Row([(0, '1'), (1, '2')])
    row_3 = Row([(0, '1'), (1, '2'), (2, '3')])

    assert tuple(cr._process_table_rows(
        infile_path=file,
        first_line_num=0)
        ) == (row_1, row_2, row_3)


def test_process_table_rows_generator_three_rows_less_header():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_3_rows.dat')
    header = ['Label_0']

    row_1 = Row([('Label_0', '1')])
    row_2 = row_1
    row_3 = row_1

    assert tuple(cr._process_table_rows(
        infile_path=file, header=header, first_line_num=0)) == (row_1, row_2, row_3)


def test_process_table_rows_generator_three_rows_header_row():
    file = os.path.join(TEST_DATA_DIR, 'csv_testdata_3_rows_header.dat')

    row_1 = Row([('Label_0', '1')])
    row_2 = Row([('Label_0', '1'), ('Label_1', '2')])
    row_3 = Row([('Label_0', '1'), ('Label_1', '2'), ('Label_2', '3')])

    assert tuple(cr._process_table_rows(
        infile_path=file, header_row=0, first_line_num=0)) == (row_1, row_2, row_3)