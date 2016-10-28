# !/usr/bin/env
# -*- coding: utf-8 -*-

import pytest

from campbellsciparser.dataset import DataSet
from campbellsciparser.dataset import Row


def test_dataset_init():
    dataset = DataSet()
    assert dataset.rows == []

    test_list = [Row([('a', 1), ('b', 2), ('c', 3)])]
    dataset = DataSet(test_list)

    for row, ref_row in zip(dataset.rows, test_list):
        assert row == ref_row


def test_dataset_append():
    dataset = DataSet([])
    dataset.append(Row([('a', 1)]))
    assert dataset.rows == [Row([('a', 1)])]

    with pytest.raises(TypeError):
        dataset.append(1)


def test_dataset_validate_row():
    with pytest.raises(TypeError):
        DataSet._validate_row(1)


def test_dataset_validate_rows():
    with pytest.raises(TypeError):
        DataSet._validate_rows([1])


def test_row_init():

    assert Row([]) == Row()
    assert Row([('a', 1), ('b', 2), ('c', 3)]) == Row([('a', 1), ('b', 2), ('c', 3)])
