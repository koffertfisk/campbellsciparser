from campbellsciparser import cr


def test_data_generator():
    test_list = [1, 2, 3]
    assert tuple(cr._data_generator(test_list)) == (1, 2, 3)
