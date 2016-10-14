import os.path
import shutil
import unittest

TEST_DEVICES_DIR = os.path.dirname(os.path.abspath(__file__))


class ReadDataTestCase(unittest.TestCase):

    def assertDataLengthEqual(self, data, data_length):
        self.assertEqual(len(data), data_length)

    def assertDataLengthNotEqual(self, data, data_length):
        self.assertNotEqual(len(data), data_length)

    def assertTwoDataSetsLengthEqual(self, data_1, data_2):
        self.assertEqual(len(data_1), len(data_2))

    def assertTwoDataSetsContentEqual(self, data_1, data_2):
        data_1_list = [list(row.values()) for row in data_1]
        data_2_list = [list(row.values()) for row in data_2]

        data_1_diff_elements = [row for row in data_1_list if row not in data_2_list]
        data_2_diff_elements = [row for row in data_2_list if row not in data_1_list]

        self.assertDataLengthEqual(data_1_diff_elements, 0)
        self.assertDataLengthEqual(data_2_diff_elements, 0)


class ExportFileTestCase(unittest.TestCase):

    def assertExportedFileExists(self, file_path):
        self.assertTrue(os.path.exists(file_path))

    @staticmethod
    def delete_output(file_path):
        dirname = os.path.dirname(file_path)
        shutil.rmtree(dirname)
