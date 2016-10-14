#!/usr/bin/env
# -*- coding: utf-8 -*-

from tests.devices.common_all import ReadDataTestCase


class CR10ParserTestCase(ReadDataTestCase):
    def assertFloatingPointsFixed(self, data_no_fp_fix, data_fp_fixed, replacements_lib):
        unprocessed = []
        for row_no_fp_fix, row_fp_fix in zip(data_no_fp_fix, data_fp_fixed):
            for value_no_fix, value_fix in zip(list(row_no_fp_fix.values()),
                                               list(row_fp_fix.values())):
                for source, replacement in replacements_lib.items():
                    if value_no_fix.startswith(source) and not value_fix.startswith(
                            replacement):
                        unprocessed.append(value_no_fix)

        self.assertDataLengthEqual(unprocessed, 0)
