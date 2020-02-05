#!/usr/bin/python3
# -*- mode: python; coding: utf-8 -*-

import logging
import unittest

from sgmt import common
from sgmt import csvutil

from sgmt.cmd import cat


_logger = logging.getLogger(__name__)


class TestCmdCat(unittest.TestCase):
    def testOk(self):
        cmd = cat.CatCmd()
        rows = [
            csvutil.Row(src='src1', dst='dst1'),
            csvutil.Row(src='src2', dst='dst2'),
            csvutil.Row(src='src3', dst='dst3'),
        ]
        expected = rows
        self.assertEqual(expected, list(cmd.process(iter(rows))))


class TestCmdExtract(unittest.TestCase):
    def testSingleColumn(self):
        cmd = cat.ExtractCmd()
        cmd.flags = common.Struct(
            column=['result=src'],
        )
        rows = [
            csvutil.Row(src='src1', dst='dst1'),
            csvutil.Row(src='src2', dst='dst2'),
            csvutil.Row(src='src3', dst='dst3'),
        ]
        expected = [
            csvutil.Row(result='src1'),
            csvutil.Row(result='src2'),
            csvutil.Row(result='src3'),
        ]
        self.assertEqual(expected, list(cmd.process(iter(rows))))


if __name__ == '__main__':
    unittest.main()
