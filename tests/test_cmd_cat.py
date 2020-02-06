#!/usr/bin/python3
# -*- mode: python; coding: utf-8 -*-

import itertools
import logging
import unittest

from sgmt import common
from sgmt import csvutil

from sgmt.cmd import cat


_logger = logging.getLogger(__name__)


class TestCmdCat(unittest.TestCase):
    def testOk(self):
        cmd = cat.CatCmd()
        cmd.flags = common.Struct(
            debug=False,
        )
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


class TestCmdFilter(unittest.TestCase):
    def testOk(self):
        cmd = cat.FilterCmd()
        cmd.flags = common.Struct(
            column='src',
        )
        cmd.set_nodes(['^a^', 'b^', '^c', 'd'])
        prefix = ['y.', 'y.x']
        word = ['a', 'b', 'c', 'd']
        suffix = ['.z', 'x.z']

        rows = [
            csvutil.Row(src=p+w+s) for w, p, s in itertools.product(word, prefix, suffix)
        ]
        expected = [
            csvutil.Row(src='y.a.z'),
            csvutil.Row(src='y.b.z'),
            csvutil.Row(src='y.xb.z'),
            csvutil.Row(src='y.c.z'),
            csvutil.Row(src='y.cx.z'),
            csvutil.Row(src='y.d.z'),
            csvutil.Row(src='y.dx.z'),
            csvutil.Row(src='y.xd.z'),
            csvutil.Row(src='y.xdx.z'),
        ]
        self.assertEqual(expected, list(cmd.process(iter(rows))))


if __name__ == '__main__':
    unittest.main()
