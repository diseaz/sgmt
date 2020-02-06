#!/usr/bin/python3
# -*- mode: python; coding: utf-8 -*-

import logging
import unittest

from sgmt import common
from sgmt import csvutil

from sgmt.cmd import graph_ops


_logger = logging.getLogger(__name__)


class TestCmdBfs(unittest.TestCase):
    def testOk(self):
        cmd = graph_ops.BfsCmd()
        cmd.set_nodes(['a'])
        cmd.flags = common.Struct(
            src='src',
            dst='dst',
        )
        rows = [
            csvutil.Row(src='a', dst='c'),
            csvutil.Row(src='a', dst='d'),
            csvutil.Row(src='a', dst='e'),
            csvutil.Row(src='b', dst='c'),
            csvutil.Row(src='b', dst='d'),
            csvutil.Row(src='b', dst='f'),
        ]
        expected = [
            csvutil.Row(src='a', dst='c'),
            csvutil.Row(src='a', dst='d'),
            csvutil.Row(src='a', dst='e'),
        ]
        self.assertEqual(expected, list(cmd.process(iter(rows))))

    def testBfs(self):
        cmd = graph_ops.BfsCmd()
        cmd.set_nodes(['a'])
        cmd.flags = common.Struct(
            src='src',
            dst='dst',
        )
        rows = [
            csvutil.Row(src='a', dst='c'),
            csvutil.Row(src='b', dst='c'),
            csvutil.Row(src='c', dst='d'),
            csvutil.Row(src='c', dst='e'),
            csvutil.Row(src='a', dst='f'),
            csvutil.Row(src='b', dst='g'),
        ]
        expected = [
            csvutil.Row(src='a', dst='c'),
            csvutil.Row(src='a', dst='f'),
            csvutil.Row(src='c', dst='d'),
            csvutil.Row(src='c', dst='e'),
        ]
        self.assertEqual(expected, sorted(cmd.process(iter(rows)), key=lambda r: (r.src, r.dst)))


if __name__ == '__main__':
    unittest.main()
