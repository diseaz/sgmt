#!/usr/bin/python3
# -*- mode: python; coding: utf-8 -*-

import csv
import re

from sgmt import common
from sgmt import csvutil


ANCHOR = '^'


class PatternsMixin(csvutil.Base):
    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)

        parser.add_argument(
            '--nodes',
            action='append',
            help='Nodes file',
        )
        parser.add_argument(
            '--nodes-dialect',
            metavar='DIALECT',
            choices=csv.list_dialects(),
            default='default',
            help='CSV dialect for nodes file',
        )
        parser.add_argument(
            '--node-column',
            metavar='NAME',
            default='node',
            help='Column name for the node name',
        )

    @common.lazy
    def get_nodes(self):
        result = set()
        nodes_filenames = self.flags.nodes or ['-']
        for filename in nodes_filenames:
            with self.csv_file_reader(filename=filename, dialect=self.flags.nodes_dialect) as nodes_reader:
                for r in nodes_reader:
                    result.add(r[self.flags.node_column])
        return result
    set_nodes = get_nodes.set

    @common.lazy
    def nodes_re(self):
        return re_contains(self.get_nodes())

    @common.lazy
    def nodes_match_func(self):
        return self.nodes_re().search


def re_contains(strings):
    def boundary(a):
        if a:
            return '\\b'
        return ''

    def make_re(s):
        s, af = strip_prefix(ANCHOR, s)
        s, ab = strip_suffix(ANCHOR, s)
        return boundary(af) + re.escape(s) + boundary(ab)

    return re.compile('(?:' +
            '|'.join(make_re(s) for s in strings) +
            ')')


def strip_suffix(suffix, s):
    if s.endswith(suffix):
        return s[:len(s)-len(suffix)], True
    return s, False


def strip_prefix(prefix, s):
    if s.startswith(prefix):
        return s[len(prefix):], True
    return s, False
