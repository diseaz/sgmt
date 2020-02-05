#!/usr/bin/python3
# -*- mode: python; coding: utf-8 -*-

"""Command description."""

import itertools
import logging

from dsapy import app

from sgmt import common
from sgmt import csvutil


_logger = logging.getLogger(__name__)


class SetOpMixin(object):
    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)

        parser.add_argument(
            '--key',
            help='Comma-separated key column names',
        )

    @common.lazy
    def key_columns(self):
        key = self.flags.key
        if key is None:
            return self.get_in_fieldnames()
        return tuple(key.split(','))

    def row_key(self, row):
        return tuple(
            row[k]
            for k in self.key_columns()
        )


class IntersectionCmd(SetOpMixin, csvutil.Filter, app.Command):
    '''Intersect sets.'''
    name = 'int'

    def process(self, rows):
        result = {
            self.row_key(r): r
            for r in itertools.takewhile(lambda r: r['@fn'] == 0, rows)
        }
        for _, grs in itertools.groupby(rows, key=lambda r: r['@fn']):
            result, old = {}, result
            for r in grs:
                k = self.row_key(r)
                if k in old:
                    result[k] = old[k]
        return result.values()


class UnionCmd(SetOpMixin, csvutil.Filter, app.Command):
    '''Union sets.'''
    name = 'uni'

    def process(self, rows):
        known = set()
        for r in rows:
            k = self.row_key(r)
            if k in known:
                continue
            known.add(k)
            yield r


class SubtractCmd(SetOpMixin, csvutil.Filter, app.Command):
    '''Subtract subsequent sets from the first one.'''
    name = 'sub'

    def process(self, rows):
        result = {
            self.row_key(r): r
            for r in itertools.takewhile(lambda r: r['@fn'] == 0, rows)
        }
        for _, grs in itertools.groupby(rows, key=lambda r: r['@fn']):
            for r in grs:
                k = self.row_key(r)
                result.pop(k, None)
        return result.values()


class DiffCmd(SetOpMixin, csvutil.Filter, app.Command):
    '''Keep records contained in only one set.'''
    name = 'diff'

    def process(self, rows):
        result = {}
        conflict = set()
        for _, grs in itertools.groupby(rows, key=lambda r: r['@fn']):
            for r in grs:
                k = self.row_key(r)
                if k in conflict:
                    continue
                if k in result:
                    conflict.add(k)
                    result.pop(k, None)
                    continue
                result[k] = r
        return result.values()
