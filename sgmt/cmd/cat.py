#!/usr/bin/python3
# -*- mode: python; coding: utf-8 -*-

"""Command description."""

import collections
import itertools
import logging

from dsapy import app

from sgmt import common
from sgmt import csvutil
from sgmt import nodeutil


_logger = logging.getLogger(__name__)


class CatCmd(csvutil.Filter, app.Command):
    '''Concatenate inputs to output.'''
    name = 'cat'

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)

        parser.add_argument(
            '--debug',
            action='store_true',
            help='Debug output',
        )

    def process(self, rows):
        if self.flags.debug:
            for row in rows:
                _logger.debug('Row: %r', row)
                yield row
        else:
            yield from rows


class ExtractCmd(csvutil.Filter, app.Command):
    '''Extract columns with rename.'''
    name = 'extract'

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)

        parser.add_argument(
            '--column',
            action='append',
            default=[],
            help='DST=SRC column mapping',
        )

    @common.lazy
    def column_pairs(self):
        return [
            tuple(line.split('='))
            for line in self.flags.column
        ]

    @common.lazy
    def get_out_fieldnames(self):
        return [dst for dst, _ in self.column_pairs()]

    def process(self, rows):
        known = set()
        for r in rows:
            q = csvutil.Row()
            k = []
            for dst, src in self.column_pairs():
                q[dst] = r[src]
                k.append(r[src])
            k = tuple(k)
            if k in known:
                continue
            known.add(k)
            yield q


class FilterCmd(nodeutil.PatternsMixin, csvutil.Filter, app.Command):
    '''Filter by column patterns.'''
    name = 'filter'

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)

        parser.add_argument(
            '--column',
            help='Column name to apply filter to',
        )

    def process(self, rows):
        if not self.flags.column:
            matches = lambda r: True
        else:
            def matches(r):
                return self.nodes_match_func()(r[self.flags.column])

        return (row for row in rows if matches(row))


class SetCmd(csvutil.Filter, app.Command):
    '''Set column value.'''
    name = 'set'

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)

        parser.add_argument(
            '--data',
            action='append',
            default=[],
            help='column=value to set in output. column will be added as needed',
        )

    @common.lazy
    def get_update_dict(self):
        result = collections.OrderedDict()
        for s in self.flags.data:
            column, value = s.split('=')
            result[column] = value
        return result

    @common.lazy
    def get_out_fieldnames(self):
        fieldnames = list(super().get_out_fieldnames())
        known = set(fieldnames)
        for n in self.get_update_dict():
            if n in known:
                continue
            fieldnames.append(n)
            known.add(n)
        return fieldnames

    def process(self, rows):
        update_dict = self.get_update_dict()
        for r in rows:
            r.update(update_dict)
            yield r
