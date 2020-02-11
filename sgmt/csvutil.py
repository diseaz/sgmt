#!/usr/bin/python3
# -*- mode: python; coding: utf-8 -*-

"""Utilities to deal with CSV."""

import contextlib
import csv
import itertools
import logging


from . import common


_logger = logging.getLogger(__name__)


ANCHOR = '^'


class Row(common.Struct):
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)

        for k, v in list(self.items()):
            if v is None or v == '':
                del self[k]

    def __getitem__(self, key):
        try:
            return super().__getitem__(key)
        except (AttributeError, KeyError, IndexError):
            return ''
    __getattr__ = __getitem__

    def __setitem__(self, key, value):
        if value is None or value == '':
            self.pop(key, None)
            return
        super().__setitem__(key, value)
    __setattr__ = __setitem__


class Reader(csv.DictReader):
    def __init__(self, *args, **kw):
        preprocess = kw.pop('preprocess', None)
        fn = kw.pop('fn', None)
        rn = kw.pop('rn', None)
        super().__init__(*args, **kw)

        if preprocess is None:
            preprocess = lambda r: r
        self.preprocess = preprocess

        self.fn = fn
        if fn is None:
            self.frn = None
        else:
            self.frn = itertools.count()
        self.rn = None
        if rn is None:
            if fn is not None:
                self.rn = itertools.count()
        else:
            self.rn = rn

    def __next__(self):
        r = Row(super().__next__())
        if self.fn is not None:
            r['@fn'] = self.fn
            r['@frn'] = next(self.frn)
        if self.rn is not None:
            r['@rn'] = next(self.rn)
        return self.preprocess(r)


class Writer(csv.DictWriter):
    def __init__(self, *args, **kw):
        postprocess = kw.pop('postprocess', None)
        super().__init__(*args, **kw)
        if postprocess is None:
            postprocess = lambda r: r
        self.postprocess = postprocess

    def writerow(self, row):
        return super().writerow(self.postprocess(row))

    def writerows(self, rows):
        return super().writerows(self.postprocess(row) for row in rows)


class Base(object):
    SNIFF_SIZE = 1024

    @contextlib.contextmanager
    def csv_file_reader(self, filename, dialect=None, fn=None, rn=None):
        with common.open_file(filename) as f:
            if not dialect:
                dialect = csv.Sniffer().sniff(f.buffer.peek(self.SNIFF_SIZE).decode(common.ENCODING))
            yield Reader(f, dialect=dialect, fn=fn, rn=rn)


class In(Base):
    @classmethod
    def input_args(cls, parser):
        parser.add_argument(
            '--input',
            action='append',
            help='Input file',
        )
        parser.add_argument(
            '--input-dialect',
            metavar='DIALECT',
            choices=csv.list_dialects(),
            default='default',
            help='CSV dialect for input file',
        )

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)
        cls.input_args(parser)

    @common.lazy
    @common.stepback
    def iter_inputs(self):
        rn = itertools.count()
        input_list = self.flags.input or ['-']
        for fn, filename in enumerate(input_list):
            with self.csv_file_reader(filename=filename, dialect=self.flags.input_dialect, fn=fn, rn=rn) as csv_reader:
                yield csv_reader

    @common.lazy
    @common.stepback
    def iter_rows(self):
        for reader in self.iter_inputs():
            for r in reader:
                yield self.preprocess_input(r)

    def preprocess_input(self, row):
        return row

    def get_current_input(self):
        return next(iter(self.iter_inputs()))

    def get_in_fieldnames(self):
        return self.get_current_input().fieldnames



class Out(object):
    OUT_FIELDS = None

    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)

        parser.add_argument(
            '--output',
            default='-',
            help='Output file',
        )
        parser.add_argument(
            '--output-dialect',
            metavar='DIALECT',
            choices=csv.list_dialects(),
            default='default',
            help='CSV dialect for output file',
        )

    def get_out_fieldnames(self):
        return self.OUT_FIELDS

    @contextlib.contextmanager
    def get_output(self):
        with common.open_file(self.flags.output, 'w+') as out_f:
            w = Writer(out_f, fieldnames=self.get_out_fieldnames(), dialect=self.flags.output_dialect, extrasaction='ignore', postprocess=self.postprocess_output)
            w.writeheader()
            yield w

    def postprocess_output(self, row):
        return row


class Filter(Out, In):
    def get_out_fieldnames(self):
        return super().get_out_fieldnames() or self.get_in_fieldnames()

    def main(self):
        with self.get_output() as csv_out:
            csv_out.writerows(self.process(self.iter_rows()))

    def process(self, rows):
        return []


class DefaultDialect(csv.Dialect):
    """Describe the usual properties of Excel-generated CSV files."""
    delimiter = ','
    quotechar = '"'
    doublequote = True
    skipinitialspace = False
    lineterminator = '\n'
    quoting = csv.QUOTE_MINIMAL
csv.register_dialect("default", DefaultDialect)
