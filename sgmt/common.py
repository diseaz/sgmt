#!/usr/bin/python3
# -*- mode: python; coding: utf-8 -*-

"""Common tools."""

import contextlib
import functools
import locale
import logging
import sys


_logger = logging.getLogger(__name__)


ENCODING = locale.getpreferredencoding(False)


@contextlib.contextmanager
def open_file(filename, mode='r'):
    '''Like `open` but returns a standard stream for name "-".'''
    if filename and filename != '-':
        with open(filename, mode) as f:
            yield f
    else:
        yield (sys.stdout if mode and mode[0] in 'wxa' else sys.stdin)


def lazy(f):
    member_name = f.__name__ + '$value'
    @functools.wraps(f)
    def wrapper(self):
        if hasattr(self, member_name):
            return getattr(self, member_name)
        value = f(self)
        setattr(self, member_name, value)
        return value
    def set(self, value):
        setattr(self, member_name, value)
    wrapper.set = set
    return wrapper


def stepback(f):
    @functools.wraps(f)
    def wrapper(*args, **kw):
        return StepBackIterable(f(*args, **kw))
    return wrapper


class StepBackIterable(object):
    def __init__(self, itr):
        self.itr = iter(itr)
        self.last = None

    def __iter__(self):
        return StepBackIterator(self)

    def _get_next(self):
        try:
            self.last = next(self.itr)
        except StopIteration:
            self.last = None
            raise
        return self.last


class StepBackIterator(object):
    def __init__(self, itr):
        self.itr = itr
        self.last = itr.last

    def __iter__(self):
        return self

    def __next__(self):
        if self.last is not None:
            last, self.last = self.last, None
        else:
            last = self.itr._get_next()
        return last


class Struct(dict):
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self.__dict__ = self

    def _as_dict(self):
        return {k:v for k,v in self.items() if k != '__dict__'}

    def __eq__(self, other):
        return as_dict(self) == as_dict(other)

    def __req__(self, other):
        return as_dict(self) == as_dict(other)

    def copy(self):
        return type(self)(self._as_dict())


def as_dict(obj):
    f = getattr(obj, '_as_dict', None)
    if f is not None:
        return f()
    return dict(obj)
