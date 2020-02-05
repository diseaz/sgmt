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


class GraphOpMixin(object):
    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)

        parser.add_argument(
            '--src',
            default='src',
            help='Edge source column name.',
        )
        parser.add_argument(
            '--dst',
            default='dst',
            help='Edge destination column name.',
        )

    @common.lazy
    def graph_tool(self):
        return GraphTool(src=self.flags.src, dst=self.flags.dst)


class GraphNodeOpMixin(GraphOpMixin):
    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)

        parser.add_argument(
            '--node',
            default='node',
            help='Node output column name.',
        )

    @common.lazy
    def graph_tool(self):
        return GraphTool(src=self.flags.src, dst=self.flags.dst, node=self.flags.node)

    def get_out_fieldnames(self):
        return [self.flags.node]


class InvertableArg(object):
    @classmethod
    def add_arguments(cls, parser):
        super().add_arguments(parser)

        parser.add_argument(
            '--inverted',
            action='store_true',
            help='Operate on a graph with edges inverted',
        )


class InvertableInputMixin(InvertableArg, GraphOpMixin):
    def preprocess_input(self, row):
        row = super().preprocess_input(row)
        if self.flags.inverted:
            return self.graph_tool().invert(row)
        return row


class InvertableOutputMixin(InvertableArg, GraphOpMixin):
    def postprocess_output(self, row):
        row = super().preprocess_input(row)
        if self.flags.inverted:
            return self.graph_tool().invert(row)
        return row


class InvertableMixin(InvertableInputMixin, InvertableOutputMixin):
    pass


class BfsCmd(InvertableMixin, nodeutil.PatternsMixin, csvutil.Filter, app.Command):
    '''BFS on graph.'''
    name = 'bfs'

    def process(self, rows):
        gt = self.graph_tool()
        return gt.bfs(rows, self.nodes_match_func())


class SourcesCmd(InvertableInputMixin, GraphNodeOpMixin, csvutil.Filter, app.Command):
    '''Extract source nodes of the graph.'''
    name = 'srcs'

    def process(self, rows):
        gt = self.graph_tool()
        return gt.sources(rows)


class GraphTool(object):
    def __init__(self, src='src', dst='dst', node='node'):
        self.src = src
        self.dst = dst
        self.node = node

    def bfs(self, rows, is_src):
        return bfs(rows, is_src, self.src, self.dst)

    def invert(self, row):
        row = row.copy()
        row[self.src], row[self.dst] = row[self.dst], row[self.src]
        return row

    def sources(self, rows):
        return sources(rows, self.node, self.src, self.dst)


def bfs(rows, is_src, src, dst):
    '''BFS.

        - rows: an iterable of dicts.  Each row represents a graph edge with
          columns for edge source and edge destination.  Edge source and edge
          destination are `node`.

        - is_src is a predicate function.  is_src(node) is true for initial
          nodes for BFS.

        - src: name of the edge source column

        - dst: name of the edge destination column

    Yields rows.
    '''
    deps = collections.defaultdict(dict)
    for r in rows:
        deps[r[src]][r[dst]] = r

    visited = {n for n in deps if is_src(n)}
    queue = collections.deque(visited)
    while queue:
        fsrc = queue.popleft()
        for fdst, row in deps[fsrc].items():
            yield row
            if fdst in visited:
                continue
            visited.add(fdst)
            queue.append(fdst)


def sources(rows, node, src, dst):
    srcs, dsts = set(), set()
    for row in rows:
        srcs.add(row[src])
        if dst != src:
            dsts.add(row[dst])
    for n in sorted(srcs - dsts):
        r = csvutil.Row()
        r[node] = n
        yield r
