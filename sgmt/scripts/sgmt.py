#!/usr/bin/python3
# -*- mode: python; coding: utf-8 -*-

"""Sets and Graphs Manipulation Tool."""

from dsapy import app
from dsapy import flag

import sgmt

@flag.argroup('Options')
def _options(parser):
    return


@app.main()
def main(flags):
    print(flags)


def run():
    app.start()


if __name__ == '__main__':
    run()
