#!/usr/bin/python

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="diseaz-sgmt",
    version="0.0.1",
    author="Dmitry Azhichakov",
    author_email="dazhichakov@gmail.com",
    description="Sets and Graphs Manipulation Tool",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/diseaz/sgmt",
    packages=setuptools.find_packages(),
    install_requires=[
        "diseaz-dsapy @ git+https://github.com/diseaz/dsapy.git",
    ],
    entry_points={
        "console_scripts": [
            "sgmt=sgmt.scripts.sgmt:run",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
