# coding: utf-8

import sys
from setuptools import setup, find_packages

NAME = "routes_aggregator"
VERSION = "1.0.0"

# To install the library, run the following
#
# python setup.py install
#
# prerequisite: setuptools
# http://pypi.python.org/pypi/setuptools

REQUIRES = []

setup(
    name=NAME,
    version=VERSION,
    description="Route Aggregator",
    author_email="alexanderanosov1996@gmail.com",
    url="",
    keywords=["Route Aggregator API"],
    install_requires=REQUIRES,
    packages=find_packages(),
    package_data={'': []},
    include_package_data=True,
    long_description="""\
    Routes aggregator
    """
)

