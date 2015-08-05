"""
Packaging Qizx python bindings

This code is part of the Qizx application components
Copyright (c) 2003-2015 Michael Paddon

For conditions of use, see the accompanying license files.

This module is designed for Python 3, and is backwards compatible with
Python 2.
"""
import re

from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

version = ''
with open(path.join(here, 'qizx/__init__.py'), encoding='utf-8') as f:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        f.read(), re.MULTILINE).group(1)
if not version:
    raise RuntimeError('Version information missing from qizx/__init__.py')

with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    readme = f.read()

with open('HISTORY.rst', 'r', 'utf-8') as f:
    history = f.read()

long_description = readme + '\n\n' + history

setup(
    name='qizx',
    version=version,
    description='Qualcomm Qizx Python API',
    long_description=long_description,
    url='https://github.com/qizxdb/qizx-python',
    author="Shaun O'Keefe",
    author_email='shaun.okeefe.0@gmail.com',
    license='MIT',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Database :: Front-Ends',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ],
    keywords='qizx bindings api xml database',
    entry_points={
        'console_scripts': [
            'qizxpy = qizx.qizx:main',
        ],
    },
    packages=find_packages(exclude=['tests*']),
    package_data={'': ['LICENSE']},
    test_suite='tests',
    install_requires=['isodate', 'requests', 'pyyaml'],
)
