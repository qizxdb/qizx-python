===========
Qizx-Python
===========

A Python interface for the for Qualcomm Qizx Database.

Usage
-----
The client can be used directly from the command line (after installation)::

    sh
    qizxpy eval --library phonedb '//employee/name'

Or as a module from your program::

    python
    import qizx
    from lxml import etree

    client = qizx.Client(url='https://user:pass@qizx-server.mycompany.com/api')
    result = etree.XML(client.eval('//employee/name', library='phonedb', raw=True))

    for name in result:
        print('%s' % (name.text,))

The ``url`` parameter to ``qizx.Client`` can also specify a configuration file
section (see below).  See the pydocs for details of other operations.

Configuration
-------------
The client configuration is stored in a yaml formatted file, by default in
either ``$HOME/.qizx`` or ``.qizx`` in the working directory.

The configuration file can have multiple sections with different
configurations.  The command-line tool uses the ``qizx`` section by default.

Each section can have the following parameters:
 * ``url``: The API url for the server (including username and password)
 * ``verify``: Control SSL certificate verification for https URLs
 * ``cert``: Specifies a client-side SSL certificate

An example complete configuration file is::

    yaml
    [qizx]
    url: https://admin:SparkleMotion123@qizx-server.mycompany.com/api
    verify: False

Installing
----------
To install the module from source::

    sh
    pip install .

Testing
-------
To run the unit tests::

    sh
    python setup.py test

(requires a connection to be configured with write access)

Compatibility
-------------
This module is designed to be compatible with Python 2.7.x and Python 3.3+.
