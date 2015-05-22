# -*- coding: utf-8 -*-
"""Qizx RESTful API bindings and command line interface.

This code is part of the Qizx application components
Copyright (c) 2015 Michael Paddon

For conditions of use, see the accompanying license files.

This module is designed for Python 3, and is backwards compatible with
Python 2.
"""

# major/minor version
version = (0, 9)

import argparse
import cgi
import collections
import isodate
import itertools
import json
import logging
import os
import re
import requests
import sys
import time
import xml.etree.ElementTree
import yaml
import datetime

# Python 3 is our reference target
if sys.version_info[0] >= 3:
    import configparser
    import urllib.parse
    import http.client
else:
    # backwards compatibility with python2
    configparser = __import__("ConfigParser")
    urllib = __import__("urllib")
    urllib.parse = __import__("urlparse")
    http = lambda: None
    http.client = __import__("httplib")

class QizxError(Exception):
    """Base class for all Qizx errors."""
    pass

class QizxBadRequestError(QizxError):
    """Request is unknown or has invalid or missing parameters."""
    pass

class QizxServerError(QizxError):
    """Incident in server, for example a request on a stopped XML engine."""
    pass

class QizxNotFoundError(QizxError):
    """Attempt to access a non-existent document or collection."""
    pass

class QizxAccessControlError(QizxError):
    """Attempt to access forbidden documents or collections."""
    pass

class QizxXMLDataError(QizxError):
    """Error detected by the Qizx XML database engine."""
    pass

class QizxCompilationError(QizxError):
    """XQuery compile time error, in syntax or static analysis."""
    pass

class QizxEvaluationError(QizxError):
    """XQuery evaluation error."""
    pass

class QizxTimeoutError(QizxError):
    """Eval time limit reached."""
    pass

class QizxImportError(QizxError):
    """Import of at least some documents failed."""
    pass

class UnexpectedResponseError(QizxError):
    """Unexpected response received from Qizx server."""

    def __init__(self, response):
        Exception.__init__(self, response.content)

class TransactionError(QizxError):
    """A try/catch transaction failed."""

    @staticmethod
    def itemToString(item):
        return "%s:%s" % (item.get("type", "") , item.text)

    def __init__(self, items):
        Exception.__init__(self, ";".join(map(self.itemToString, items)))

class Client:
    """Qizx RESTful API client.

    Each qizx client is wrapped around a requests.Session instance.
    """

    # map Qizx errors to exceptions
    _errors = {
        "BadRequest": QizxBadRequestError,
        "Server": QizxServerError,
        "NotFound": QizxNotFoundError,
        "AccessControl": QizxAccessControlError,
        "XMLData": QizxXMLDataError,
        "Compilation": QizxCompilationError,
        "Evaluation": QizxEvaluationError,
        "TimeOut": QizxTimeoutError}

    # parser for import errors message
    _import_parser = re.compile(r"^IMPORT ERRORS ([0-9]+)\s*$",
        re.MULTILINE)

    def __init__(self, url = "qizx", client_timeout = None,
        configpaths = ["/etc/qizx", os.path.expanduser("~/.qizx"), ".qizx"]):
        """Construct a client.

        @param url: Qizx service URL.
        @param configpaths: configuration file paths.

        A typical URL is of the form
        "http://user:password@host:port/qizx/api#library".
        Port defaults to 80.

        If credentials are not supplied in the URL,
        the user's .netrc file is used.

        The library fragment is optional and specifies the default for
        library specific API calls.

        The "https" scheme is also supported.
        In this case, port defaults to 443.
        If the "verify" and "cert" attributes are set, they are
        used to control server side certifcate verification and
        client side authentication, respectively. Please see the
        request module documentation for more information.

        If the "client_timeout" paramater is set, the client will set a
        timeout on the socket.  is not a time limit on the entire response
        download; rather, an exception is raised if the server has not
        issued a response for timeout seconds (more precisely, if no
        bytes have been received on the underlying socket for timeout seconds).

        A URL without a scheme is treated as a section name,
        which is expected to be found in a configuration file.
        The "url" field in that section then specifies the service URL.
        The "verify" field and "cert" field, if present, are used to
        initialize those attributes.
        """

        # no scheme in url?
        if ":" not in url:
            # read configuration
            config = configparser.ConfigParser()
            config.read(configpaths)

            # TLS server verify
            try:
                self.verify = config.getboolean(url, "verify")
            except ValueError:
                self.verify = config.get(url, "verify")
            except configparser.NoOptionError:
                pass

            # TLS client side certificate
            try:
                self.cert = config.get(url, "cert")
            except configparser.NoOptionError:
                pass

            # resolve url
            url = config.get(url, "url")

        # parse url
        url = urllib.parse.urlparse(url)

        # credentials in url?
        if url.username and url.password:
            # use credentials for basic authorization
            auth = (url.username, url.password)

            # strip credentials from url
            url = urllib.parse.ParseResult(url.scheme,
                url.netloc.split("@", 1)[1],
                url.path, url.params, url.query, url.fragment)
        else:
            auth = None

        # Set socket timeout
        if client_timeout is not None:
            self.client_timeout = int(client_timeout)

        # set default library
        self._library = url.fragment if url.fragment else None

        # construct request session and base URL
        self._session = requests.Session()
        self._session.auth = auth
        self._baseurl = urllib.parse.urlunparse(
            (url.scheme, url.netloc, url.path, url.params, None, None))

        # files & properties batches
        self._storables = []
        self._props = {}

    def close(self):
        """Close the client."""

        # older versions of requests don't have a close method
        if hasattr(self._session, 'close'):
            self._session.close()

    def info(self):
        """Get server information.

        Returns a mapping of names to values.
        """

        # send request
        response = self._get_request(params = {
            "op": "info"})

        # parse response
        if response.mimetype != "text/xml":
            raise UnexpectedResponseError(response)
        root = xml.etree.ElementTree.fromstring(response.content)
        return collections.OrderedDict(
            [self._decode_property(property)
                for property in root.findall("property")])

    def eval(self, query,
        format = None, mode = None, maxtime = None, counting = None,
        count = None, first = None, library = None, raw = False):
        """Evaluate an XQuery expression.

        @param query: the xquery expression to evaluate.
        @param format: format of response.
            Possible values are "items", "xml" (default), "html" or "xhtml".
        @param mode: execution mode (when format == "items").
            Possible values are "profile".
        @param maxtime: maximum execution time in milliseconds.
        @param counting: counting method (when format == "items").
            Possible values are "exact" (default), "estimated", or "none".
        @param count: maximum number of items (when format == "items").
        @param first: rank of first item to return (when format == "items").
        @param library: library name (default library if None).
        @param raw: return raw bytes rather than a string.

        If the format was "items", the result is a list of values.
        Otherwise returns a string (or a buffer if raw = True).
        """

        # sanity check
        assert format in ("items", "xml", "html", "xhtml", None)
        if mode is not None:
            assert format == "items"
            assert mode in ("profile",)
        if maxtime is not None:
            assert maxtime >= 0
        if counting is not None:
            assert format == "items"
            assert counting in ("exact", "estimated", "none")
        if count is not None:
            assert format == "items"
            assert count >= 1
        if first is not None:
            assert format == "items"
            assert first >= 1

        # send request
        response = self._post_request(data = {
            "op": "eval",
            "query": query,
            "format": format,
            "mode": mode,
            "maxtime": maxtime,
            "counting": counting,
            "count": count,
            "first": first,
            "library": library if library is not None else self._library})

        # parse response
        if format == "items":
            root = xml.etree.ElementTree.fromstring(response.content)
            return [self._decode_item(item) for item in root.findall("item")]

        # return raw bytes or string
        return response.content if raw else response.text

    def get(self, path, library = None, raw = False):
        """Retrieve a document or collection listing.

        @param path: path of document or collection.
        @param library: library name (default library if None).
        @param raw: return raw bytes rather than a string.

        Returns a string (or a buffer if raw = True).
        """

        # send request
        response = self._get_request(params = {
            "op": "get",
            "path": path,
            "library": library if library is not None else self._library})

        # return raw bytes or string
        return response.content if raw else response.text

    def put(self, storables, xml = True, library = None):
        """Store documents.

        @param storables: sequence of (path, content) tuples.
        @param xml: store documents as XML?
        @param library: library name (default library if None).

        Content may be specified as a buffer, a string or a readable.
        """

        # construct request
        data = {
            "op": "put" if xml else "putnonxml",
            "library": library if library is not None else self._library}
        files = {}
        counter = itertools.chain([""], itertools.count(2))
        for storable in storables:
            count = next(counter)
            data["path{0}".format(count)] = storable[0]
            files["data{0}".format(count)] = storable[1]

        # send request as multipart/form-data
        response = self._post_request(data = data, files = files)

        # parse response
        if response.mimetype != "text/plain":
            raise UnexpectedResponseError(response)
        m = self._import_parser.search(response.text)
        if m:
            errors = int(m.group(1))
            if errors > 0:
                raise QizxImportError(response.text)
        else:
            raise UnexpectedResponseError(response)

    def batch(self, storable):
        """Batches documents for later storage.

        @param storable: (path, content) tuple.

        Content may be specified as a buffer, a string or a readable.
        """

        self._storables.append(storable)

    def flush(self, xml = True, library = None):
        """Store batched documents.

        @param xml: store documents as XML?
        @param library: library name (default library if None).
        """

        if len(self._storables) > 0:
            self.put(self._storables, xml, library)
            self._storables = []

    def mkcol(self, path, parents = True, library = None):
        """Create a collection.

        @param path: path of collection.
        @param parents: create parent collections?
        @param library: library name (default library if None).

        Returns the path of the collection.
        """

        # send request
        response = self._post_request(data = {
            "op": "mkcol",
            "path": path,
            "parents": "true" if parents else "false",
            "library": library if library is not None else self._library})

        # parse response
        if response.mimetype != "text/plain":
            raise UnexpectedResponseError(response)
        if not response.text:
            raise UnexpectedResponseError(response)
        return response.text.splitlines()[0]

    def move(self, src, dst, library = None):
        """Move a document or collection.

        @param src: source path.
        @param dst: destination path.
        @param library: library name (default library if None).

        Returns the path of the destination.
        """

        # send request
        response = self._post_request(data = {
            "op": "move",
            "src": src,
            "dst": dst,
            "library": library if library is not None else self._library})

        # parse response
        if response.mimetype != "text/plain":
            raise UnexpectedResponseError(response)
        if not response.text:
            raise UnexpectedResponseError(response)
        return response.text.splitlines()[0]

    def copy(self, src, dst, library = None):
        """Copy a document or collection.

        @param src: source path.
        @param dst: destination path.
        @param library: library name (default library if None).

        Returns the path of the destination.
        """

        # send request
        response = self._post_request(data = {
            "op": "copy",
            "src": src,
            "dst": dst,
            "library": library if library is not None else self._library})

        # parse response
        if response.mimetype != "text/plain":
            raise UnexpectedResponseError(response)
        if not response.text:
            raise UnexpectedResponseError(response)
        return response.text.splitlines()[0]

    def delete(self, path, library = None):
        """Delete a document or collection.

        @param path: document or collection path.
        @param library: library name (default library if None).

        Returns the deleted path, or None if the member didn't exist.
        """

        # send request
        response = self._post_request(data = {
            "op": "delete",
            "path": path,
            "library": library if library is not None else self._library})

        # parse response
        if response.mimetype != "text/plain":
            raise UnexpectedResponseError(response)
        if not response.text:
            raise UnexpectedResponseError(response)
        return response.text.splitlines()[0] or None

    def getprop(self, path, names = None, depth = 0, library = None):
        """Get document or collection properties.

        @param path: path of document or collection.
        @param names: sequence of property names to return (all by default).
        @param depth: depth to descend in to collection (default = 0).
        @param library: library name (default library if None).

        Returns a mapping of paths to properties,
        where properties is a mapping of names to values.
        """

        # send request
        response = self._get_request(params = {
            "op": "getprop",
            "path": path,
            "properties": " ".join(names) if names else None,
            "depth": depth if depth > 0 else None,
            "library": library if library is not None else self._library})

        # parse response
        if response.mimetype != "text/xml":
            raise UnexpectedResponseError(response)
        root = xml.etree.ElementTree.fromstring(response.content)
        return collections.OrderedDict(
            [self._decode_properties(properties)
                for properties in root.findall("properties")])

    def setprop(self, path, properties, library = None):
        """Set document or collection properties.

        @param path: path of document or collection.
        @param properties: a sequence of (name, value, type) tuples.
        If value is None, the property is deleted.
        Type must be one of "string", "boolean", "integer",
        "double", "dateTime", "node()", "<expression>" or None.
        Value and type may be ommitted, in which case they default to None.
        @param library: library name (default library if None).

        Returns the path of the document or collection.
        """

        # construct request
        params = {
            "op": "setprop",
            "path": path,
            "library": library if library is not None else self._library}
        counter = itertools.chain([""], itertools.count(2))
        for property in properties:
            name = property[0]
            value = property[1] if len(property) > 1 else None
            type = property[2] if len(property) > 2 else None
            assert type in ("string", "boolean", "integer",
                "double", "dateTime", "node()", "<expression>", None)

            count = next(counter)
            params["name{0}".format(count)] = name
            params["value{0}".format(count)] = value
            params["type{0}".format(count)] = type

        # send request
        response = self._post_request(data = params)

        # parse response
        if response.mimetype != "text/plain":
            raise UnexpectedResponseError(response)
        if not response.text:
            raise UnexpectedResponseError(response)
        return response.text.splitlines()[0]

    def batchprop(self, path, properties):
        """Batches up document or collection properties.

        @param path: path of document or collection.
        @param properties: a sequence of (name, value, type) tuples.
        If value is None, the property is deleted.
        Type must be one of "string", "boolean", "integer",
        "double", "dateTime", "node()", "<expression>" or None.
        Value and type may be ommitted, in which case they default to None.
        """

        if path not in self._props:
            self._props[path] = []
        self._props[path].extend(properties)

    def flushprops(self, library = None):
        """Flushes document or collection properties batch.

        @param library: library name (default library if None).
        """

        query = "try {"
        for path in self._props:
            for property in self._props[path]:
                name = property[0]
                value = property[1] if len(property) > 1 else None
                type = property[2] if len(property) > 2 else None
                assert type in ("string", "boolean", "integer", "double", "dateTime", "node()", "<expression>", None)

                data = ''
                if type is None or type == "string" :
                    data = '"%s"' % (value)
                elif type == "node()":
                    data = xml.etree.ElementTree.tostring(value) if isinstance(value, xml.etree.ElementTree.Element) else value
                elif type == "boolean":
                    data = 'true()' if value else 'false()'
                elif type == "dateTime":
                    data = 'xs:dateTime("%s")' % (value.isoformat() if isinstance(value, datetime.datetime) else value)
                else:
                    data = value
                query += 'xlib:set-property("%s", "%s", %s);' % (
                                                            path, name, data)

        query += "xlib:commit();"
        # wrap an error inside an xml response
        # eg. <error type="errors:XLIB0001">no such library member: /a</error>
        # NB. Errors are returned returned as HTTP 200 OK
        query += "}catch($err){"
        query += "xlib:rollback(),"
        query += "element error{attribute type{name($err)},string($err)}"
        query += "}"
        self._props = {}

        # send request
        response = self._post_request(data = {
            "op": "eval",
            "query": query,
            "format": "items",
            "library": library if library is not None else self._library})

        # parse response (format=items)
        if response.mimetype != "text/xml":
            raise UnexpectedResponseError(response)
        root = xml.etree.ElementTree.fromstring(response.content)
        items = [self._decode_item(item) for item in root.findall("item")]
        if len(items) > 0:
            raise TransactionError(items)

    def queryprop(self, query, names = None, path = None, library = None):
        """Query document or collection properties.

        @param query: expression specifying documents or collections.
        @param names: sequence property names to return
            ("path" and "nature" by default).
        @param path: path of collection restricting query (optional).
        @param library: library name (default library if None).

        Returns a mapping of paths to properties,
        where properties is a mapping of names to values.
        """

        # send request
        response = self._post_request(data = {
            "op": "queryprop",
            "query": query,
            "properties": " ".join(names) if names else None,
            "path": path,
            "library": library if library is not None else self._library})

        # parse response
        if response.mimetype != "text/xml":
            raise UnexpectedResponseError(response)
        root = xml.etree.ElementTree.fromstring(response.content)
        return collections.OrderedDict(
            [self._decode_properties(properties)
                for properties in root.findall("properties")])

    def listlib(self):
        """List XML libraries.

        Returns a sequence of library names.
        """

        # send request
        response = self._get_request(params = {
            "op": "listlib"})

        # parse response
        if response.mimetype != "text/plain":
            raise UnexpectedResponseError(response)
        if not response.text:
            raise UnexpectedResponseError(response)
        return response.text.splitlines()

    def server(self, command):
        """Send a server control command.

        @param command: server command, from the following:
        "status": check if Qizx engine is running,
        "online": start the Qizx engine,
        "offline": stop the Qizx engine,
        "reload": reload the Qizx engine.

        Returns status of server ("online" or "offline")
        """

        # send request
        assert command in ("status", "online", "offline", "reload")
        response = self._post_request(data = {
            "op": "server",
            "command": command})

        # parse response
        if response.mimetype != "text/plain":
            raise UnexpectedResponseError(response)
        if not response.text:
            raise UnexpectedResponseError(response)
        return response.text.splitlines()[0]

    def mklib(self, name):
        """Create a library.

        @param name: library to create.

        Returns name of library.
        """

        # send request
        response = self._post_request(data = {
            "op": "mklib",
            "name": name})

        # parse response
        if response.mimetype != "text/plain":
            raise UnexpectedResponseError(response)
        if not response.text:
            raise UnexpectedResponseError(response)
        return response.text.splitlines()[0]

    def dellib(self, name):
        """Delete a library.

        @param name: library to delete.

        Returns name of library.
        """

        # send request
        response = self._post_request(data = {
            "op": "dellib",
            "name": name})

        # parse response
        if response.mimetype != "text/plain":
            raise UnexpectedResponseError(response)
        if not response.text:
            raise UnexpectedResponseError(response)
        return response.text.splitlines()[0]

    def reindex(self, library = None):
        """Request a reindexing of a library.

        @param library: library to reindex (default library if None).

        Returns a progress identifier.
        """

        # send request
        response = self._post_request(data = {
            "op": "reindex",
            "library": library if library is not None else self._library})

        # parse response
        if response.mimetype != "text/plain":
            raise UnexpectedResponseError(response)
        if not response.text:
            raise UnexpectedResponseError(response)
        return response.text.splitlines()[0]

    def optimize(self, library = None):
        """Request optimization of a library.

        @param library: library to optimize (default library if None).

        Returns a progress identifier.
        """

        # send request
        response = self._post_request(data = {
            "op": "optimize",
            "library": library if library is not None else self._library})

        # parse response
        if response.mimetype != "text/plain":
            raise UnexpectedResponseError(response)
        if not response.text:
            raise UnexpectedResponseError(response)
        return response.text.splitlines()[0]

    def backup(self, path, library = "*"):
        """Start a backup.

        @param path: server side directory where backup is written.
        @param library: library to backup ("*" == all libraries).

        Returns a progress identifier.
        """

        # send request
        response = self._post_request(data = {
            "op": "backup",
            "path": path,
            "library": library if library is not None else self._library})

        # parse response
        if response.mimetype != "text/plain":
            raise UnexpectedResponseError(response)
        if not response.text:
            raise UnexpectedResponseError(response)
        return response.text.splitlines()[0]

    def progress(self, id):
        """Return progress information about a long task.

        @param id: progress identifier.

        Returns a (task, done) tuple, where done is a number between 0 and 1.
        """

        # send request
        response = self._get_request(params = {
            "op": "progress",
            "id": id})

        # parse response
        if response.mimetype != "text/plain":
            raise UnexpectedResponseError(response)
        if not response.text:
            raise UnexpectedResponseError(response)
        lines = response.text.splitlines()
        if len(lines) < 2:
            raise UnexpectedResponseError(response)
        try:
            done = float(lines[1])
        except ValueError:
            raise UnexpectedResponseError(response)
        return (lines[0], done)

    def getindexing(self, library = None):
        """Get the indexing specification of a library.

        @param library: library name (default library if None).

        Returns the indexing specification.
        """

        # send request
        response = self._get_request(params = {
            "op": "getindexing",
            "library": library if library is not None else self._library})

        # parse response
        if response.mimetype != "text/xml":
            raise UnexpectedResponseError(response)
        return response.text

    def setindexing(self, indexing, library = None):
        """Set the indexing specification of a library.

        @param indexing: indexing specification.
        @param library: library name (default library if None).

        The specification may be a string or a readable.

        Returns the name of the library.
        """

        # send request
        response = self._post_request(
            data = {
                "op": "setindexing",
                "library": library if library is not None else self._library},
            files = {
                "indexing": indexing})

        # parse response
        if response.mimetype != "text/plain":
            raise UnexpectedResponseError(response)

    def getacl(self, path, scope = None, library = None):
        """Get the access control information for a library member.

        @param path: path of library member.
        @param scope: "local" (default) or "inherit".
        @param library: library name (default library if None).

        Returns the access control information.
        """

        # send request
        assert scope in ("local", "inherit", None)
        response = self._get_request(params = {
            "op": "getacl",
            "path": path,
            "scope": scope,
            "library": library if library is not None else self._library})

        # parse response
        if response.mimetype != "text/xml":
            raise UnexpectedResponseError(response)
        return response.text

    def setacl(self, acl, library = None):
        """Set access control information.

        @param acl: access control specification.
        @param library: library name (default library if None).

        The specification may be a string or a readable.

        Returns the name of the library.
        """

        # resolve readable acl
        if hasattr(acl, "read"):
            acl = acl.read()

        # send request
        response = self._post_request(
            data = {
                "op": "setacl",
                "acl": acl,
                "library": library if library is not None else self._library})

        # parse response
        if response.mimetype != "text/plain":
            raise UnexpectedResponseError(response)

    def getconfig(self, level = None):
        """Get configuration properties.

        @param level: "admin" (default) or "expert".

        Returns the server configuration.
        """

        # send request
        assert level in ("admin", "expert", None)
        response = self._get_request(params = {
            "op": "getconfig",
            "level": level,
            "format": "json"})

        # parse response
        if response.mimetype != "application/json":
            raise UnexpectedResponseError(response)
        return json.loads(self._canonical_json(response.text))["records"]

    def changeconfig(self, properties):
        """Change Configuration properties.

        @param properties: sequence of (name, value) tuples.

        Returns True if at least one property was changed.
        """

        # construct request
        params = {
            "op": "changeconfig"}
        counter = itertools.count(0)
        for name, value in properties:
            count = next(counter)
            params["property{0}".format(count)] = name
            params["value{0}".format(count)] = value

        # send request
        response = self._post_request(data = params)

        # parse response
        if response.mimetype != "text/plain":
            raise UnexpectedResponseError(response)
        if not response.text:
            raise UnexpectedResponseError(response)
        return response.text.splitlines()[0] == "true"

    def getstats(self, level = "admin"):
        """Get server statistics

        @param level: "admin" or "expert".
        """

        # send request
        response = self._get_request(params = {
            "op": "getstats",
            "level": level,
            "format": "json"})

        # parse response
        if response.mimetype != "application/json":
            raise UnexpectedResponseError(response)
        return json.loads(self._canonical_json(response.text))["records"]

    def listtasks(self, timeline = 0):
        """List maintenance tasks.

        @param timeline: maximum age in hours of past tasks (0 == current).

        Returns a sequence of mappings, one for each task.
        """

        # send request
        assert timeline >= 0
        response = self._get_request(params = {
            "op": "listtasks",
            "timeline": timeline,
            "format": "json"})

        # parse response
        if response.mimetype != "application/json":
            raise UnexpectedResponseError(response)
        return json.loads(self._canonical_json(response.text))["records"]

    def listqueries(self):
        """List running queries.

        Returns a sequence of mappings, one for each query.
        """

        # send request
        response = self._get_request(params = {
            "op": "listqueries",
            "format": "json"})

        # parse response
        if response.mimetype != "application/json":
            raise UnexpectedResponseError(response)
        return json.loads(self._canonical_json(response.text))["records"]

    def cancelquery(self, id):
        """Cancel a query.

        @param id: query identifier.

        Returns cancel status ("OK", "idle" or "unknown").
        """

        # send request
        response = self._get_request(params = {
            "op": "cancelquery",
            "xid": id})

        # parse response
        if response.mimetype != "text/plain":
            raise UnexpectedResponseError(response)
        if not response.text:
            raise UnexpectedResponseError(response)
        return response.text.splitlines()[0]

    def wait(self, id, timeout = None, poll = 5):
        """Convenience function to wait for a long task.

        @param id: progress identifier.
        @param timeout: maximum seconds to wait.
        @param poll: seconds between polling for task completion.

        Returns True if the task is complete.
        """

        start = time.time()
        task, complete = self.progress(id)
        while complete < 1:
            if timeout and time.time() - start > timeout:
                return False
            time.sleep(poll)
            task, complete = self.progress(id)
        return True

    def _decode_item(self, item):
        """Decode an <item> element.

        @param item: item element.

        Returns a value.
        """

        ptype = item.get("type", "string")
        if ptype == "boolean":
            return item.text == "true"
        elif ptype == "integer":
            return int(item.text)
        elif ptype == "double":
            return float(item.text)
        elif ptype == "dateTime":
            return isodate.parse_datetime(item.text)
        if ptype == "element()":
            return list(item)[0]
        else:
            return item.text

    def _decode_property(self, property):
        """Decode a <property> element.

        @param property: a property element.

        Returns a (name, value) tuple.
        """

        return property.get("name"), self._decode_item(property)

    def _decode_properties(self, properties):
        """Decode a <properties> element.

        @param properties: a properties element.

        Returns a (path, mapping) tuple.
        """

        return properties.get("path"), collections.OrderedDict(
            [self._decode_property(property)
                for property in properties.findall("property")])

    def _get_request(self, *args, **kwargs):
        """Perform a Qizx get request."""

        if hasattr(self, "verify"):
            kwargs["verify"] = self.verify
        if hasattr(self, "cert"):
            kwargs["cert"] = self.cert
        if hasattr(self, "client_timeout"):
            kwargs["timeout"] = self.client_timeout
        return self._check_response(
            self._session.get(self._baseurl, *args, **kwargs))

    def _post_request(self, *args, **kwargs):
        """Perform a Qizx post request."""

        if hasattr(self, "verify"):
            kwargs["verify"] = self.verify
        if hasattr(self, "cert"):
            kwargs["cert"] = self.cert
        if hasattr(self, "client_timeout"):
            kwargs["timeout"] = self.client_timeout
        return self._check_response(
            self._session.post(self._baseurl, *args, **kwargs))

    def _check_response(self, response):
        """Check the reponse from a Qizx request.

        @param response: response object.
        """

        # request succeeded?
        response.raise_for_status()

        # content-type specified?
        if "content-type" in response.headers:
            # parse content type header
            response.mimetype, params = cgi.parse_header(
                response.headers["content-type"])

            # default to utf-8 for text content
            if response.mimetype.startswith("text/")\
                and "charset" not in params:
                response.encoding = "utf-8"
        else:
            response.mimetype = None

        # qizx error response?
        if response.mimetype == "text/x-qizx-error":
            # raise appropriate exception
            error = response.text.partition(":")[0]
            raise self._errors.get(error, QizxError)(response.text)

        return response

    def _canonical_json(self, text):
        """Canonicalise JSON.

        This is a temprary hack to work around bugs in the Qizx server
        JSON serialization.

        @param text: JSON text.

        Returns JSON text with barewords quoted
        and quotes escaped.
        """

        output = []
        for tokentype, token in self._json_tokens(text):
            if tokentype == "bareword":
               output.append("\"{0}\"".format(token))
            elif tokentype == "string":
               output.append(re.sub(r'([^\\])"(.)', r'\1\"\2', token))
            else:
                output.append(token)
        return "".join(output)

    # JSON token regular expression
    _json_tokenizer = re.compile(r"""
        (?P<true>true)
        |(?P<false>false)
        |(?P<null>null)
        |(?P<bareword>[A-Za-z][A-Za-z0-9]+)
        |(?P<string>"(?:[^"\\\x00-\x1f\x7f-\x9f]
            |\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4})
            |"[^\[\]{},:]*")*")
        |(?P<number>-?(?:0|[1-9][0-9]*)
            (?:\.[0-9]+)?
            (?:[Ee][+-]?[0-9]+)?)
        |(?P<whitespace>\s+)
        |(?P<char>.)
        """, re.VERBOSE)

    def _json_tokens(self, text):
        """Generate JSON tokens.

        @param text: JSON text.

        Yields (tokentype, token) tuples.
        """

        offset = 0
        while offset < len(text):
            m = self._json_tokenizer.match(text, offset)
            if m:
                yield m.lastgroup, m.group(0)
            else:
                return
            offset = m.end()


# debugging convenience
if "QIZX_DEBUG" in os.environ:
    # enable http level debugging
    http.client.HTTPConnection.debuglevel = 1

    # enable debug logging
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)

def main(argv = sys.argv):
    """Command line interface."""

    def info(client, args):
        info = client.info()
        yaml.dump(info, default_flow_style = False, stream = sys.stdout)

    def eval(client, args):
        print(client.eval(args.query,
             args.format, args.mode, args.maxtime,
             args.counting, args.count, args.first, args.library))

    def get(client, args):
        sys.stdout.buffer.write(client.get(args.path, args.library, True))

    def put(client, args):
        if args.src == "-":
            storables = [(args.dst, sys.stdin.buffer)]
        else:
            storables = [(args.dst, open(args.src, "rb"))]
        client.put(storables, not args.nonxml, args.library)

    def mput(client, args):
        storables = [
            ("{0}/{1}".format(args.collection, os.path.basename(path)),
                open(path, "rb")) for path in args.paths]
        client.put(storables, not args.nonxml, args.library)

    def mkcol(client, args):
        client.mkcol(args.path, args.parents, args.library)

    def move(client, args):
        client.move(args.src, args.dst, args.library)

    def copy(client, args):
        client.copy(args.src, args.dst, args.library)

    def delete(client, args):
        client.delete(args.path, args.library)

    def getprop(client, args):
        properties = client.getprop(args.path, args.names,
            max(args.depth, 0), args.library)
        yaml.dump(properties, default_flow_style = False, stream = sys.stdout)

    def setprop(client, args):
        client.setprop(args.path, [(args.name, args.value, args.type)],
            args.library)

    def queryprop(client, args):
        properties = client.queryprop(args.query, args.names, args.path,
            args.library)
        yaml.dump(properties, default_flow_style = False, stream = sys.stdout)

    def listlib(client, args):
        for library in client.listlib():
            print(library)

    def server(client, args):
        print(client.server(args.command))

    def mklib(client, args):
        client.mklib(args.name)

    def dellib(client, args):
        client.dellib(args.name)

    def reindex(client, args):
        print(client.reindex(args.library))

    def optimize(client, args):
        print(client.optimize(args.library))

    def backup(client, args):
        if args.exclusive:
            # backup already running?
            for t in client.listtasks():
                if t["TaskName"] == "backup":
                    raise QizxError("backup already running")

        id = client.backup(args.directory, args.library)
        print(id)

        if args.wait:
            client.wait(id)

    def progress(client, args):
        progress = client.progress(args.id)
        print("{0} {1}".format(*progress))

    def getindexing(client, args):
        print(client.getindexing(args.library))

    def setindexing(client, args):
        if args.path is None:
            args.indexing = sys.stdin
        else:
            args.indexing = open(args.path)
        client.setindexing(args.indexing, args.library)

    def getacl(client, args):
        print(client.getacl(args.path, args.scope, args.library))

    def setacl(client, args):
        if args.path is None:
            args.acl = sys.stdin
        else:
            args.acl = open(args.path)
        client.setacl(args.acl, args.library)

    def getconfig(client, args):
        config = client.getconfig(args.level)
        yaml.dump(config, default_flow_style = False, stream = sys.stdout)

    def changeconfig(client, args):
        if not client.changeconfig([(args.name, args.value)]):
            sys.exit(1)

    def getstats(client, args):
        stats = client.getstats(args.level)
        yaml.dump(stats, default_flow_style = False, stream = sys.stdout)

    def listtasks(client, args):
        tasks = client.listtasks(args.timeline)
        yaml.dump(tasks, default_flow_style = False, stream = sys.stdout)

    def listqueries(client, args):
        queries = client.listqueries()
        yaml.dump(queries, default_flow_style = False, stream = sys.stdout)

    def cancelquery(client, args):
        print(client.cancelquery(args.id))

    def wait(client, args):
        if not client.wait(args.id, args.timeout, args.poll):
            sys.exit(1)

    # yaml representer for mappings
    def mapping_representer(dumper, mapping):
        return dumper.represent_dict(mapping)

    # yaml representer for xml elements
    def element_representer(dumper, element):
        return dumper.represent_str(
            xml.etree.ElementTree.tostring(element, encoding="unicode"))

    # install custom yaml representations
    yaml.add_representer(collections.OrderedDict, mapping_representer)
    yaml.add_representer(xml.etree.ElementTree.Element, element_representer)

    # general options
    parser = argparse.ArgumentParser(prog = argv[0],
        description = "Qizx command line interface.")
    parser.add_argument("--url",
        default = "qizx",
        help = "service URL")
    subparsers = parser.add_subparsers()

    # info subcommand
    info_parser = subparsers.add_parser("info",
        help = "get server information")
    info_parser.set_defaults(handler = info)

    # eval subcommand
    eval_parser = subparsers.add_parser("eval",
        help = "evaluate an XQuery expression")
    eval_parser.add_argument("--format",
        choices = ["items", "xml", "html", "xhtml"],
        help = "format of response")
    eval_parser.add_argument("--mode",
        choices = ["profile"],
        help = "items execution mode")
    eval_parser.add_argument("--maxtime",
        help = "maximum execution time in milliseconds")
    eval_parser.add_argument("--counting",
        choices = ["exact", "estimated", "none"],
        help = "items counting method")
    eval_parser.add_argument("--count",
        type = int,
        help = "maximum number of items")
    eval_parser.add_argument("--first",
        type = int,
        help = "rank of first item")
    eval_parser.add_argument("--library",
        help = "library name")
    eval_parser.add_argument("query",
        help = "XQuery expression")
    eval_parser.set_defaults(handler = eval)

    # get subcommand
    get_parser = subparsers.add_parser("get",
        help = "retrieve a document or collection listing")
    get_parser.add_argument("--library",
        help = "library name")
    get_parser.add_argument("path",
        help = "path of document or collection")
    get_parser.set_defaults(handler = get)

    # put subcommand
    put_parser = subparsers.add_parser("put",
        help = "upload a document")
    put_parser.add_argument("--nonxml",
        action = "store_true",
        default = False,
        help = "store as non-xml data")
    put_parser.add_argument("--library",
        help = "library name")
    put_parser.add_argument("src",
        help = "source path ('-' for standard input)")
    put_parser.add_argument("dst",
        help = "destination path")
    put_parser.set_defaults(handler = put)

    # mput subcommand
    mput_parser = subparsers.add_parser("mput",
        help = "upload multiple documents")
    mput_parser.add_argument("--nonxml",
        action = "store_true",
        default = False,
        help = "store as non-xml data")
    mput_parser.add_argument("--library",
        help = "library name")
    mput_parser.add_argument("--collection",
        default = "",
        help = "destination collection")
    mput_parser.add_argument("paths",
        nargs = "+",
        metavar = "path",
        help = "source paths")
    mput_parser.set_defaults(handler = mput)

    # mkcol subcommand
    mkcol_parser = subparsers.add_parser("mkcol",
        help = "make a document collection")
    mkcol_parser.add_argument("--parents",
        action = "store_true",
        default = False,
        help = "create parents")
    mkcol_parser.add_argument("--library",
        help = "library name")
    mkcol_parser.add_argument("path",
        help = "collection path")
    mkcol_parser.set_defaults(handler = mkcol)

    # move subcommand
    move_parser = subparsers.add_parser("move",
        help = "move a document or collection")
    move_parser.add_argument("--library",
        help = "library name")
    move_parser.add_argument("src",
        help = "source path")
    move_parser.add_argument("dst",
        help = "source path")
    move_parser.set_defaults(handler = move)

    # copy subcommand
    copy_parser = subparsers.add_parser("copy",
        help = "copy a document or collection")
    copy_parser.add_argument("--library",
        help = "library name")
    copy_parser.add_argument("src",
        help = "source path")
    copy_parser.add_argument("dst",
        help = "source path")
    copy_parser.set_defaults(handler = copy)

    # delete subcommand
    delete_parser = subparsers.add_parser("delete",
        help = "delete a document or collection")
    delete_parser.add_argument("--library",
        help = "library name")
    delete_parser.add_argument("path",
        help = "document or collection")
    delete_parser.set_defaults(handler = delete)

    # getprop subcommand
    getprop_parser = subparsers.add_parser("getprop",
        help = "get document or collection properties")
    getprop_parser.add_argument("--depth",
        type = int,
        default = 0,
        help = "depth to descend to in a collection")
    getprop_parser.add_argument("--library",
        help = "library name")
    getprop_parser.add_argument("path",
        help = "document or collection")
    getprop_parser.add_argument("names",
        nargs = "*",
        metavar = "name",
        help = "property names (all by default)")
    getprop_parser.set_defaults(handler = getprop)

    # setprop subcommand
    setprop_parser = subparsers.add_parser("setprop",
        help = "set document or collection properties")
    setprop_parser.add_argument("--library",
        help = "library name")
    setprop_parser.add_argument("path",
        help = "document or collection")
    setprop_parser.add_argument("name",
        help = "property name")
    setprop_parser.add_argument("value",
        nargs = "?",
        help = "property value")
    setprop_parser.add_argument("type",
        nargs = "?",
        choices = ["string", "boolean", "integer",
            "double", "dateTime", "node()", "<expression>"],
        help = "property type")
    setprop_parser.set_defaults(handler = setprop)

    # queryprop subcommand
    queryprop_parser = subparsers.add_parser("queryprop",
        help = "query document or collection properties")
    queryprop_parser.add_argument("--path",
        help = "collection restriction")
    queryprop_parser.add_argument("--library",
        help = "library name")
    queryprop_parser.add_argument("query",
        help = "expression specifying documents or collections")
    queryprop_parser.add_argument("names",
        nargs = "*",
        metavar = "name",
        help = "names of properties ('path' and 'nature' by default)")
    queryprop_parser.set_defaults(handler = queryprop)

    # listlib subcommand
    listlib_parser = subparsers.add_parser("listlib",
        help = "list XML libraries")
    listlib_parser.set_defaults(handler = listlib)

    # server subcommand
    server_parser = subparsers.add_parser("server",
        help = "server control")
    server_parser.add_argument("command",
        choices = ["status", "online", "offline", "reload"],
        help = "command")
    server_parser.set_defaults(handler = server)

    # mklib subcommand
    mklib_parser = subparsers.add_parser("mklib",
        help = "create an XML library")
    mklib_parser.add_argument("name",
        help = "name of library")
    mklib_parser.set_defaults(handler = mklib)

    # dellib subcommand
    dellib_parser = subparsers.add_parser("dellib",
        help = "delete an XML library")
    dellib_parser.add_argument("name",
        help = "name of library")
    dellib_parser.set_defaults(handler = dellib)

    # reindex subcommand
    reindex_parser = subparsers.add_parser("reindex",
        help = "reindex an XML library")
    reindex_parser.add_argument("--library",
        help = "library name")
    reindex_parser.set_defaults(handler = reindex)

    # optimize subcommand
    optimize_parser = subparsers.add_parser("optimize",
        help = "optimize an XML library")
    optimize_parser.add_argument("--library",
        help = "library name")
    optimize_parser.set_defaults(handler = optimize)

    # backup subcommand
    backup_parser = subparsers.add_parser("backup",
        help = "backup libraries")
    backup_parser.add_argument("--library",
        default = "*",
        help = "library name (default '*' = all libraries)")
    backup_parser.add_argument("--exclusive",
        action = "store_true",
        default = False,
        help = "abort if a backup is already running")
    backup_parser.add_argument("--wait",
        action = "store_true",
        default = False,
        help = "wait for backup to finish")
    backup_parser.add_argument("directory",
        help = "server side directory")
    backup_parser.set_defaults(handler = backup)

    # progress subcommand
    progress_parser = subparsers.add_parser("progress",
        help = "progress of long task")
    progress_parser.add_argument("id",
        help = "progress identifier")
    progress_parser.set_defaults(handler = progress)

    # getindexing subcommand
    getindexing_parser = subparsers.add_parser("getindexing",
        help = "get indexing specification")
    getindexing_parser.add_argument("--library",
        help = "library name")
    getindexing_parser.set_defaults(handler = getindexing)

    # setindexing subcommand
    setindexing_parser = subparsers.add_parser("setindexing",
        help = "set indexing specification")
    setindexing_parser.add_argument("--library",
        help = "library name")
    setindexing_parser.add_argument("path",
        nargs = "?",
        help = "specification file")
    setindexing_parser.set_defaults(handler = setindexing)

    # getacl subcommand
    getacl_parser = subparsers.add_parser("getacl",
        help = "get access control information")
    getacl_parser.add_argument("--scope",
        choices = ["local", "inherit"],
        help = "request scope")
    getacl_parser.add_argument("--library",
        help = "library name")
    getacl_parser.add_argument("path",
        help = "path of library member")
    getacl_parser.set_defaults(handler = getacl)

    # setacl subcommand
    setacl_parser = subparsers.add_parser("setacl",
        help = "set access control specification")
    setacl_parser.add_argument("--library",
        help = "library name")
    setacl_parser.add_argument("path",
        nargs = "?",
        help = "specification file")
    setacl_parser.set_defaults(handler = setacl)

    # getconfig subcommand
    getconfig_parser = subparsers.add_parser("getconfig",
        help = "get configuration properties")
    getconfig_parser.add_argument("--level",
        choices = ["admin", "expert"],
        help = "configuration level")
    getconfig_parser.set_defaults(handler = getconfig)

    changeconfig_parser = subparsers.add_parser("changeconfig",
        help = "change configuration property")
    changeconfig_parser.add_argument("name",
        help = "property name")
    changeconfig_parser.add_argument("value",
        help = "property value")
    changeconfig_parser.set_defaults(handler = changeconfig)

    # changeconfig subcommand

    # getstats subcommand
    getstats_parser = subparsers.add_parser("getstats",
        help = "get server statistics")
    getstats_parser.add_argument("--level",
        default = "admin",
        choices = ["admin", "expert"],
        help = "level of detail")
    getstats_parser.set_defaults(handler = getstats)

    # listtasks subcommand
    listtasks_parser = subparsers.add_parser("listtasks",
        help = "list maintenance tasks")
    listtasks_parser.add_argument("--timeline", type = int, default = 0,
        help = "maximum age in hours of past tasks (0 == current)")
    listtasks_parser.set_defaults(handler = listtasks)

    # listqueries subcommand
    listqueries_parser = subparsers.add_parser("listqueries",
        help = "list running queries")
    listqueries_parser.set_defaults(handler = listqueries)

    # wait subcommand
    wait_parser = subparsers.add_parser("wait",
        help = "wait for a long task to complete")
    wait_parser.add_argument("--id",
        help = "progress identifier")
    wait_parser.add_argument("--timeout",
        help = "maximum seconds to wait")
    wait_parser.add_argument("--poll",
        help = "seconds between polling for task completion")
    wait_parser.set_defaults(handler = wait)

    # parse arguments and call handler
    if len(argv) < 2:
        parser.print_usage()
    else:
        args = parser.parse_args(argv[1:])
        client = Client(args.url)
        args.handler(client, args)

if __name__ == '__main__':
    main()
