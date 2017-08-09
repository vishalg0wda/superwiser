import xmlrpclib


class TimeoutTransport(xmlrpclib.Transport):

    def __init__(self, timeout, use_datetime=0):
        self.timeout = timeout
        # xmlrpclib uses old-style classes so we cannot use super()
        xmlrpclib.Transport.__init__(self, use_datetime)

    def make_connection(self, host):
        connection = xmlrpclib.Transport.make_connection(self, host)
        connection.timeout = self.timeout
        return connection


class TimeoutServerProxy(xmlrpclib.ServerProxy):
    def __init__(self, uri, timeout=10, transport=None, encoding=None, verbose=0, allow_none=0, use_datetime=0):
        t = TimeoutTransport(timeout)
        xmlrpclib.ServerProxy.__init__(self, uri, t, encoding, verbose, allow_none, use_datetime)
