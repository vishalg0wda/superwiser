import sys

from twisted.python import log
from twisted.internet import protocol, reactor, endpoints
from twisted.protocols.basic import LineReceiver


log.startLogging(sys.stdout)


class Superwiser(LineReceiver):
    SUPPORTED_OPS = ["NOTIFY_SUPERVISOR_DOWN"]

    def lineReceived(self, line):
        log.msg(line)
        if line in self.SUPPORTED_OPS:
            log.msg("Handling {}".format(line))
            self.sendLine("Handled {}".format(line))
        else:
            self.sendLine("Unsupported operation")


class SuperwiserFactory(protocol.Factory):
    def buildProtocol(self, addr):
        return Superwiser()


endpoints.serverFromString(reactor, "tcp:1234").listen(SuperwiserFactory())

reactor.run()
