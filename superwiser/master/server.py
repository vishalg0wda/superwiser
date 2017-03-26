import requests
from twisted.internet import reactor
from twisted.internet.protocol import Protocol, Factory

from superwiser.master.core import Sauron
from superwiser.master.settings import MASTER_PORT
from superwiser.common.log import logger


class Superwiser(Protocol):
    def parse_command(self, data):
        cmd, args = None, []
        try:
            cmd, rest = data.split('(')
            if cmd in ['increase_procs', 'decrease_procs']:
                prog, factor = rest.split(',')
                prog = prog.strip()
                factor = int(factor)
                args = [prog, factor]
            elif cmd in ['update_conf']:
                args = [rest]
        except:
            pass

        return (cmd, args)

    def dataReceived(self, data):
        cmd, args = self.parse_command(data)
        if cmd is None:
            self.transport.write('Invalid operation\n')
        elif cmd == 'increase_procs':
            result = self.overlord.increase_procs(*args)
            self.transport.write(str(result) + '\n')
        elif cmd == 'decrease_procs':
            result = self.overlord.decrease_procs(*args)
            self.transport.write(str(result) + '\n')
        elif cmd == 'update_conf':
            conf = requests.get(*args).content
            result = self.overlord.update_conf(conf)
            self.transport.write(str(result) + '\n')

    def connectionMade(self):
        self.transport.write('Hello\n')

    def connectionLost(self, reason):
        self.transport.write('Goodbye\n')


class SuperwiserFactory(Factory):
    def __init__(self):
        self.overlord = Sauron()

    def buildProtocol(self, addr):
        prot = Superwiser()
        prot.overlord = self.overlord
        return prot

    def teardown(self):
        self.overlord.teardown()


def start_server():
    factory = SuperwiserFactory()
    reactor.listenTCP(MASTER_PORT, factory)
    logger.info('Starting server now')
    reactor.addSystemEventTrigger('before', 'shutdown', factory.teardown)
    reactor.run()
