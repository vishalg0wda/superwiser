import optparse

import requests
from twisted.internet import reactor
from twisted.internet.protocol import Protocol, Factory

from superwiser.master.factory import SauronFactory
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
                factor = factor.strip().rstrip(')')
                factor = int(factor)
                args = [prog, factor]
            elif cmd in ['update_conf', 'stop_program',
                         'start_program', 'restart_program']:
                rest = rest.strip().rstrip(')')
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
        elif cmd == 'stop_program':
            result = self.overlord.stop_program(*args)
            self.transport.write(str(result) + '\n')
        elif cmd == 'start_program':
            result = self.overlord.start_program(*args)
            self.transport.write(str(result) + '\n')
        elif cmd == 'restart_program':
            result = self.overlord.restart_program(*args)
            self.transport.write(str(result) + '\n')

    def connectionMade(self):
        self.transport.write('Hello\n')

    def connectionLost(self, reason):
        self.transport.write('Goodbye\n')


class SuperwiserFactory(Factory):
    def __init__(self, overlord):
        self.overlord = overlord

    def buildProtocol(self, addr):
        prot = Superwiser()
        prot.overlord = self.overlord
        return prot

    def teardown(self):
        self.overlord.teardown()


def parse_opts():
    parser = optparse.OptionParser()
    parser.add_option('-c', '--config', dest='conf_path')
    parser.add_option('--zk-host')
    parser.add_option('--zk-port')
    options, _ = parser.parse_args()
    return options.__dict__


def start_server():
    factory = SuperwiserFactory(
        SauronFactory().make_sauron(**parse_opts()))
    reactor.listenTCP(MASTER_PORT, factory)
    logger.info('Starting server now')
    reactor.addSystemEventTrigger('before', 'shutdown', factory.teardown)
    reactor.run()
