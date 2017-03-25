import requests
from twisted.internet import reactor
from twisted.internet.protocol import Protocol, Factory

from superwiser.common.parser import parse_content
from superwiser.master.factory import EyeOfMordorFactory, ZkClientFactory
from superwiser.settings import MASTER_PORT
from superwiser.common.log import logger


class Superwiser(Protocol):
    def dataReceived(self, data):
        # command: increase_procs(program_name, 1)
        try:
            cmd, rest = data.split('(')
        except:
            return
        rest = rest.strip().rstrip(')')
        if cmd == 'increase_procs':
            program_name, factor = rest.split(',')
            program_name = program_name.strip()
            factor = int(factor)
            result = self.factory.master.increase_procs(program_name, factor)
            self.transport.write(str(result) + '\n')
        elif cmd == 'decrease_procs':
            program_name, factor = rest.split(',')
            program_name = program_name.strip()
            factor = int(factor)
            result = self.factory.master.decrease_procs(program_name, factor)
            self.transport.write(str(result) + '\n')
        elif cmd == 'update_conf':
            # TODO: implement update conf
            conf = requests.get(rest).content
            result = self.factory.master.update_conf(parse_content(conf))
            self.transport.write(str(result) + '\n')
        else:
            self.transport.write('Invalid operation\n')

    def connectionMade(self):
        self.transport.write('Hello\n')

    def connectionLost(self):
        self.transport.write('Goodbye\n')


class SuperwiserFactory(Factory):
    protocol = Superwiser

    def __init__(self):
        self.master = EyeOfMordorFactory().make_eye_of_mordor()
        self.zk = ZkClientFactory().make_zk_client()

    def teardown(self):
        self.master.teardown()
        self.zk.teardown()


def start_server():
    factory = SuperwiserFactory()
    reactor.listenTCP(MASTER_PORT, factory)
    logger.info('starting server now')
    reactor.addSystemEventTrigger('before', 'shutdown', factory.teardown)
    reactor.run()
