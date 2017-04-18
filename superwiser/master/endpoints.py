import requests
from collections import defaultdict

from twisted.web.server import Site
from twisted.web.resource import Resource
from twisted.internet.protocol import Protocol, Factory

from superwiser.common.jinja_manager import JinjaTemplateManager


class SuperwiserHome(Resource):
    isLeaf = True

    def __init__(self, sauron):
        self.template_manager = JinjaTemplateManager()
        self.sauron = sauron

    def get_process_states(self):
        # Collect all node details
        orcs = [
            {
                'name': orc,
                'node_ip': self.sauron.eye.get_orc_ip(orc),
            } for orc in self.sauron.eye.list_orcs()]

        # Map process to all orcs that it is running in
        proc_orc_map = defaultdict(list)
        for orc in orcs:
            procs = self.sauron.eye.list_processes_for_orc(orc['name'])
            for (proc_name, _, _) in procs:
                proc_orc_map[proc_name].append(orc)

        # Collect all process details
        processes = [
            {
                'name': name,
                'command': ele['command'],
                'nodes': proc_orc_map[name],
                'numprocs': ele['numprocs'],
                'weight': ele['weight'],
                'running': True,
            } for (name, ele) in self.sauron.eye.list_processes().items()]
        # Include all stopped processes as well
        stopped_processes = self.sauron.eye.get_stopped_processes()
        # Patch it to include nodes and running keys
        for proc in stopped_processes:
            proc['nodes'] = []
            proc['running'] = False
        # Concatenate the two lists
        processes.extend(stopped_processes)

        return processes, orcs

    def render_GET(self, request):
        processes, orcs = self.get_process_states()
        context = {'all_procs': processes,
                   'all_nodes': orcs}
        return self.template_manager.render_template('index.html',
                                                     context)


class SuperwiserWebFactory(object):
    def make_site(self, sauron):
        root = SuperwiserHome(sauron)
        site = Site(root)
        return site


class SuperwiserTCP(Protocol):
    def parse_command(self, data):
        cmd, args = None, []
        try:
            splits = data.split('(')
            if len(splits) > 1:
                cmd, rest = splits
            else:
                cmd = data.strip()
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
        elif cmd in ['quit', 'exit']:
            self.transport.write('Goodbye\n')
            self.transport.loseConnection()

    def connectionMade(self):
        self.transport.write("It's time to be super wiser\n")


class SuperwiserTCPFactory(Factory):
    def __init__(self, overlord):
        self.overlord = overlord

    def buildProtocol(self, addr):
        prot = SuperwiserTCP()
        prot.overlord = self.overlord
        return prot
