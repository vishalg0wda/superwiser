import os
import requests
import xmlrpclib
from kazoo.client import KazooClient
from kazoo.recipe.watchers import DataWatch, ChildrenWatch
from kazoo.protocol.states import EventType
from twisted.internet import inotify, task
from twisted.python import filepath

from superwiser.common.log import logger
from superwiser.common.path import PathMaker
from superwiser.common.parser import manipulate_numprocs, build_conf
from superwiser.common.parser import parse_content, unparse, extract_section
from superwiser.common.parser import section_from_program
from superwiser.common.parser import extract_prog_tuples, extract_programs
from superwiser.master.distribute import distribute_work


class EyeOfMordor(object):
    def __init__(self, host, port, auto_redistribute, **kwargs):
        self.zk = KazooClient('{}:{}'.format(host, port))
        self.path = PathMaker()
        self.auto_redistribute = auto_redistribute
        self.node_drop_callbacks = kwargs.get('node_drop_callbacks', [])
        self.supervisor_down_callbacks = kwargs.get(
            'supervisor_down_callbacks', [])
        self.supervisor_poll_interval = kwargs.get(
            'supervisor_poll_interval', 15)
        self.toolchains = []
        self.setup()

    def setup(self):
        logger.info('Setting up the Eye of Mordor')
        self.zk.start()
        # Setup paths
        self.setup_paths()
        # Initializse children
        self.toolchains = self.zk.get_children(self.path.toolchain())
        # Register watches
        self.register_watches()
        # Setup looping call to poll for supervisor states
        self.setup_poller()

    def is_supervisor_running(self, host):
        logger.info('Polling supervisor')
        server = xmlrpclib.Server(
            'http://sauron:lotr@{}:9001/RPC2'.format(host))
        try:
            state = server.supervisor.getState()
            return state['statecode'] == 1
        except:
            return False

    def setup_poller(self):
        logger.info('Setting up poller')

        def poller():
            hosts = [self.get_orc_ip(orc) for orc in self.list_orcs()]
            # Iterate over every orc and poll for supervisor state
            for host in hosts:
                if not self.is_supervisor_running(host):
                    logger.warn('Supervisor on {} is not running'.format(host))
                    # Hit the registered callback to indicate that
                    # supervisor is not running
                    for cb in self.supervisor_down_callbacks:
                        requests.post(
                            cb['url'],
                            json={
                                'host': host,
                                'event': 'supervisor_down',
                            },
                            headers={
                                'Authorization': cb.get('auth_token', '')
                            })

        loop = task.LoopingCall(poller)
        loop.start(self.supervisor_poll_interval)

    def setup_paths(self):
        logger.info('Setting up paths')
        self.zk.ensure_path(self.path.toolchain())
        self.zk.ensure_path(self.path.node())
        self.zk.ensure_path(self.path.baseconf())
        self.zk.ensure_path(self.path.stateconf())
        self.zk.ensure_path(self.path.stateconfbkp())

    def register_watches(self):
        logger.info('Registering watches')
        DataWatch(self.zk, self.path.baseconf(), self.on_base_conf_change)
        DataWatch(self.zk, self.path.stateconf(), self.on_state_conf_change)
        ChildrenWatch(
            self.zk, self.path.toolchain(),
            self.on_toolchains, send_event=True)

    def _distribute(self, work, toolchains):
        # Distribute conf across toolchains
        assigned_work = distribute_work(work, toolchains)
        for (toolchain, awork) in assigned_work.items():
            self.zk.set(
                self.path.toolchain(toolchain),
                unparse(build_conf(awork, parse_content(work))))

    def distribute(self):
        self._distribute(
            self.get_state_conf(),
            self.zk.get_children(self.path.toolchain()))

    def on_base_conf_change(self, data, stat, event):
        if event and event.type == EventType.CHANGED:
            logger.info('Handling base conf change')
            state_programs = {
                name: (numprocs, weight)
                for name, numprocs, weight in extract_prog_tuples(
                        parse_content(self.get_state_conf()))}
            base_conf = parse_content(data)
            base_tuples = extract_prog_tuples(base_conf)
            # Rebuild state conf
            prog_tuples = []
            for (program_name, numprocs, weight) in base_tuples:
                tup = (program_name, )
                if program_name in state_programs:
                    tup += state_programs[program_name]
                else:
                    tup += (numprocs, weight)

                prog_tuples.append(tup)
            # Trigger distribute
            self.set_state_conf(unparse(build_conf(prog_tuples, base_conf)))

    def on_state_conf_change(self, data, stat, event):
        if event and event.type == EventType.CHANGED:
            logger.info('Handling state conf change')
            # Get toolchains
            toolchains = self.zk.get_children(self.path.toolchain())
            if toolchains:
                # Distribute work across toolchains
                self._distribute(data, toolchains)

    def on_toolchains(self, children, event):
        if not event:
            return
        added = set(children) - set(self.toolchains)
        removed = set(self.toolchains) - set(children)
        if added:
            logger.info('Toolchain joined')
            self.distribute()
        elif removed:
            logger.info('Toolchain left')
            # Hit callbacks
            for cb in self.node_drop_callbacks:
                requests.post(cb['url'],
                              json={
                                  'node_count': len(children),
                                  'event': 'toolchain_dropped',
                              },
                              headers={
                                  'Authorization': cb.get('auth_token', ''),
                              })
            if self.auto_redistribute:
                self.distribute()
        self.toolchains = children

    def get_base_conf(self):
        logger.info('Getting base conf')
        data, _ = self.zk.get(self.path.baseconf())
        return data

    def set_base_conf(self, conf):
        logger.info('Updating base conf')
        self.zk.set(self.path.baseconf(), conf)

    def get_state_conf(self):
        logger.info('Getting state conf')
        data, _ = self.zk.get(self.path.stateconf())
        return data

    def set_state_conf(self, conf):
        logger.info('Updating state conf')
        self.zk.set(self.path.stateconf(), conf)

    def teardown(self):
        logger.info('Tearing down the eye of Mordor!')
        self.zk.stop()
        self.zk.close()

    def list_orcs(self):
        return self.zk.get_children(self.path.toolchain())

    def get_orc_ip(self, orc):
        return self.zk.get(self.path.node(orc))[0]

    def list_processes_for_orc(self, orc):
        return extract_prog_tuples(
            parse_content(self.zk.get(self.path.toolchain(orc))[0]))

    def list_processes(self):
        # Build list of processes for each orc
        procs = []
        for orc in self.list_orcs():
            procs.extend(self.list_processes_for_orc(orc))

        # Remap tuples with program body
        parsed = parse_content(self.get_state_conf())
        out = {}
        for proc in procs:
            out[proc[0]] = extract_section(
                parsed,
                section_from_program(proc[0]))

        return out

    def backup_state(self):
        logger.info('Backing up state')
        self.zk.set(
            self.path.stateconfbkp(),
            self.get_state_conf())

    def restore_state(self):
        logger.info('Restoring state')
        self.set_state_conf(
            self.zk.get(self.path.stateconfbkp())[0])

    def get_stopped_processes(self):
        base_tups = extract_prog_tuples(
            parse_content(self.get_base_conf()))
        state_tups = extract_prog_tuples(
            parse_content(self.get_state_conf()))
        base_progs = set(ele[0] for ele in base_tups)
        state_progs = set(ele[0] for ele in state_tups)
        stopped_progs = (base_progs - state_progs)
        base_conf = extract_programs(parse_content(self.get_base_conf()))
        out = []
        for (name, body) in base_conf.items():
            if name in stopped_progs:
                body['name'] = name
                out.append(body)
        return out


class Sauron(object):
    def __init__(self, eye, conf, override_state, conf_path):
        self.eye = eye
        self.conf = conf
        self.override_state = override_state
        self.conf_path = conf_path
        self.notifier = None
        self.setup()

    def setup(self):
        logger.info('Setting up Sauron')
        if self.override_state:
            # Override previous state of conf
            self.eye.set_state_conf(self.conf)
        else:
            # Merge provided conf with state conf
            self.eye.set_base_conf(self.conf)
        # Register File Watcher
        notifier = inotify.INotify()
        notifier.startReading()
        notifier.watch(filepath.FilePath(
            self.conf_path), callbacks=[self.on_conf_change])
        self.notifer = notifier

    def on_conf_change(self, ignore, fp, mask):
        """Callback to be invoked when the file at config path changes."""
        if mask == inotify.IN_DELETE_SELF:
            logger.info('Handling conf path change')
            self.notifer.stopReading()
            notifier = inotify.INotify()
            notifier.startReading()
            notifier.watch(filepath.FilePath(fp.path),
                           callbacks=[self.on_conf_change])
            self.notifer = notifier
            with fp.open() as conf:
                self.eye.set_base_conf(conf.read())
        elif mask == inotify.IN_MODIFY:
            with fp.open() as conf:
                self.eye.set_base_conf(conf.read())

    def increase_procs(self, program_name, factor=1):
        logger.info('Increasing procs')

        def adder(x):
            return x + factor

        new_conf = manipulate_numprocs(
            parse_content(self.eye.get_state_conf()),
            program_name,
            adder)
        # Simply set the conf to trigger a distribute and sync
        self.eye.set_state_conf(new_conf)

        return True

    def decrease_procs(self, program_name, factor=1):
        logger.info('Decreasing procs')

        def subtractor(x):
            return x - factor

        new_conf = manipulate_numprocs(
            parse_content(self.eye.get_state_conf()),
            program_name,
            subtractor)
        # Simply set the conf to trigger a distribute and sync
        self.eye.set_state_conf(new_conf)

        return True

    def start_program(self, program_name):
        prog_tuples = extract_prog_tuples(
            parse_content(self.eye.get_state_conf()))
        has_program = any(ele for ele in prog_tuples
                          if ele[0] == program_name)
        if has_program:
            logger.info('Already contains program')
            return False
        base_conf_tuples = extract_prog_tuples(
            parse_content(self.eye.get_base_conf()))
        try:
            program_tuple = next(ele for ele in base_conf_tuples
                                 if ele[0] == program_name)
        except StopIteration:
            logger.info('Program does not exist')
            return False
        # Program did not exist, let's add it now
        # Note: We set numprocs to one while adding
        prog_tuples.append(program_tuple)
        # Update conf and distribute
        self.eye.set_state_conf(
            unparse(
                build_conf(
                    prog_tuples,
                    parse_content(self.eye.get_base_conf()))))
        return True

    def stop_program(self, program_name):
        logger.info('Stopping program')
        conf = parse_content(self.eye.get_state_conf())
        # Check if program exists
        prog_tuples = extract_prog_tuples(conf)
        for (pos, (prog_name, _, _)) in enumerate(prog_tuples):
            if prog_name == program_name:
                del prog_tuples[pos]
                break
        else:
            logger.info('Program is either already stopped/not defined')
            return False
        self.eye.set_state_conf(
            unparse(
                build_conf(
                    prog_tuples,
                    conf)))
        return True

    def restart_program(self, program_name):
        self.stop_program(program_name)
        return self.start_program(program_name)

    def restart_all_programs(self):
        logger.info('Restarting all programs')
        self.stop_all_programs()
        return self.start_all_programs()

    def was_stopped(self):
        return self.eye.get_state_conf() == ''

    def stop_all_programs(self):
        """We copy state conf onto a backup path first, truncate state conf
        and distribute.
        """
        logger.info('Stopping all programs')
        self.eye.backup_state()
        # Now truncate state conf, which triggers a distribute.
        self.eye.set_state_conf('')
        return True

    def start_all_programs(self):
        logger.info('Starting all programs')
        # If state already contains conf, then do not override it
        state_conf = self.eye.get_state_conf()
        if state_conf != '':
            logger.info('State conf may be overidden. Skipping')
            return False
        # Restore the backup state, which triggers a distribute
        self.eye.restore_state()
        return True

    def teardown(self):
        logger.info('Tearing down Sauron')
        self.eye.teardown()


class SauronFactory(object):
    _instance = None

    def make_sauron(self, **kwargs):
        inst = SauronFactory._instance
        if inst is None:
            logger.info('Making Sauron')

            zk_host = kwargs['zookeeper_host']
            zk_port = kwargs['zookeeper_port']
            auto_redistribute = kwargs['auto_redistribute_on_failure']
            node_drop_callbacks = kwargs.get('node_drop_callbacks', [])
            supervisor_down_callbacks = kwargs.get(
                'supervisor_down_callbacks', [])
            conf_path = kwargs['supervisor_conf']
            override_state = kwargs['override_state']
            supervisor_poll_interval = kwargs['supervisor_poll_interval']

            if not os.path.exists(conf_path):
                raise Exception('Supervisor conf does not exist')

            supervisor_conf = self.read_conf(conf_path)

            inst = Sauron(
                EyeOfMordor(
                    host=zk_host,
                    port=zk_port,
                    auto_redistribute=auto_redistribute,
                    node_drop_callbacks=node_drop_callbacks,
                    supervisor_down_callbacks=supervisor_down_callbacks,
                    supervisor_poll_interval=supervisor_poll_interval),
                supervisor_conf,
                override_state,
                conf_path)

            SauronFactory._instance = inst

        return inst

    def read_conf(self, path):
        logger.info('Reading conf')
        with open(path) as source:
            return source.read()
