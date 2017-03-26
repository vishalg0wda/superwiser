from kazoo.client import KazooClient
from kazoo.recipe.watchers import DataWatch, ChildrenWatch
from kazoo.protocol.states import EventType

from superwiser.common.log import logger
from superwiser.common.settings import ZK_HOST, ZK_PORT
from superwiser.master.settings import BASE_CONFIG
from superwiser.common.path import PathMaker
from superwiser.master.distribute import distribute_work
from superwiser.common.parser import manipulate_numprocs, build_conf
from superwiser.common.parser import parse_content, unparse


class EyeOfMordor(object):
    def __init__(self):
        self.zk = KazooClient('{}:{}'.format(ZK_HOST, ZK_PORT))
        self.path = PathMaker()
        self.setup()

    def setup(self):
        logger.info('Setting up the Eye of Mordor')
        self.zk.start()
        # Setup paths
        self.setup_paths()
        # Register watches
        self.register_watches()
        # Set base conf
        with open(BASE_CONFIG) as source:
            self.set_conf(source.read())

    def setup_paths(self):
        logger.info('Setting up paths')
        self.zk.ensure_path(self.path.toolchain())
        self.zk.ensure_path(self.path.conf())

    def register_watches(self):
        logger.info('Registering watches')
        DataWatch(self.zk, self.path.conf(), self.on_conf_change)
        ChildrenWatch(
            self.zk, self.path.toolchain(),
            self.on_toolchains, send_event=True)

    def distribute(self, work, toolchains):
        # Distribute conf across toolchains
        assigned_work = distribute_work(work, toolchains)
        for (toolchain, awork) in assigned_work.items():
            self.zk.set(
                self.path.toolchain(toolchain),
                unparse(build_conf(awork, parse_content(work))))

    def on_conf_change(self, data, stat, event):
        if event and event.type == EventType.CHANGED:
            logger.info('Handling conf change')
            # Get toolchains
            children = self.zk.get_children(self.path.toolchain())
            toolchains = [ele.split('/')[-1] for ele in children]
            if toolchains:
                # Distribute work across toolchains
                self.distribute(data, toolchains)

    def on_toolchains(self, children, event):
        if event:
            logger.info('Toolchains joined/left')
            if children:
                # Distribute work across toolchains
                work = self.get_conf()
                self.distribute(work, children)

    def set_conf(self, conf):
        logger.info('Updating conf')
        self.zk.set(self.path.conf(), conf)

    def get_conf(self):
        logger.info('Getting conf')
        data, stat = self.zk.get(self.path.conf())
        return data

    def teardown(self):
        logger.info('Tearing down the eye of Mordor!')
        self.zk.stop()
        self.zk.close()


class Sauron(object):
    def __init__(self):
        self.eye = EyeOfMordor()

    def update_conf(self, conf):
        logger.info('Updating conf')
        self.eye.set_conf(conf)

    def increase_procs(self, program_name, factor=1):
        logger.info('Increasing procs')

        def adder(x):
            return x + factor

        new_conf = manipulate_numprocs(
            self.eye.get_conf(),
            program_name,
            adder)
        # Simply set the conf to trigger a distribute and sync
        self.eye.set_conf(new_conf)

    def decrease_procs(self, program_name, factor=1):
        logger.info('Decreasing procs')

        def subtractor(x):
            return x - factor

        new_conf = manipulate_numprocs(
            self.eye.get_conf(),
            program_name,
            subtractor)
        # Simply set the conf to trigger a distribute and sync
        self.eye.set_conf(new_conf)

    def teardown(self):
        logger.info('Tearing down Sauron')
        self.eye.teardown()
