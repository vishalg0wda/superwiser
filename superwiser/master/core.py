from kazoo.client import KazooClient
from kazoo.recipes.watches import DataWatch
from kazoo.protocl.states import EventType

from superwiser.common.log import logger
from superwiser.common.settings import ZK_HOST, ZK_PORT
from superwiser.master.settings import BASE_CONFIG
from superwiser.common.path import PathMaker
from superwiser.master.distribute import distribute_work
from superwiser.common.parser import manipulate_numprocs


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
        self.zk.ensure_path(self.path.node())
        self.zk.ensure_path(self.path.conf())

    def register_watches(self):
        logger.info('Registering watches')
        DataWatch(self.zk, self.path.conf(), self.on_conf_change)

    def on_conf_change(self, data, stat, event):
        if event and event.type == EventType.CHANGED:
            logger.info('Handling conf change')
            # Get toolchains
            toolchains = self.zk.get_children(self.path.toolchain())
            if toolchains:
                # Distribute conf across toolchains
                assigned_work = distribute_work(data, toolchains)
                for (toolchain, work) in assigned_work.items():
                    lock = self.zk.Lock(
                        self.path.toolchain(toolchain),
                        identifier='on-conf-change')
                    with lock:
                        self.zk.write(self.path.toolchain(toolchain), work)

    def set_conf(self, conf):
        logger.info('Updating conf')
        self.zk.write(self.path.conf(), conf)

    def get_conf(self, conf):
        logger.info('Getting conf')
        data, stat = self.zk.read(self.path.conf())
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
