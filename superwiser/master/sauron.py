from superwiser.common.parser import manipulate_numprocs
from superwiser.common.log import logger
from superwiser.master.eye import EyeOfMordor


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
