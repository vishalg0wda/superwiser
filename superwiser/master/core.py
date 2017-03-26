import math
from collections import defaultdict
from operator import attrgetter, itemgetter

from superwiser.common.parser import calculate_delta, extract_conf_from_parsed
from superwiser.common.parser import build_section_from_program
from superwiser.common.parser import extract_section, update_section
from superwiser.common.parser import get_program_from_section, list_proc_tuples
from superwiser.common.log import logger


class BaseConf(object):
    def __init__(self):
        self.parsed = None

    def set_parsed(self, parsed):
        self.parsed = parsed

    def add_section(self, section_name, section_body):
        update_section(
            self.parsed,
            section_name,
            section_body)

    def remove_section(self, section_name):
        self.parsed.remove_section(section_name)

    def get_program_body(self, program_name):
        return extract_section(
            self.parsed,
            build_section_from_program(program_name))


class Distributor(object):
    def __init__(self, base_conf):
        self.nodes = []
        self.base_conf = base_conf

    def add_node(self, node):
        self.nodes.append(node)

    def remove_node(self, node_name):
        self.nodes.remove(
            next(
                node for node in self.nodes
                if node.name == node_name))

    def get_node(self, node_name):
        return (n for n in self.nodes if n.name == node_name).next()

    def synchronize_nodes(self, callback):
        for node in self.nodes:
            if node.is_dirty:
                conf = extract_conf_from_parsed(node.parsed)
                # TODO: Do whatever with conf
                callback(node, conf)
                node.is_dirty = False

    def calculate_assignable_loads(self):
        node_loads = sum(node.load for node in self.nodes)
        mean_load = float(node_loads) / len(self.nodes)
        assignable_loads = []
        can_floor = False
        for node in self.nodes:
            assignable_load = mean_load - node.load
            if not assignable_load.is_integer():
                if not can_floor:
                    assignable_load = math.ceil(assignable_load)
                    can_floor = True
                else:
                    assignable_load = math.floor(assignable_load)
                    can_floor = False
            assignable_loads.append((node.name, assignable_load))
        # sort in the order of nodes with most assignable load
        assignable_loads = sorted(
            assignable_loads,
            key=itemgetter(1),
            reverse=True)
        return assignable_loads

    def rlighten(self, load_tuples, excess_load, churned):
        if not load_tuples or excess_load <= 0:
            return churned
        tup = load_tuples[0]
        name, nprocs, weight = tup
        load = math.ceil(nprocs * weight)
        remaining_load = excess_load - load
        if remaining_load > 0:
            churned[name] += nprocs
            return self.rlighten(load_tuples[1:], remaining_load, churned)
        if remaining_load < 0:
            if nprocs > 1 and weight <= excess_load:
                churned[name] += 1
                load_tuples[0] = (name, nprocs - 1, weight)
                return self.rlighten(
                    load_tuples, excess_load - weight, churned)
            return self.rlighten(load_tuples[1:], excess_load, churned)

    def get_relievable_programs(self, node, excess_load):
        prog_tuples = node.get_prog_tuples()
        prog_load_tuples = []
        # arrange programs in decreasing order of load
        for (program_name, numprocs) in prog_tuples:
            program = self.base_conf.get_program_body(program_name)
            program['numprocs'] = numprocs
            prog_weight = float(program.get('hs_weight', '1'))
            prog_load_tuples.append((program_name, numprocs, prog_weight))
        prog_load_tuples = sorted(prog_load_tuples,
                                  key=lambda(t): t[1] * t[2],
                                  reverse=True)

        # try and fill excess load by pulling out tuples
        out = self.rlighten(
            prog_load_tuples,
            excess_load,
            defaultdict(int))
        return out

    def calculate_program_load(self, program):
        weight = float(program.get('hs_weight', '1'))
        nprocs = int(program.get('numprocs', '1'))
        return weight * nprocs

    def rburden(self, prog_tuples, lazy_nodes):
        if not prog_tuples:
            return
        if not lazy_nodes:
            raise Exception('Pending work remaining')

        prog_name, numprocs = prog_tuples[0]
        pgm = self.base_conf.get_program_body(prog_name)
        pgm['numprocs'] = numprocs
        weight = float(pgm.get('hs_weight', '1'))
        node_name, load_deficit = lazy_nodes[0]
        node = self.get_node(node_name)

        prog_load = self.calculate_program_load(pgm)
        remaining_load = load_deficit - prog_load
        if remaining_load > 0:
            lazy_nodes[0] = (node_name, remaining_load)
            node.undertake(pgm)
            return self.rburden(prog_tuples[1:], lazy_nodes)
        if remaining_load < 0:
            if numprocs > 1 and weight <= load_deficit:
                pgm['numprocs'] = 1
                node.undertake(pgm)
                prog_tuples[0] = (prog_name, numprocs - 1)
                lazy_nodes[0] = (node_name, load_deficit - weight)
                return self.rburden(prog_tuples, lazy_nodes)

        node.undertake(pgm)
        return self.rburden(prog_tuples[1:], lazy_nodes[1:])

    def distribute(self):
        assignable_loads = self.calculate_assignable_loads()
        # segregate lazy and busy nodes
        lazy_nodes = [e for e in assignable_loads if e[1] > 0]
        busy_nodes = [e for e in assignable_loads if e[1] < 0]

        # relieve busy nodes
        allottables = defaultdict(int)
        for (node_name, excess_load) in busy_nodes:
            node = self.get_node(node_name)
            relievables = self.get_relievable_programs(node, -excess_load)
            for (k, v) in relievables.items():
                allottables[k] += v
                program = self.base_conf.get_program_body(k)
                program['numprocs'] = -v
                node.undertake(program)

        # burden lazy nodes
        self.rburden(allottables.items(), lazy_nodes)

    def distribute_conf(self, conf):
        for (program_name, numprocs) in list_proc_tuples(conf):
            program = self.base_conf.get_program_body(program_name)
            program['numprocs'] = numprocs
            self.add_program(program)

    def add_program(self, program):
        lazy_node = sorted(self.nodes, key=attrgetter('load'))[0]
        lazy_node.undertake(program)

    def remove_program(self, program_name):
        # identify nodes that are running this program
        for node in self.nodes:
            if node.has_program(program_name):
                node.relieve(program_name)

    def increase_procs(self, program_name, factor=1):
        lazy_node = sorted(self.nodes, key=attrgetter('load'))[0]
        program = self.base_conf.get_program_body(program_name)
        program['numprocs'] = factor
        lazy_node.undertake(program)

    def decrease_procs(self, program_name, factor=1):
        for node in self.nodes:
            if factor <= 0:
                break
            if node.has_program(program_name):
                program = node.get_program(program_name)
                deductable = program['numprocs'] - factor
                if deductable <= 0:
                    node.relieve(program_name)
                    factor -= program['numprocs']
                else:
                    program['numprocs'] = deductable
                    node.undertake(program)
                    factor = 0


class WNode(object):
    def __init__(self, name, parsed, base_conf):
        self.name = name
        self.parsed = parsed
        self.base_conf = base_conf

        self.is_dirty = False

    def get_prog_tuples(self):
        return list_proc_tuples(self.parsed)

    @property
    def load(self):
        total = 0.0
        for (program_name, numprocs) in self.get_prog_tuples():
            program = self.base_conf.get_program_body(program_name)
            weight = float(program.get('hs_weight', '1'))
            total += numprocs * weight
        return total

    def undertake(self, program):
        program = program.copy()
        program_name = program.pop('hs_program_name')
        if self.has_program(program_name):
            old_program = self.get_program(program_name)
            old_numprocs = int(old_program.get('numprocs', '1'))
            delta_numprocs = int(program.get('numprocs', '1'))
            new_numprocs = old_numprocs + delta_numprocs
            if new_numprocs <= 0:
                self.relieve(program_name)
            else:
                program['numprocs'] = new_numprocs
                update_section(
                    self.parsed,
                    build_section_from_program(program_name),
                    program)
        else:
            update_section(self.parsed,
                           build_section_from_program(program_name),
                           program)

        self.is_dirty = True

    def relieve(self, program_name):
        self.parsed.remove_section(
            build_section_from_program(program_name))

        self.is_dirty = True

    def flush(self, fp):
        if self.is_dirty:
            self.parsed.write(fp)
            self.is_dirty = False

    def has_program(self, program_name):
        return self.parsed.has_section(
            build_section_from_program(program_name))

    def get_program(self, program_name):
        return extract_section(
            self.parsed,
            build_section_from_program(program_name))


class EyeOfMordor(object):
    def __init__(self, base_conf, distributor, zk):
        self.base_conf = base_conf
        self.distributor = distributor
        self.zk = zk

    def update_conf(self, new_conf):
        delta = calculate_delta(self.base_conf.parsed, new_conf)
        for section in delta['added_sections']:
            # first add new section to base conf
            program = extract_section(new_conf, section)
            self.base_conf.add_section(section, program)
            # allot program across nodes
            self.distributor.add_program(program)
        for section in delta['removed_sections']:
            # remove program across nodes containing it
            self.distributor.remove_program(get_program_from_section(section))

        self.distributor.distribute()
        # Remove removed sections from base conf
        for section in delta['removed_sections']:
            self.base_conf.remove_section(section)

        self.zk.set_base_conf(extract_conf_from_parsed(new_conf))
        self.distributor.synchronize_nodes(self.zk.sync_node)

    def increase_procs(self, program_name, factor=1):
        status = self.distributor.increase_procs(program_name, factor)
        self.distributor.distribute()
        self.distributor.synchronize_nodes(self.zk.sync_node)
        return status

    def decrease_procs(self, program_name, factor=1):
        status = self.distributor.decrease_procs(program_name, factor)
        self.distributor.distribute()
        self.distributor.synchronize_nodes(self.zk.sync_node)
        return status

    def teardown(self):
        logger.info('tearing down the eye of mordor')
