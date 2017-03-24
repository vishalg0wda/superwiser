from superwiser.master.factory import BaseConfFactory, EyeOfMordorFactory
from superwiser.common.parser import *
from superwiser.master.core import *



def main():
    bc = BaseConfFactory().make_base_conf(parse_content(''))
    e = EyeOfMordorFactory().make_eye_of_mordor(bc)
    n1 = WNode('node1', parse_content(''), bc)
    n2 = WNode('node2', parse_content(''), bc)
    e.distributor.add_node(n1)
    e.distributor.add_node(n2)
    e.update_conf(parse_file('./supervisord_0.conf'))
    return e
