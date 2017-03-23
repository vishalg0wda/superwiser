from superwiser.master.factory import BaseConfFactory, DistributorFactory
from superwiser.common.parser import parse_file


def main():
    bc = BaseConfFactory().make_base_conf(parse_file('./supervisord_0.conf'))
    d = DistributorFactory().make_distributor(bc)
    return d


if __name__ == '__main__':
    main()
