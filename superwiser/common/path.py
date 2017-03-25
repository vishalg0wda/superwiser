from superwiser.settings import SERVICE_NAMESPACE


class PathMaker(object):
    def node(self, node_name=None):
        path = "/{}/wnode".format(SERVICE_NAMESPACE)
        if node_name is not None:
            path = "{}/{}".format(path, node_name)
        return path

    def nsync(self, node_name=None):
        path = "{}/sync".format(self.node())
        if node_name is not None:
            path = "{}/{}".format(path, node_name)
        return path

    def ncurrent(self, node_name=None):
        path = "{}/current".format(self.node())
        if node_name is not None:
            path = "{}/{}".format(path, node_name)
        return path

    def master(self):
        return "{}/master".format(SERVICE_NAMESPACE)

    def baseconf(self):
        return "{}/conf-base".format(self.master())

    def stateconf(self):
        return "{}/conf-state".format(self.master())
