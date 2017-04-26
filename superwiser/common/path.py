from superwiser.common.settings import SERVICE_NAMESPACE


class PathMaker(object):
    def namespace(self):
        return "/{}".format(SERVICE_NAMESPACE)

    def toolchain(self, name=None):
        path = "{}/toolchains".format(self.namespace())
        if name is not None:
            path = "{}/{}".format(path, name)
        return path

    def node(self, name=None):
        path = "{}/nodes".format(self.namespace())
        if name is not None:
            path = "{}/{}".format(path, name)
        return path

    def baseconf(self):
        return "{}/conf/base".format(self.namespace())

    def stateconf(self):
        return "{}/conf/state".format(self.namespace())

    def stateconfbkp(self):
        return "{}/conf/state-bkp".format(self.namespace())

    def namelock(self):
        return "{}/namelock".format(self.namespace())
