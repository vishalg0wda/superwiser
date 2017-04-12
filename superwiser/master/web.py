import datetime

from twisted.web.server import Site
from twisted.web.resource import Resource

from superwiser.common.jinja_manager import JinjaTemplateManager
from superwiser.master.factory import SauronFactory


class SuperwiserHome(Resource):
    isLeaf = True

    def __init__(self):
        self.template_manager = JinjaTemplateManager()
        self.sauron = SauronFactory.get_sauron()
        #super(SuperwiserHome, self).__init__(self)

    def get_all_node_procs(self):
        all_procs = []

        nodes = self.sauron.eye.list_nodes()
        for node in nodes:
            procs = self.sauron.eye.list_processes(node)
            node_ip = self.sauron.eye.get_node_details(node)
            for proc in procs:
                proc_details = {}
                proc_details.update(procs[proc])
                proc_details['name'] = proc
                proc_details['node'] = node_ip
                all_procs.append(proc_details)
        return all_procs

    def render_GET(self, request):
        context = {'all_procs': self.get_all_node_procs()}
        return self.template_manager.render_template('index.html',
                                                     context)


def get_site_root():
    root = SuperwiserHome()
    site = Site(root)
    return site
