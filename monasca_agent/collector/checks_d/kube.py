"""kubernetes check
Collects metrics from cAdvisor instance
"""
# stdlib
import numbers
from fnmatch import fnmatch
import re
import traceback

# 3rd party
import urllib
import json

# project
import monasca_agent.collector.checks.services_checks as services_checks
from monasca_agent.collector.checks.kubeutil import set_kube_settings, get_kube_settings, get_kube_labels

NAMESPACE = "kubernetes"
DEFAULT_MAX_DEPTH = 10

class Kubernetes(services_checks.ServicesCheck):
    """ Collect metrics and events from kubelet """

    pod_names_by_container = {}

    def __init__(self, name, init_config, agent_config, instances=None):
        if instances is not None and len(instances) > 1:
            raise Exception('Kubernetes check only supports one configured instance.')
        super(Kubernetes, self).__init__(name, init_config, agent_config, instances)
        self.kube_settings = set_kube_settings(instances[0])
        self.max_depth = instances[0].get('max_depth', DEFAULT_MAX_DEPTH)

    def _check(self, instance):
        kube_settings = get_kube_settings()
        # self.log.info("kube_settings: %s" % kube_settings)
        if not kube_settings.get("host"):
            raise Exception('Unable to get default router and host parameter is not set')

        # kubelet metrics
        self._update_metrics(instance, kube_settings)


    def _update_metrics(self, instance, kube_settings):
        metrics = urllib.urlopen(kube_settings["metrics_url"]).read()
        metrics_list = metrics.split()
        metrics_list.remove('#')
        replaced = [w.replace('}',',') for w in metrics_list]

       # row_json = json.dumps(metrics.split())

        self.log.info("replaced: %s " % replaced)

        if not metrics:
            raise Exception('No metrics retrieved cmd=%s' % self.metrics_cmd)

