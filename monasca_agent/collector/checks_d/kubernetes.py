"""kubernetes check
Collects metrics from cAdvisor instance
"""
# stdlib
import numbers
from fnmatch import fnmatch
import re
import traceback

# 3rd party
import requests

# project
import monasca_agent.collector.checks.services_checks as services_checks
from monasca_agent.collector.checks.kubeutil import set_kube_settings, get_kube_settings, get_kube_labels

def retrieve_json(url):
    r = requests.get(url)
    r.raise_for_status()
    return r.json()


def _is_affirmative(s):
    # int or real bool
    if isinstance(s, int):
        return bool(s)
    # try string cast
    return s.lower() in ('yes', 'true', '1')


NAMESPACE = "kubernetes"
DEFAULT_MAX_DEPTH = 10
DEFAULT_PUBLISH_ALIASES = False
DEFAULT_ENABLED_RATES = [
    'diskio.io_service_bytes.stats.total',
    'network.??_bytes',
    'cpu.*.total']

NET_ERRORS = ['rx_errors', 'tx_errors', 'rx_dropped', 'tx_dropped']

DEFAULT_ENABLED_GAUGES = [
    'memory.usage',
    'filesystem.usage']

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

        self.publish_aliases = _is_affirmative(instance.get('publish_aliases', DEFAULT_PUBLISH_ALIASES))
        enabled_gauges = instance.get('enabled_gauges', DEFAULT_ENABLED_GAUGES)
        self.enabled_gauges = ["{0}.{1}".format(NAMESPACE, x) for x in enabled_gauges]
        enabled_rates = instance.get('enabled_rates', DEFAULT_ENABLED_RATES)
        self.enabled_rates = ["{0}.{1}".format(NAMESPACE, x) for x in enabled_rates]

        # kubelet metrics
        self._update_metrics(instance, kube_settings)

    def _publish_raw_metrics(self, metric, dat, dims, depth=0):
        # self.log.info("execute def _publish_raw_metrics")
        if depth >= self.max_depth:
            self.log.warning('Reached max depth on metric=%s' % metric)
            return

        if isinstance(dat, numbers.Number):
            if self.enabled_rates and any([fnmatch(metric, pat) for pat in self.enabled_rates]):
                self.log.info("_publish_raw_metrics numbers float: {0}, {1}, {2}".format(metric, dat, dims))
            if self.enabled_gauges and any([fnmatch(metric, pat) for pat in self.enabled_gauges]):
                self.gauge(metric, float(dat), dimensions=dims)
                self.log.info("_publish_raw_metrics numbers gauge: {0}, {1}, {2}".format(metric, dat, dims))

        elif isinstance(dat, dict):
            for k,v in dat.iteritems():
                self._publish_raw_metrics(metric + '.%s' % k.lower(), v, dims)
                self.log.info("_publish_raw_metrics dat dict: {0}, {1}, {2}".format(metric, (k.lower(), v), dims))
        elif isinstance(dat, list):
            self._publish_raw_metrics(metric, dat[-1], dims)
            self.log.info("_publish_raw_metrics dat list: {0}, {1}, {2}".format(metric, dat[-1], dims))

    @staticmethod
    def _shorten_name(name):
        # shorten docker image id
        return re.sub('([0-9a-fA-F]{64,})', lambda x: x.group(1)[0:12], name)

    @staticmethod
    def _normalize_name(name):
        # remove invalid characters
        return re.sub('><=\{\}\(\),\'"\;& ', '_', name)

    def _update_container_metrics(self, instance, subcontainer, kube_labels):
        dims = instance.get('dimensions', {})  # add support for custom dims
        if len(subcontainer.get('aliases', [])) >= 1:
            # The first alias seems to always match the docker container name
            container_name = subcontainer['aliases'][0]
        else:
            # We default to the container id
            container_name = subcontainer['name']

        dims['container_name'] = self._normalize_name(container_name)

        pod_name_set = False
        try:
            for label_name, label in subcontainer['spec']['labels'].items():
                self.log.info("subcontainer.spec.labels: %s", type(subcontainer["spec"]["labels"]))
                label_name = label_name.replace('io.kubernetes.pod.name', 'pod_name')
                if label_name == "pod_name":
                    pod_name_set = True
                    pod_labels = kube_labels.get(label)
                    if pod_labels:
                        pod_labels.update(dims)
                        if "-" in label:
                            replication_controller = "-".join(
                            label.split("-")[:-1])
                        if "/" in replication_controller:
                            namespace, replication_controller = replication_controller.split("/", 1)
                            dims["kube_namespace"] = self._normalize_name(namespace)
                            dims["kube_replication_controller"] = self._normalize_name(replication_controller)
                dims["label_name"] = self._normalize_name(label)
        except KeyError:
           pass

        if not pod_name_set:
            dims['pod_name'] = "no_pod"

        if self.publish_aliases and subcontainer.get("aliases"):
             for alias in subcontainer['aliases'][1:]:
               # we don't add the first alias as it will be the container_name
                dims.append('container_alias:%s' % (self._shorten_name(alias)))

        stats = subcontainer['stats'][-1]  # take the latest
        self._publish_raw_metrics(NAMESPACE, stats, dims)

        if subcontainer.get("spec", {}).get("has_filesystem"):
             fs = stats['filesystem'][-1]
             fs_utilization = float(fs['usage']) / float(fs['capacity'])
             self.log.info("filesystem for subcontainer get: {0}, {1}.filesystem.usage_pct, {2}".format(NAMESPACE, fs_utilization, dims))
             self.gauge(self, NAMESPACE + '.filesystem.usage_pct', fs_utilization, dims)

        if subcontainer.get("spec", {}).get("has_network"):
             net = stats['network']
             self.log.info("network for subcontainer get: {0}, {1}".format(stats['network'], dims))
             self.rate(self, NAMESPACE + '.network_errors', sum(float(net[x]) for x in NET_ERRORS), dims)

    @staticmethod
    def _retrieve_metrics(url):
        return retrieve_json(url)

    @property
    def _retrieve_kube_labels(self):
        return get_kube_labels()

    def _update_metrics(self, instance, kube_settings):
        metrics = self._retrieve_metrics(kube_settings["metrics_url"])
        kube_labels = self._retrieve_kube_labels
        if not metrics:
            raise Exception('No metrics retrieved cmd=%s' % self.metrics_cmd)

        for subcontainer in metrics:
             try:
                 self._update_container_metrics(instance, subcontainer, kube_labels)
             except Exception, e:
                 self.log.error("Unable to collect metrics for container: {0} ({1}".format(
                         subcontainer.get('name'), e))
             traceback.print_exc()
