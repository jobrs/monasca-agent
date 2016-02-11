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

# DEFAULT_USE_HISTOGRAM = False
# DEFAULT_PUBLISH_ALIASES = False
DEFAULT_ENABLED_RATES = [
    'diskio.io_service_bytes.stats.total',
    'network.??_bytes',
    'cpu.*.total']

NET_ERRORS = ['rx_errors', 'tx_errors', 'rx_dropped', 'tx_dropped']

DEFAULT_ENABLED_GAUGES = [
    'memory.usage',
    'filesystem.usage']

# HISTORATE = AgentCheck.generate_historate_func(["container_name"])
# HISTO = AgentCheck.generate_histogram_func(["container_name"])
# FUNC_MAP = {
#    GAUGE: {True: HISTO, False: GAUGE},
#    RATE: {True: HISTORATE, False: RATE}
# }

class Kubernetes(services_checks.ServicesCheck):
    """ Collect metrics and events from kubelet """

    pod_names_by_container = {}

    def __init__(self, name, init_config, agent_config, instances=None):
        if instances is not None and len(instances) > 1:
            raise Exception('Kubernetes check only supports one configured instance.')
        super(Kubernetes, self).__init__(name, init_config, agent_config, instances)
        self.kube_settings = set_kube_settings(instances[0])
        self.max_depth = instances[0].get('max_depth', DEFAULT_MAX_DEPTH)


# """    def _perform_kubelet_checks(self, url):
#         service_check_base = NAMESPACE + '.kubelet.check'
#         is_ok = True
#         try:
#             r = requests.get(url)
#             for line in r.iter_lines():
#
                # avoid noise; this check is expected to fail since we override the container hostname
                # if line.find('hostname') != -1:
                #     continue
                #
                # matches = re.match('\[(.)\]([^\s]+) (.*)?', line)
                # if not matches or len(matches.groups()) < 2:
                #     continue
                #
                # service_check_name = service_check_base + '.' + matches.group(2)
                # status = matches.group(1)
                # if status != '+':
                #     is_ok = False
        # return services_checks.Status.UP, status
        #
        # except Exception, e:
        #     self.log.warning('kubelet check failed: %s' % str(e))
        #     self.service_check(service_check_base, AgentCheck.CRITICAL,
        #                        message='Kubelet check failed: %s' % str(e))
        #
        # else:
        #     if is_ok:
        #         self.service_check(service_check_base, AgentCheck.OK)
        #     else:
        #         self.service_check(service_check_base, AgentCheck.CRITICAL)
   #
    def _perform_master_checks(self, url):
    #    try:
            r = requests.get(url)
            r.raise_for_status()
            for nodeinfo in r.json()['items']:
                 nodename = nodeinfo['name']
                 service_check_name = "{0}.master.{1}.check".format(NAMESPACE, nodename)
                 cond = nodeinfo['status'][-1]['type']
                 minion_name = nodeinfo['metadata']['name']
                 tags = ["minion_name:{0}".format(minion_name)]
   #              if cond != 'Ready':
   #                  self.service_check(service_check_name, AgentCheck.CRITICAL,
   #                                     tags=tags, message=cond)
   #             else:
   #         self.service_check(service_check_name, AgentCheck.OK, tags=tags)
   #     except Exception, e:
   #         self.service_check(service_check_name, AgentCheck.CRITICAL, message=str(e))
   #         self.log.warning('master checks url=%s exception=%s' % (url, str(e)))
   #         raise

    def _check(self, instance):
        kube_settings = get_kube_settings()
        # self.log.info("kube_settings: %s" % kube_settings)
        if not kube_settings.get("host"):
            raise Exception('Unable to get default router and host parameter is not set')

        enabled_gauges = instance.get('enabled_gauges', DEFAULT_ENABLED_GAUGES)
        self.enabled_gauges = ["{0}.{1}".format(NAMESPACE, x) for x in enabled_gauges]
        enabled_rates = instance.get('enabled_rates', DEFAULT_ENABLED_RATES)
        self.enabled_rates = ["{0}.{1}".format(NAMESPACE, x) for x in enabled_rates]

        # master health checks
        if instance.get('enable_master_checks', False):
            master_url = kube_settings["master_url_nodes"]
            self._perform_master_checks(master_url)

        # kubelet health checks
        # if instance.get('enable_kubelet_checks', True):
        #     kube_health_url = kube_settings["kube_health_url"]
        #     self._perform_kubelet_checks(kube_health_url)

        # kubelet metrics
        self._update_metrics(instance, kube_settings)

    def _publish_raw_metrics(self, metric, dat, dimensions, depth=0):
        # self.log.info("execute def _publish_raw_metrics")
        if depth >= self.max_depth:
            self.log.warning('Reached max depth on metric=%s' % metric)
            return

        if isinstance(dat, numbers.Number):
            if self.enabled_rates and any([fnmatch(metric, pat) for pat in self.enabled_rates]):
                self.rate(metric, float(dat), dimensions={})
            if self.enabled_gauges and any([fnmatch(metric, pat) for pat in self.enabled_gauges]):
                self.gauge(metric, float(dat), dimensions={})

        elif isinstance(dat, dict):
            for k,v in dat.iteritems():
                self._publish_raw_metrics(metric + '.%s' % k.lower(), v, dimensions={})

        elif isinstance(dat, list):
            self._publish_raw_metrics(metric, dat[-1], dimensions={})

    @staticmethod
    def _shorten_name(name):
        # shorten docker image id
        return re.sub('([0-9a-fA-F]{64,})', lambda x: x.group(1)[0:12], name)


    def _update_container_metrics(self, instance, subcontainer, kube_labels):
        tags = instance.get('dimensions', {})  # add support for custom tags
        if len(subcontainer.get('aliases', [])) >= 1:
            # The first alias seems to always match the docker container name
            container_name = subcontainer['aliases'][0]
        else:
            # We default to the container id
            container_name = subcontainer['name']

        tags['container_name'] = container_name

        pod_name_set = False
        try:
            for label_name, label in subcontainer['spec']['labels'].items():
                self.log.info("subcontainer.spec.labels: %s", type(subcontainer["spec"]["labels"]))
                label_name = label_name.replace('io.kubernetes.pod.name', 'pod_name')
                if label_name == "pod_name":
                    pod_name_set = True
                    pod_labels = kube_labels.get(label)
                    if pod_labels:
                        pod_labels.update(tags)
                        if "-" in label:
                            replication_controller = "-".join(
                            label.split("-")[:-1])
                        if "/" in replication_controller:
                            namespace, replication_controller = replication_controller.split("/", 1)
                            tags["kube_namespace"] = namespace
                            tags["kube_replication_controller"] = replication_controller
                tags["label_name"] = label
        except KeyError:
           pass

        if not pod_name_set:
            tags['pod_name'] = "no_pod"

        #if self.publish_aliases and subcontainer.get("aliases"):
        #     for alias in subcontainer['aliases'][1:]:
        #        we don't add the first alias as it will be the container_name
        #        tags.append('container_alias:%s' % (self._shorten_name(alias)))

        stats = subcontainer['stats'][-1]  # take the latest
        self._publish_raw_metrics(NAMESPACE, stats, tags)
 #       self.log.debug('publish %s %s %s', NAMESPACE, stats, tags)

        # if subcontainer.get("spec", {}).get("has_filesystem"):
        #      fs = stats['filesystem'][-1]
        #      fs_utilization = float(fs['usage']) / float(fs['capacity'])
        #      self.publish_gauge(self, NAMESPACE + '.filesystem.usage_pct', fs_utilization, tags)
        #
        # if subcontainer.get("spec", {}).get("has_network"):
        #      net = stats['network']
        #      self.publish_rate(self, NAMESPACE + '.network_errors',
        #                        sum(float(net[x]) for x in NET_ERRORS),
        #                        tags)

    @staticmethod
    def _retrieve_metrics(url):
        return retrieve_json(url)

    @property
    def _retrieve_kube_labels(self):
        return get_kube_labels()

    def _update_metrics(self, instance, kube_settings):
        # self.log.info("execute def _update_metrics")
        metrics = self._retrieve_metrics(kube_settings["metrics_url"])
        # self.log.info('metrics: %s' % metrics)
        kube_labels = self._retrieve_kube_labels
        # self.log.info('kube_labels: %s' % kube_labels)
        if not metrics:
            raise Exception('No metrics retrieved cmd=%s' % self.metrics_cmd)

        for subcontainer in metrics:
             try:
                 self._update_container_metrics(instance, subcontainer, kube_labels)
             except Exception, e:
                 self.log.error("Unable to collect metrics for container: {0} ({1}".format(
                         subcontainer.get('name'), e))
             traceback.print_exc()