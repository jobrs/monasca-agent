# stdlib
import os
import unittest
import time

# project
from monasca_agent.collector.checks_d.prometheus import Prometheus

dir = os.getcwd()
prometheus_metrics_t0 = "file://" + os.path.join(dir, 'prometheus_metrics_t0')
prometheus_metrics_t1 = "https://prometheus.staging.cloud.sap/federate"

LOCAL_CONFIG = {'name': 'prometheus-federate',
                'url': prometheus_metrics_t0,  # pass in sample prometheus metrics via file
                'mapping': {
                    'gauges': ['apiserver_request_count',
                               'apiserver_request_latencies_summary_sum',
                               'apiserver_request_latencies_bucket',
                               'apiserver_sample_rate'],
                    'rates': ['apiserver_sample_rate'],
                    'dimensions': {
                        'resource': 'resource',
                        'verb': 'verb',
                        'instance': 'instance',
                        'component': 'kubernetes_role',
                        'service': 'job'
                    }
                }}

FEDERATE_CONFIG = {'name': 'Prometheus',
                   'url': prometheus_metrics_t1,  # pass in sample prometheus metrics via file
                   'mapping': {
                       'dimensions': {
                           'resource': 'resource',
                           'kubernetes.namespace': 'kubernetes_namespace',
                           'kubernetes.pod_name': 'kubernetes_pod_name'
                       },
                       'groups': {
                           'dns.bind': {
                               'gauges': ['bind_(up)'],
                               'rates': ['bind_(incoming_queries)_total', 'bind_(responses)_total'],
                               'dimensions': {
                                   "kubernetes.container_name": "kubernetes_name"
                               },
                           }
                       }
                   }}

METRICS_FAMILIES = ["container_cpu_system_seconds_total",
                    "container_cpu_usage_seconds_total",
                    "container_fs_io_time_seconds_total",
                    "container_fs_write_seconds_total"]

CONFIG = {'init_config': {}, 'instances': [LOCAL_CONFIG]}


# kubernetes:
# gauges: ['(container_start_time_sec)onds', 'container_memory_usage_bytes']
# rates: ['(container_cpu_usage_sec)onds_total', '(container_network_.*_packages)_total']
# dimensions:
# kubernetes.container_name: kubernetes_container_name

class TestPrometheusClientScraping(unittest.TestCase):
    def setUp(self):
        self.check = Prometheus('prometheus', CONFIG['init_config'], {}, instances=CONFIG['instances'])

    def runTest(self):
        self.run_check()

    def run_check(self):
        self.setUp()

    def testEndpointScraping(self):
        # check parsing and pushing metrics works
        self.check.run()
        time.sleep(2)
        self.check.run()

        time.sleep(2)
        metrics = self.check.get_metrics()

        metric_family_names = [m.name for m in metrics]

        # check parsed metrics
        self.assertTrue(metrics)
        self.assertTrue('apiserver_request_count' in metric_family_names)
        self.assertEqual([i.value for i in metrics if i.name == 'apiserver_request_count'][0], 403)
        self.assertEqual(
            [i.dimensions['instance'] for i in metrics if i.name == 'apiserver_request_count'][0],
            u'kubernetes.default:443')

        self.assertTrue('apiserver_request_latencies_bucket' in metric_family_names)
        self.assertEqual([i.value for i in metrics if i.name == 'apiserver_request_latencies_bucket'][0],
                         443394)
        self.assertEqual(
            [i.dimensions['component'] for i in metrics if i.name == 'apiserver_request_latencies_bucket'][
                0],
            'apiserver')


if __name__ == '__main__':
    t = TestPrometheusClientScraping()
    t.runTest()
