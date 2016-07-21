# stdlib
import os
import unittest
import time

# project
from monasca_agent.collector.checks_d.prometheus import Prometheus

METRICS_FAMILIES = ["container_cpu_system_seconds_total",
                    "container_cpu_usage_seconds_total",
                    "container_fs_io_time_seconds_total",
                    "container_fs_write_seconds_total"]

dir = os.getcwd()
prometheus_metrics_t0 = "file://" + os.path.join(dir, 'prometheus_metrics_t0')

CONFIG = {'init_config': {}, 'instances': [{'name': 'prometheus-federate',
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
                                            }}]
          }


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
        self.assertTrue('prometheus.apiserver_request_count' in metric_family_names)
        self.assertEqual([i.value for i in metrics if i.name == 'prometheus.apiserver_request_count'][0], 403)
        self.assertEqual(
            [i.dimensions['instance'] for i in metrics if i.name == 'prometheus.apiserver_request_count'][0],
            u'kubernetes.default:443')

        self.assertTrue('prometheus.apiserver_request_latencies_bucket' in metric_family_names)
        self.assertEqual([i.value for i in metrics if i.name == 'prometheus.apiserver_request_latencies_bucket'][0],
                         443394)
        self.assertEqual(
            [i.dimensions['component'] for i in metrics if i.name == 'prometheus.apiserver_request_latencies_bucket'][
                0],
            'apiserver')


if __name__ == '__main__':
    t = TestPrometheusClientScraping()
    t.runTest()
