import os

import unittest
import time

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
                                                'gauges': ['container_last_seen'],
                                                'rates': ['container_cpu_system_seconds_total'],
                                                'dimensions': {
                                                    'index': 'index',
                                                    'container_id': {
                                                        'source_key': 'name',
                                                        'regex': 'k8s_([._\-a-zA-Z0-9]*)'
                                                    }
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
        self.testEndpointScraping()

    def testEndpointScraping(self):
        # check parsing and pushing metrics works
        self.check.run()
        time.sleep(2)
        self.check.run()

        # manually pushing another, because a rate needs at least 2 samples
        self.check._publisher.push_metric(CONFIG['instances'][0],
                                          metric='container_cpu_system_seconds_total',
                                          value=950,
                                          labels={u'id': u'/docker', u'name': u'k8s_bar'},
                                          fixed_dimensions=None)

        metrics = self.check.get_metrics()

        metric_family_names = [m.name for m in metrics]

        # check parsed rates
        self.assertTrue(metrics)
        self.assertTrue('prometheus.container_cpu_system_seconds_total' in metric_family_names)
        self.assertEqual([i.value for i in metrics if i.name == 'prometheus.container_cpu_system_seconds_total'][0] , 28.410000000000025)
        self.assertEqual([i.dimensions['container_id'] for i in metrics if i.name == 'prometheus.container_cpu_system_seconds_total'][0] ,'bar')

        # check parsed gauge
        self.assertTrue('prometheus.container_last_seen' in metric_family_names)
        self.assertEqual([i.value for i in metrics if i.name == 'prometheus.container_last_seen'][0] ,1467191065.0)
        self.assertEqual([i.dimensions['container_id'] for i in metrics if i.name == 'prometheus.container_last_seen'][0] ,'bar')

if __name__ == '__main__':
    t = TestPrometheusClientScraping()
    t.runTest()
