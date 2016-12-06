"""
Plugin to scrape prometheus endpoint
"""

# This file uses 'print' as a function rather than a statement, a la Python3
from __future__ import print_function

import math
import requests

# import prometheus client dependency dynamically
from monasca_agent.common import util
from requests import RequestException

try:
    import prometheus_client.parser as prometheus_client_parser
except ImportError:
    prometheus_client_parser = None

# stdlib
import logging
from datetime import datetime
import calendar

# project
import monasca_agent.collector.checks.utils as utils
import monasca_agent.collector.checks.services_checks as services_checks
import monasca_agent.common.exceptions as exceptions

log = logging.getLogger(__name__)


class Prometheus(services_checks.ServicesCheck):
    """
    Collect metrics and events
    """

    def __init__(self, name, init_config, agent_config, instances=None):
        super(Prometheus, self).__init__(name, init_config, agent_config, instances)
        # last time of polling
        self._last_ts = {}
        self._publisher = utils.DynamicCheckHelper(self)
        self._config = {}
        for inst in instances:
            url = inst['url']
            # for Prometheus federation URLs, set the name match filter according to the mapping
            if url.endswith('/federate'):
                mapped_metrics = self._publisher.get_mapped_metrics(inst)
                url += '?match[]={__name__=~"' + ("|".join(mapped_metrics) + '"')
                for key, value in inst.get('match_labels', {}).items():
                    if isinstance(value, list):
                        url += ',{}=~"{}"'.format(key, "|".join(value))
                    else:
                        url += ',{}=~"{}"'.format(key, value)
                url += '}'
                log.info("Fetching from Prometheus federation URL: %s", url)
            self._config[inst['name']] = {'url': url, 'timeout': int(inst.get('timeout', 5)),
                                    'collect_response_time': bool(inst.get('collect_response_time', False))}

    def _check(self, instance):
        if prometheus_client_parser is None:
            self.log.warning("Skipping prometheus plugin check due to missing 'prometheus_client' module.")
            return

        self._update_metrics(instance)

    # overriding method to catch Infinity exception
    def get_metrics(self, prettyprint=False):
        """Get all metrics, including the ones that are tagged.

        @return the list of samples
        @rtype list of Measurement objects from monasca_agent.common.metrics
        """
        try:
            return super(Prometheus, self).get_metrics(prettyprint)
        except exceptions.Infinity:
            # self._disabledMetrics.append(metric_name)
            self.log.exception("Caught infinity exception in prometheus plugin.")
            if not prettyprint:
                self.log.error("More dimensions needs to be mapped in order to resolve clashing measurements")
                return self.get_metrics(True)
            else:
                return []

    @staticmethod
    def _convert_timestamp(timestamp):
        # convert from string '2016-03-16T16:48:59.900524303Z' to a float monasca can handle 164859.900524
        # conversion using strptime() works only for 6 digits in microseconds so the timestamp is limited to
        # 26 characters
        ts = datetime.strptime(timestamp[:25] + timestamp[-1], "%Y-%m-%dT%H:%M:%S.%fZ")
        return calendar.timegm(datetime.timetuple(ts))

    def _update_container_metrics(self, instance, metric_name, container, timestamp=None,
                                  fixed_dimensions=None):

        # TBD determine metric from Prometheus input

        labels = container[1]
        value = float(container[2])
        if math.isnan(value):
            self.log.debug('filtering out NaN value provided for metric %s{%s}', metric_name, labels)
            return

        self._publisher.push_metric(instance,
                                    metric=metric_name,
                                    value=value,
                                    labels=labels,
                                    timestamp=timestamp,
                                    fixed_dimensions=fixed_dimensions)

    def _retrieve_and_parse_metrics(self, url, timeout, collect_response_time, instance_name):
        """
        Metrics from prometheus come in plain text from the endpoint and therefore need to be parsed.
        To do that the prometheus client's text_string_to_metric_families -method is used. That method returns a
        generator object.

        The method consumes the metrics from the endpoint:
            # HELP container_cpu_system_seconds_total Cumulative system cpu time consumed in seconds.
            # TYPE container_cpu_system_seconds_total counter
            container_cpu_system_seconds_total{id="/",name="/"} 1.59578817e+06
            ....
        and produces a metric family element with (returned from generator) with the following attributes:
            name          -> e.g. ' container_cpu_system_seconds_total '
            documentation -> e.g. ' container_cpu_system_seconds_total Cumulative system cpu time consumed in seconds. '
            type          -> e.g. ' counter '
            samples       -> e.g. ' [.. ,("container_cpu_system_seconds_total", {id="/",name="/"}, 1.59578817e+06),
                                      ('container_cpu_system_seconds_total', {u'id': u'/docker', u'name': u'/docker'},
                                      922.66),
                                    ..] '

        :param url: the url of the prometheus metrics
        :return: metric_families iterable
        """

        timer = util.Timer()

        try:
            response = requests.get(url, timeout=timeout)

            # report response time first, even when there is HTTP errors
            if collect_response_time:
                # Stop the timer as early as possible
                running_time = timer.total()
                self.gauge('monasca.agent.collect_time', running_time, dimensions={'agent_check': 'influxdb',
                                                                                   'instance': instance_name})

            response.raise_for_status()
            body = response.text
        except RequestException:
            self.log.exception("Retrieving metrics from endpoint %s failed", url)
            self.rate('monasca.agent.collect_errors', 1, dimensions={'agent_check': 'prometheus',
                                                                     'instance': instance_name})
            return []

        metric_families = prometheus_client_parser.text_string_to_metric_families(body)
        return metric_families

    def _update_metrics(self, instance):
        cfg = self._config[instance['name']]
        metric_families_generator = self._retrieve_and_parse_metrics(cfg['url'], cfg['timeout'],
                                                                     cfg['collect_response_time'], instance['name'])

        for metric_family in metric_families_generator:
            container = None
            try:
                for container in metric_family.samples:
                    # currently there is no support for detecting metric types from P8S
                    self._update_container_metrics(instance, metric_family.name, container)
            except Exception as e:
                self.log.warning("Unable to collect metric: {0} for container: {1} . - {2} ".format(
                    metric_family.name, container[1].get('name'), repr(e)))
                self.rate('monasca.agent.collect_errors', 1, dimensions={'agent_check': 'prometheus',
                                                                         'instance': instance['name']})

    def _update_last_ts(self, instance_name):
        utc_now = datetime.utcnow()
        self._last_ts[instance_name] = utc_now.isoformat('T')
