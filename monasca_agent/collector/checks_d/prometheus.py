"""
Plugin to scrape prometheus endpoint
"""

# This file uses 'print' as a function rather than a statement, a la Python3
from __future__ import print_function

# Check if dependency is available
try:
    import prometheus_client.parser as prometheus_client_parser
except Exception:
    prometheus_client_parser = None

# stdlib
import logging
from datetime import datetime
import calendar

# 3rd party
import urllib2

# project
import monasca_agent.collector.checks.utils as utils
import monasca_agent.collector.checks.services_checks as services_checks
import monasca_agent.common.exceptions as exceptions

logging.basicConfig()
log = logging.getLogger(__name__)

PROMETHEUS_PREFIX = "prometheus"


class Prometheus(services_checks.ServicesCheck):
    """
    Collect metrics and events
    """

    def __init__(self, name, init_config, agent_config, instances=None):
        super(Prometheus, self).__init__(name, init_config, agent_config, instances)
        # last time of polling
        self._last_ts = {}
        self._publisher = utils.DynamicCheckHelper(self)
        self._urls = {}
        for inst in instances:
            url = inst['url']
            # for Prometheus federation URLs, set the name match filter according to the mapping
            if url.endswith('/federate'):
                mapped_metrics = self._publisher.get_mapped_metrics(inst)
                url += '?match[]={__name__=~"'+"|".join(mapped_metrics)+'"}'
                log.info("Fetching from Prometheus federation URL: %s", url)
            self._urls[inst['name']] = url

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
        # conversion using strptime() works only for 6 digits in microseconds so the timestamp is limited to 26 characters
        ts = datetime.strptime(timestamp[:25] + timestamp[-1], "%Y-%m-%dT%H:%M:%S.%fZ")
        return calendar.timegm(datetime.timetuple(ts))

    def _update_container_metrics(self, instance, metric_name, container, metric_type=None, timestamp=None,
                                  fixed_dimensions=None):

        if metric_type == 'untyped':
            metric_type = None

            self._publisher.push_metric(instance,
                                        metric=metric_name,
                                        value=container[2],
                                        labels=container[1],
                                        timestamp=timestamp,
                                        fixed_dimensions=fixed_dimensions)

    def _retrieve_and_parse_metrics(self, url):
        """
        Metrics from prometheus come in plain text from the endpoint and therefore need to be parsed.
        To do that the prometheus client's text_string_to_metric_families -method is used. That method returns a generator object.

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
                                      ('container_cpu_system_seconds_total', {u'id': u'/docker', u'name': u'/docker'}, 922.66),
                                    ..] '

        :param url: the url of the prometheus metrics
        :return: metric_families generator
        """
        str = urllib2.urlopen(url).read()
        metric_families = prometheus_client_parser.text_string_to_metric_families(str)
        return metric_families

    def _update_metrics(self, instance):
        metric_families_generator = self._retrieve_and_parse_metrics(instance['url'])

        if not metric_families_generator:
            raise Exception('No metrics retrieved cmd=%s' % self.metrics_cmd)

        for metric_family in metric_families_generator:
            try:
                for container in metric_family.samples:
                    self._update_container_metrics(instance, metric_family.name, container, metric_family.type)
            except Exception as e:
                self.log.warning("Unable to collect metric: {0} for container: {1} . - {2} ".format(
                    metric_family.name, container[1].get('name'), repr(e)))

    def _update_last_ts(self, instance_name):
        utc_now = datetime.utcnow()
        self._last_ts[instance_name] = utc_now.isoformat('T')
