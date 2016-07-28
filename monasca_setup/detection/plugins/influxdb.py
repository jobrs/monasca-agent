import logging
import monasca_agent.collector.checks_d.influxdb as influxdb
import monasca_setup.agent_config
import monasca_setup.detection as detection
import os
import re
import requests

log = logging.getLogger(__name__)

# set up some defaults
DEFAULT_TIMEOUT = 1
DEFAULT_COLLECT_RESPONSE_TIME = True

# meaningful defaults, keep configuration small (currently only for 0.9.5/0.9.6)
DEFAULT_METRICS_WHITELIST = {'httpd': ['points_write_ok', 'query_req', 'write_req'],
                             'engine': ['points_write', 'points_write_dedupe'],
                             'shard': ['series_create', 'fields_create', 'write_req', 'points_write_ok']}


class InfluxDB(monasca_setup.detection.ArgsPlugin):
    """Setup an InfluxDB according to the passed in args.
    """

    def _detect(self):
        """Run detection, set self.available True if the service is detected.
        """

        self.influxd = detection.find_process_name('influxd')
        if self.influxd is not None:
            self.available = True

    def build_config(self):
        """Build the config as a Plugins object and return.
        """
        config = monasca_setup.agent_config.Plugins()

        try:
            # do not add another instance if there is already something configured
            if self._get_config():
                log.info("\tEnabling the InfluxDB check for {:s}".format(self.url))
                instance = {'name': 'localhost',
                            'url': self.url,
                            'collect_response_time':
                                self.collect_response_time,
                            }
                if self.timeout is not None:
                    instance['timeout'] = self.timeout
                if self.whitelist is not None:
                    instance['whitelist'] = self.whitelist
                # extract stats continuously
                config['influxdb'] = {'init_config': None,
                                      'instances': [instance]}
                # watch processes using process plugin
                config.merge(detection.watch_process(['influxd'], component='influxdb', exact_match=False))
            else:
                log.warn('Unable to access the InfluxDB diagnostics URL;' +
                         ' the InfluxDB plugin is not configured.' +
                         ' Please correct and re-run monasca-setup.')
        except Exception as e:
            log.exception('Error configuring the InfluxDB check plugin: %s', repr(e))

        return config

    @staticmethod
    def _compare_versions(v1, v2):
        def normalize(v):
            return [int(x) for x in re.sub(r'(\.0+)*$', '', re.sub(r'-', '.', v)).split(".")]

        return cmp(normalize(v1), normalize(v2))

    def _connection_test(self, url):
        log.debug('Attempting to connect to InfluxDB API at %s', url)
        uri = url + "/ping"
        try:
            resp = requests.get(url=uri, timeout=self.timeout)
            self.version = resp.headers.get('x-influxdb-version')
            if self.version:
                log.info('Discovered InfluxDB version %s', self.version)
            else:
                return False

            supported = self._compare_versions(self.version, '0.9.5') >= 0
            if not supported:
                log.debug('Unsupported InfluxDB version: %s', self.version)
            return supported

        except Exception as e:
            log.debug('Unable to access the InfluxDB query URL %s: %s', uri, repr(e))

        return False

    def _discover_config(self):
        # discover API port
        for conn in self.influxd.connections('inet'):
            for protocol in ['http', 'https']:
                u = '{0}://localhost:{1}'.format(protocol, conn.laddr[1])
                if self._connection_test(u):
                    self.url = u
                    return True

        log.error('Unable to discover InfluxDB port using process %s', self.influxd.name)
        return False

    def _get_config(self):
        """Set the configuration to be used for connecting to InfluxDB
        """

        # Set defaults and read config or use arguments
        self.timeout = os.getenv('INFLUXDB_MONITORING_TIMEOUT', DEFAULT_TIMEOUT)
        self.whitelist = DEFAULT_METRICS_WHITELIST
        self.collect_response_time = DEFAULT_COLLECT_RESPONSE_TIME

        # when args have been passed, then not self discovery is attempted
        if self.args is not None:
            if self.args.get('influxdb.whitelist', None) == '*':
                self.whitelist = None    # meaning any
            self.timeout = self.args.get('influxdb.timeout', self.timeout)
            self.collect_response_time = self.args.get('collect_response_time', DEFAULT_COLLECT_RESPONSE_TIME)

        return self._discover_config()
