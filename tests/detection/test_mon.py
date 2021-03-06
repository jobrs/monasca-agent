# Copyright 2016 FUJITSU LIMITED
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
import random
import unittest

import logging
import mock
import psutil

from monasca_setup.detection.plugins import mon

_PYTHON_CMD_API = ('/opt/monasca-api/bin/gunicorn'
                   ' -n monasca-api'
                   ' -k eventlet --worker-connections=2000 '
                   '--backlog=1000 '
                   '--paste /etc/monasca/api-config.ini -w 9')
_JAVA_CMD_API = ('/usr/bin/java '
                 '-Dfile.encoding=UTF-8 -Xmx128m '
                 '-cp /opt/monasca/monasca-api.jar '
                 'monasca.api.MonApiApplication server '
                 '/etc/monasca/api-config.yml')

_PYTHON_CMD_PERSISTER = ('/opt/monasca-persister/bin/python '
                         '/opt/monasca-persister/lib/python2.7/'
                         'site-packages/monasca_persister/persister.py'
                         ' --config-file /etc/monasca/persister.conf')
_JAVA_CMD_PERSISTER = ('/usr/bin/java -Dfile.encoding=UTF-8 -Xmx128m '
                       '-cp /opt/monasca/monasca-persister.jar '
                       'monasca.persister.PersisterApplication server '
                       '/etc/monasca/persister-config.yml')

_JAVA_YML_CFG_BIT_API = '''
hibernate:
  supportEnabled: {hibernate_enabled}
server:
  adminConnectors:
    - type: {admin_type}
      port: {admin_port}
  applicationConnectors:
    - port: {app_port}
'''

_JAVA_YML_CFG_BIT_PERSISTER = '''
alarmHistoryConfiguration:
    numThreads: {ah_threads}
metricConfiguration:
    numThreads: {mc_threads}
databaseConfiguration:
    databaseType: {db_type}
server:
    adminConnectors:
        - type: {admin_type}
          port: {admin_port}
'''

LOG = logging.getLogger(mon.__name__)


class FakeInetConnection(object):
    def __init__(self, api_port=None):
        if api_port is None:
            api_port = 8070  # default one
        self.laddr = [1, api_port]


class FakeProcesses(object):
    name = None
    cmdLine = None
    inetConnections = [
        FakeInetConnection()
    ]

    def as_dict(self):
        return {'name': FakeProcesses.name,
                'cmdline': FakeProcesses.cmdLine,
                'exe': self.exe()}

    def cmdline(self):
        return self.cmdLine

    def exe(self):
        return self.cmdLine[0]

    def connections(self, *args):
        return FakeProcesses.inetConnections


class TestGetImplLang(unittest.TestCase):
    @mock.patch('psutil.Process')
    def test_should_return_python_lang_for_gunicorn_process(self, proc):
        proc.as_dict.return_value = {'exe': '/opt/monasca-api/bin/gunicorn'}
        self.assertEqual('python', mon._get_impl_lang(proc))

    @mock.patch('psutil.Process')
    def test_should_return_python_lang_for_python_process(self, proc):
        proc.as_dict.return_value = {'exe': '/usr/bin/python'}
        self.assertEqual('python', mon._get_impl_lang(proc))

    @mock.patch('psutil.Process')
    def test_should_return_java_lang_for_java_process(self, proc):
        proc.as_dict.return_value = {'exe': '/usr/bin/java'}
        self.assertEqual('java', mon._get_impl_lang(proc))

    @mock.patch('psutil.Process')
    def test_should_throw_error_for_unknown_impl(self, proc):
        proc.as_dict.return_value = {'exe': '/usr/bin/cat'}
        self.assertRaises(Exception, mon._get_impl_lang, proc)


class TestMonPersisterDetectionPlugin(unittest.TestCase):

    def setUp(self):
        super(TestMonPersisterDetectionPlugin, self).setUp()
        FakeProcesses.name = 'monasca-persister'
        with mock.patch.object(mon.MonPersister, '_detect') as mock_detect:
            self._mon_p = mon.MonPersister('foo')
            self.assertTrue(mock_detect.called)

    @mock.patch(
        'monasca_setup.detection.plugins.mon._MonPersisterJavaHelper')
    def test_should_use_java_helper_if_persister_is_java(self,
                                                              impl_helper):
        FakeProcesses.cmdLine = [_JAVA_CMD_PERSISTER]

        self._mon_p._init_impl_helper = iih = mock.Mock(
            return_value=impl_helper)

        self._detect()

        iih.assert_called_once_with('java')
        self.assertTrue(impl_helper.load_configuration.called_once)

    def test_should_detect_java_persister_has_config(self):
        FakeProcesses.cmdLine = [_JAVA_CMD_PERSISTER]

        yml_cfg = _JAVA_YML_CFG_BIT_PERSISTER.format(
            ah_threads=5,
            mc_threads=5,
            db_type="influxdb",
            admin_type="http",
            admin_port=6666
        )

        with mock.patch(
                "__builtin__.open",
                mock.mock_open(read_data=yml_cfg)) as mf:
            self._detect()
            mf.assert_called_once_with('/etc/monasca/persister-config.yml',
                                       'r')

        self.assertTrue(self._mon_p.available)

    @mock.patch('six.moves.configparser.RawConfigParser')
    def test_should_detect_python_persister_has_config(self, _):
        # NOTE(trebskit) this cannot use mocking the read of the file
        # because when either RawConfigParser or mock_open messes up with
        # reading the one file line. Instead returning empty line,
        # StopIteration is raised and RawConfigParser does not ignore that or
        # catch it
        #
        # to sum it up => ;-(((

        FakeProcesses.cmdLine = [_PYTHON_CMD_PERSISTER]
        self._mon_p._init_impl_helper = mock.Mock(return_value=mock.Mock())

        self._detect()
        self.assertTrue(self._mon_p.available)

    def test_build_java_config(self):
        FakeProcesses.cmdLine = [_JAVA_CMD_PERSISTER]

        # note(trebskit) this is always set to 2
        jvm_metrics_count = 2

        alarm_history_threads = random.randint(1, 5)
        metrics_history_threads = random.randint(1, 5)
        admin_port = random.randint(1000, 10000)

        admin_type = 'http'
        if admin_port % 2 != 0:
            admin_type += 's'

        yml_cfg = _JAVA_YML_CFG_BIT_PERSISTER.format(
            ah_threads=alarm_history_threads,
            mc_threads=metrics_history_threads,
            db_type="influxdb",
            admin_type=admin_type,
            admin_port=admin_port
        )

        with mock.patch(
                "__builtin__.open",
                mock.mock_open(read_data=yml_cfg)) as mf:
            self._detect()
            conf = self._build_config()
            mf.assert_called_once_with('/etc/monasca/persister-config.yml',
                                       'r')

        for key in ('http_check', 'http_metrics', 'process'):
            self.assertIn(key, conf)
            bit = conf[key]
            self.assertIsNotNone(bit)
            self.assertNotEqual({}, bit)

        # detailed assertions

        # http_check
        http_check_instance = conf['http_check']['instances'][0]
        self.assertFalse(http_check_instance['include_content'])
        self.assertEqual('monitoring-monasca-persister healthcheck',
                         http_check_instance['name'])
        self.assertEqual(5, http_check_instance['timeout'])
        self.assertEqual('%s://localhost:%d/healthcheck'
                         % (admin_type, admin_port),
                         http_check_instance['url'])

        # http_metrics
        http_metrics_instance = conf['http_metrics']['instances'][0]
        self.assertEqual('monitoring-monasca-persister metrics',
                         http_metrics_instance['name'])
        self.assertEqual('%s://localhost:%d/metrics'
                         % (admin_type, admin_port),
                         http_metrics_instance['url'])
        hmi_whitelist = http_metrics_instance['whitelist']
        self.assertIsNotNone(hmi_whitelist)
        self.assertEqual(len(hmi_whitelist), (
            alarm_history_threads +
            metrics_history_threads +
            jvm_metrics_count))

        jvm_metrics_found = 0
        ah_metrics_found = 0
        mh_metrics_found = 0

        for entry in hmi_whitelist:
            name = entry['name']
            if 'jvm' in name:
                jvm_metrics_found += 1
            elif 'alarm-state' in name:
                ah_metrics_found += 1
            elif 'metrics-added' in name:
                mh_metrics_found += 1
        self.assertEqual(jvm_metrics_count, jvm_metrics_found)
        self.assertEqual(alarm_history_threads, ah_metrics_found)
        self.assertEqual(metrics_history_threads, mh_metrics_found)

        # process
        process_instance = conf['process']['instances'][0]
        self.assertEqual('monasca-persister', process_instance['name'])
        self.assertFalse(process_instance['exact_match'])
        self.assertTrue(process_instance['detailed'])
        self.assertDictEqual({
            'component': 'monasca-persister',
            'service': 'monitoring'
        }, process_instance['dimensions'])

    def test_build_python_config(self):
        FakeProcesses.cmdLine = [_PYTHON_CMD_PERSISTER]
        self._detect()
        conf = self._build_config()

        for key in ('process',):
            self.assertIn(key, conf)
            bit = conf[key]
            self.assertIsNotNone(bit)
            self.assertNotEqual({}, bit)

        # process
        process_instance = conf['process']['instances'][0]
        self.assertEqual('monasca-persister', process_instance['name'])
        self.assertFalse(process_instance['exact_match'])
        self.assertTrue(process_instance['detailed'])
        self.assertDictEqual({
            'component': 'monasca-persister',
            'service': 'monitoring'
        }, process_instance['dimensions'])

    def _detect(self):
        self._mon_p.available = False
        process_iter = mock.patch.object(psutil, 'process_iter',
                                         return_value=[FakeProcesses()])
        with process_iter as mock_process_iter:
            self._mon_p._detect()
            self.assertTrue(mock_process_iter.called)

    def _build_config(self):
        conf = self._mon_p.build_config()
        self.assertIsNotNone(conf)
        self.assertNotEqual({}, conf)
        return conf


class TestMonAPIDetectionPlugin(unittest.TestCase):
    def setUp(self):
        FakeProcesses.name = 'monasca-api'
        super(TestMonAPIDetectionPlugin, self).setUp()
        with mock.patch.object(mon.MonAPI, '_detect') as mock_detect:
            self._mon_api = mon.MonAPI('foo')
            self.assertTrue(mock_detect.called)

    @mock.patch('monasca_setup.detection.plugins.mon._MonAPIPythonHelper')
    def test_should_use_python_helper_if_api_is_python(self, impl_helper):
        FakeProcesses.cmdLine = [_PYTHON_CMD_API]

        self._mon_api._init_impl_helper = iih = mock.Mock(
            return_value=impl_helper)

        self._detect()

        iih.assert_called_once_with('python')
        self.assertTrue(impl_helper.load_configuration.called_once)
        self.assertTrue(impl_helper.get_bound_port.called_once)

    @mock.patch('monasca_setup.detection.plugins.mon._MonAPIJavaHelper')
    def test_should_use_java_helper_if_api_is_java(self, impl_helper):
        FakeProcesses.cmdLine = [_JAVA_CMD_API]

        self._mon_api._init_impl_helper = iih = mock.Mock(
            return_value=impl_helper)

        self._detect()

        iih.assert_called_once_with('java')
        self.assertTrue(impl_helper.load_configuration.called_once)
        self.assertTrue(impl_helper.get_bound_port.called_once)

    def test_should_detect_java_api_has_config(self):
        app_port = random.randint(1000, 10000)
        admin_port = random.randint(1000, 10000)
        admin_type = 'http'
        if admin_port % 2 != 0:
            admin_type += 's'

        FakeProcesses.cmdLine = [_JAVA_CMD_API]
        FakeProcesses.inetConnections = [FakeInetConnection(app_port)]

        yml_cfg = _JAVA_YML_CFG_BIT_API.format(
            app_port=app_port,
            admin_port=admin_port,
            admin_type=admin_type,
            hibernate_enabled=False
        )

        with mock.patch(
                "__builtin__.open",
                mock.mock_open(read_data=yml_cfg)) as mock_file:
            self._detect()
            mock_file.assert_called_once_with('/etc/monasca/api-config.yml',
                                              'r')

        self.assertTrue(self._mon_api.available)

    @mock.patch('six.moves.configparser.RawConfigParser')
    def test_should_detect_python_api_has_config(self, rcp):
        expected_port = 6666
        actual_port = 6666

        FakeProcesses.cmdLine = [_PYTHON_CMD_API]
        FakeProcesses.inetConnections = [FakeInetConnection(actual_port)]

        # make sure we return the port as we would read from the cfg
        rcp.getint.return_value = expected_port

        # override configuration to make sure we read correct port
        impl_helper = mon._MonAPIPythonHelper()
        impl_helper._paste_config = rcp
        impl_helper.load_configuration = mock.Mock()

        self._mon_api._init_impl_helper = mock.Mock(return_value=impl_helper)

        self._detect()
        self.assertTrue(self._mon_api.available)

    @mock.patch('six.moves.configparser.RawConfigParser')
    def test_should_not_detect_if_port_dont_match(self, rcp):
        expected_port = 6666
        actual_port = 8070

        # assume having python implementation
        FakeProcesses.cmdLine = [_PYTHON_CMD_API]
        FakeProcesses.inetConnections = [FakeInetConnection(actual_port)]

        # make sure we return the port as we would read from the cfg
        rcp.getint.return_value = expected_port

        # override configuration to make sure we read correct port
        impl_helper = mon._MonAPIPythonHelper()
        impl_helper._paste_config = rcp
        impl_helper.load_configuration = mock.Mock()

        self._mon_api._init_impl_helper = mock.Mock(return_value=impl_helper)

        with mock.patch.object(LOG, 'error') as mock_log_error:
            self._detect()
            self.assertFalse(self._mon_api.available)
            mock_log_error.assert_called_with('monasca-api is not listening '
                                              'on port %d. Plugin for '
                                              'monasca-api will not '
                                              'be configured.' % expected_port)

    def test_build_java_config_no_hibernate(self):
        self._run_java_build_config(False)

    def test_build_java_config_with_hibernate(self):
        self._run_java_build_config(True)

    @mock.patch('six.moves.configparser.RawConfigParser')
    def test_build_python_config(self, rcp):
        expected_port = 8070

        FakeProcesses.cmdLine = [_PYTHON_CMD_API]
        FakeProcesses.inetConnections = [FakeInetConnection(expected_port)]

        rcp.getint.return_value = expected_port

        impl_helper = mon._MonAPIPythonHelper()
        impl_helper._paste_config = rcp
        impl_helper.load_configuration = mock.Mock()

        self._mon_api._init_impl_helper = mock.Mock(return_value=impl_helper)

        self._detect()
        conf = self._build_config()

        for key in ('process', ):
            self.assertIn(key, conf)
            bit = conf[key]
            self.assertIsNotNone(bit)
            self.assertNotEqual({}, bit)

    def _run_java_build_config(self, hibernate_enabled):
        FakeProcesses.cmdLine = [_JAVA_CMD_API]
        app_port = random.randint(1000, 10000)
        admin_port = random.randint(1000, 10000)
        admin_type = 'http'
        if admin_port % 2 != 0:
            admin_type += 's'

        FakeProcesses.cmdLine = [_JAVA_CMD_API]
        FakeProcesses.inetConnections = [FakeInetConnection(app_port)]

        # note(trebskit) this is always set to 2
        jvm_metrics_count = 2
        internal_metrics_count = 1
        sql_timers_count = 2

        total_metrics_count = jvm_metrics_count + internal_metrics_count + (
            sql_timers_count if not hibernate_enabled else 0)

        yml_cfg = _JAVA_YML_CFG_BIT_API.format(
            app_port=app_port,
            admin_port=admin_port,
            admin_type=admin_type,
            hibernate_enabled=hibernate_enabled
        )

        with mock.patch(
                "__builtin__.open",
                mock.mock_open(read_data=yml_cfg)) as mf:
            self._detect()
            conf = self._build_config()
            mf.assert_called_once_with('/etc/monasca/api-config.yml',
                                       'r')

        for key in ('http_check', 'http_metrics', 'process'):
            self.assertIn(key, conf)
            bit = conf[key]
            self.assertIsNotNone(bit)
            self.assertNotEqual({}, bit)

        # verify http_check
        http_check_instance = conf['http_check']['instances'][0]
        self.assertFalse(http_check_instance['include_content'])
        self.assertEqual('monitoring-monasca-api healthcheck',
                         http_check_instance['name'])
        self.assertEqual(5, http_check_instance['timeout'])
        self.assertEqual('%s://localhost:%d/healthcheck'
                         % (admin_type, admin_port),
                         http_check_instance['url'])

        # verify http_metrics
        http_metrics_instance = conf['http_metrics']['instances'][0]
        self.assertEqual('monitoring-monasca-api metrics',
                         http_metrics_instance['name'])
        self.assertEqual('%s://localhost:%d/metrics'
                         % (admin_type, admin_port),
                         http_metrics_instance['url'])
        hmi_whitelist = http_metrics_instance['whitelist']
        self.assertIsNotNone(hmi_whitelist)
        self.assertEqual(len(hmi_whitelist), total_metrics_count)

        jvm_metrics_found = 0
        internal_metrics_found = 0
        sql_timers_metrics_found = 0

        for entry in hmi_whitelist:
            name = entry['name']
            if 'jvm' in name:
                jvm_metrics_found += 1
            elif 'metrics.published' in name:
                internal_metrics_found += 1
            elif 'raw-sql.time' in name:
                sql_timers_metrics_found += 1

        self.assertEqual(jvm_metrics_count, jvm_metrics_found)
        self.assertEqual(internal_metrics_count, internal_metrics_found)
        if not hibernate_enabled:
            self.assertEqual(sql_timers_count, sql_timers_count)

    def _build_config(self):
        conf = self._mon_api.build_config()
        self.assertIsNotNone(conf)
        self.assertNotEqual({}, conf)
        return conf

    def _detect(self):
        self._mon_api.available = False

        process_iter = mock.patch.object(psutil, 'process_iter',
                                         return_value=[FakeProcesses()])

        with process_iter as mock_process_iter:
            self._mon_api._detect()
            self.assertTrue(mock_process_iter.called)
