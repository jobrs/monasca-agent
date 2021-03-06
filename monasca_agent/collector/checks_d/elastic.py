# (C) Copyright 2015,2016 Hewlett Packard Enterprise Development Company LP

from collections import defaultdict
import json
import socket
import subprocess
import sys
import urllib2
import urlparse

import time

from monasca_agent.collector.checks import AgentCheck
from monasca_agent.collector.checks.utils import add_basic_auth
from monasca_agent.common.util import headers


def div1000(v):
    return float(v) / 1000


class NodeNotFound(Exception):
    pass


class ElasticSearch(AgentCheck):
    METRICS = {  # Metrics that are common to all Elasticsearch versions
        "elasticsearch.docs.count": ("gauge", "indices.docs.count"),
        "elasticsearch.docs.deleted": ("gauge", "indices.docs.deleted"),
        "elasticsearch.store.size": ("gauge", "indices.store.size_in_bytes"),
        "elasticsearch.indexing.index.total": ("gauge", "indices.indexing.index_total"),
        "elasticsearch.indexing.index.time": ("gauge", "indices.indexing.index_time_in_millis",
                                              div1000),
        "elasticsearch.indexing.index.current": ("gauge", "indices.indexing.index_current"),
        "elasticsearch.indexing.delete.total": ("gauge", "indices.indexing.delete_total"),
        "elasticsearch.indexing.delete.time": ("gauge", "indices.indexing.delete_time_in_millis", div1000),
        "elasticsearch.indexing.delete.current": ("gauge", "indices.indexing.delete_current"),
        "elasticsearch.get.total": ("gauge", "indices.get.total"),
        "elasticsearch.get.time": ("gauge", "indices.get.time_in_millis", div1000),
        "elasticsearch.get.current": ("gauge", "indices.get.current"),
        "elasticsearch.get.exists.total": ("gauge", "indices.get.exists_total"),
        "elasticsearch.get.exists.time": ("gauge", "indices.get.exists_time_in_millis", div1000),
        "elasticsearch.get.missing.total": ("gauge", "indices.get.missing_total"),
        "elasticsearch.get.missing.time": ("gauge", "indices.get.missing_time_in_millis", div1000),
        "elasticsearch.search.query.total": ("gauge", "indices.search.query_total"),
        "elasticsearch.search.query.time": ("gauge", "indices.search.query_time_in_millis", div1000),
        "elasticsearch.search.query.current": ("gauge", "indices.search.query_current"),
        "elasticsearch.search.fetch.total": ("gauge", "indices.search.fetch_total"),
        "elasticsearch.search.fetch.time": ("gauge", "indices.search.fetch_time_in_millis", div1000),
        "elasticsearch.search.fetch.current": ("gauge", "indices.search.fetch_current"),
        "elasticsearch.indices.segments.count": ("gauge", "indices.segments.count"),
        "elasticsearch.indices.segments.memory_in_bytes": ("gauge", "indices.segments.memory_in_bytes"),
        "elasticsearch.merges.current": ("gauge", "indices.merges.current"),
        "elasticsearch.merges.current.docs": ("gauge", "indices.merges.current_docs"),
        "elasticsearch.merges.current.size": ("gauge", "indices.merges.current_size_in_bytes"),
        "elasticsearch.merges.total": ("gauge", "indices.merges.total"),
        "elasticsearch.merges.total.time": ("gauge", "indices.merges.total_time_in_millis", div1000),
        "elasticsearch.merges.total.docs": ("gauge", "indices.merges.total_docs"),
        "elasticsearch.merges.total.size": ("gauge", "indices.merges.total_size_in_bytes"),
        "elasticsearch.refresh.total": ("gauge", "indices.refresh.total"),
        "elasticsearch.refresh.total.time": ("gauge", "indices.refresh.total_time_in_millis", div1000),
        "elasticsearch.flush.total": ("gauge", "indices.flush.total"),
        "elasticsearch.flush.total.time": ("gauge", "indices.flush.total_time_in_millis", div1000),
        "elasticsearch.process.open_fd": ("gauge", "process.open_file_descriptors"),
        "elasticsearch.transport.rx_count": ("gauge", "transport.rx_count"),
        "elasticsearch.transport.tx_count": ("gauge", "transport.tx_count"),
        "elasticsearch.transport.rx_size": ("gauge", "transport.rx_size_in_bytes"),
        "elasticsearch.transport.tx_size": ("gauge", "transport.tx_size_in_bytes"),
        "elasticsearch.transport.server_open": ("gauge", "transport.server_open"),
        "elasticsearch.thread_pool.bulk.active": ("gauge", "thread_pool.bulk.active"),
        "elasticsearch.thread_pool.bulk.threads": ("gauge", "thread_pool.bulk.threads"),
        "elasticsearch.thread_pool.bulk.queue": ("gauge", "thread_pool.bulk.queue"),
        "elasticsearch.thread_pool.bulk.rejected": ("gauge", "thread_pool.bulk.rejected"),
        "elasticsearch.thread_pool.flush.active": ("gauge", "thread_pool.flush.active"),
        "elasticsearch.thread_pool.flush.threads": ("gauge", "thread_pool.flush.threads"),
        "elasticsearch.thread_pool.flush.queue": ("gauge", "thread_pool.flush.queue"),
        "elasticsearch.thread_pool.generic.active": ("gauge", "thread_pool.generic.active"),
        "elasticsearch.thread_pool.generic.threads": ("gauge", "thread_pool.generic.threads"),
        "elasticsearch.thread_pool.generic.queue": ("gauge", "thread_pool.generic.queue"),
        "elasticsearch.thread_pool.get.active": ("gauge", "thread_pool.get.active"),
        "elasticsearch.thread_pool.get.threads": ("gauge", "thread_pool.get.threads"),
        "elasticsearch.thread_pool.get.queue": ("gauge", "thread_pool.get.queue"),
        "elasticsearch.thread_pool.index.active": ("gauge", "thread_pool.index.active"),
        "elasticsearch.thread_pool.index.threads": ("gauge", "thread_pool.index.threads"),
        "elasticsearch.thread_pool.index.queue": ("gauge", "thread_pool.index.queue"),
        "elasticsearch.thread_pool.management.active": ("gauge", "thread_pool.management.active"),
        "elasticsearch.thread_pool.management.threads": ("gauge", "thread_pool.management.threads"),
        "elasticsearch.thread_pool.management.queue": ("gauge", "thread_pool.management.queue"),
        "elasticsearch.thread_pool.percolate.active": ("gauge", "thread_pool.percolate.active"),
        "elasticsearch.thread_pool.percolate.threads": ("gauge", "thread_pool.percolate.threads"),
        "elasticsearch.thread_pool.percolate.queue": ("gauge", "thread_pool.percolate.queue"),
        "elasticsearch.thread_pool.refresh.active": ("gauge", "thread_pool.refresh.active"),
        "elasticsearch.thread_pool.refresh.threads": ("gauge", "thread_pool.refresh.threads"),
        "elasticsearch.thread_pool.refresh.queue": ("gauge", "thread_pool.refresh.queue"),
        "elasticsearch.thread_pool.search.active": ("gauge", "thread_pool.search.active"),
        "elasticsearch.thread_pool.search.threads": ("gauge", "thread_pool.search.threads"),
        "elasticsearch.thread_pool.search.queue": ("gauge", "thread_pool.search.queue"),
        "elasticsearch.thread_pool.snapshot.active": ("gauge", "thread_pool.snapshot.active"),
        "elasticsearch.thread_pool.snapshot.threads": ("gauge", "thread_pool.snapshot.threads"),
        "elasticsearch.thread_pool.snapshot.queue": ("gauge", "thread_pool.snapshot.queue"),
        "elasticsearch.http.current_open": ("gauge", "http.current_open"),
        "elasticsearch.http.total_opened": ("gauge", "http.total_opened"),
        "jvm.mem.heap_committed": ("gauge", "jvm.mem.heap_committed_in_bytes"),
        "jvm.mem.heap_used": ("gauge", "jvm.mem.heap_used_in_bytes"),
        "jvm.mem.heap_in_use": ("gauge", "jvm.mem.heap_used_percent"),
        "jvm.mem.heap_max": ("gauge", "jvm.mem.heap_max_in_bytes"),
        "jvm.mem.non_heap_committed": ("gauge", "jvm.mem.non_heap_committed_in_bytes"),
        "jvm.mem.non_heap_used": ("gauge", "jvm.mem.non_heap_used_in_bytes"),
        "jvm.threads.count": ("gauge", "jvm.threads.count"),
        "jvm.threads.peak_count": ("gauge", "jvm.threads.peak_count"),
        "elasticsearch.fs.total.total_in_bytes": ("gauge", "fs.total.total_in_bytes"),
        "elasticsearch.fs.total.free_in_bytes": ("gauge", "fs.total.free_in_bytes"),
        "elasticsearch.fs.total.available_in_bytes": ("gauge", "fs.total.available_in_bytes"),
        "elasticsearch.pending_tasks_total": ("gauge", "pending_task_total"),
        "elasticsearch.pending_tasks_priority_high": ("gauge", "pending_tasks_priority_high"),
        "elasticsearch.pending_tasks_priority_urgent": ("gauge", "pending_tasks_priority_urgent")
    }
    PRIMARY_SHARD_METRICS = {
        "elasticsearch.primaries.docs.count": ("gauge", "_all.primaries.docs.count"),
        "elasticsearch.primaries.docs.deleted": ("gauge", "_all.primaries.docs.deleted"),
        "elasticsearch.primaries.store.size": ("gauge", "_all.primaries.store.size_in_bytes"),
        "elasticsearch.primaries.indexing.index.total": ("gauge", "_all.primaries.indexing.index_total"),
        "elasticsearch.primaries.indexing.index.time": ("gauge", "_all.primaries.indexing.index_time_in_millis",
                                                        div1000),
        "elasticsearch.primaries.indexing.index.current": ("gauge", "_all.primaries.indexing.index_current"),
        "elasticsearch.primaries.indexing.delete.total": ("gauge", "_all.primaries.indexing.delete_total"),
        "elasticsearch.primaries.indexing.delete.time": ("gauge", "_all.primaries.indexing.delete_time_in_millis",
                                                         div1000),
        "elasticsearch.primaries.indexing.delete.current": ("gauge", "_all.primaries.indexing.delete_current"),
        "elasticsearch.primaries.get.total": ("gauge", "_all.primaries.get.total"),
        "elasticsearch.primaries.get.time": ("gauge", "_all.primaries.get.time_in_millis", div1000),
        "elasticsearch.primaries.get.current": ("gauge", "_all.primaries.get.current"),
        "elasticsearch.primaries.get.exists.total": ("gauge", "_all.primaries.get.exists_total"),
        "elasticsearch.primaries.get.exists.time": ("gauge", "_all.primaries.get.exists_time_in_millis", div1000),
        "elasticsearch.primaries.get.missing.total": ("gauge", "_all.primaries.get.missing_total"),
        "elasticsearch.primaries.get.missing.time": ("gauge", "_all.primaries.get.missing_time_in_millis", div1000),
        "elasticsearch.primaries.search.query.total": ("gauge", "_all.primaries.search.query_total"),
        "elasticsearch.primaries.search.query.time": ("gauge", "_all.primaries.search.query_time_in_millis", div1000),
        "elasticsearch.primaries.search.query.current": ("gauge", "_all.primaries.search.query_current"),
        "elasticsearch.primaries.search.fetch.total": ("gauge", "_all.primaries.search.fetch_total"),
        "elasticsearch.primaries.search.fetch.time": ("gauge", "_all.primaries.search.fetch_time_in_millis", div1000),
        "elasticsearch.primaries.search.fetch.current": ("gauge", "_all.primaries.search.fetch_current"),
        "elasticsearch.number_of_nodes": ("gauge", "number_of_nodes"),
        "elasticsearch.number_of_data_nodes": ("gauge", "number_of_data_nodes"),
        "elasticsearch.active_primary_shards": ("gauge", "active_primary_shards"),
        "elasticsearch.active_shards": ("gauge", "active_shards"),
        "elasticsearch.relocating_shards": ("gauge", "relocating_shards"),
        "elasticsearch.initializing_shards": ("gauge", "initializing_shards"),
        "elasticsearch.unassigned_shards": ("gauge", "unassigned_shards"),
        "elasticsearch.cluster_status": ("gauge", "status", lambda v: {"red": 0, "yellow": 1, "green": 2}.get(v, -1)),
    }

    PRIMARY_SHARD_METRICS_POST_1_0 = {
        "elasticsearch.primaries.merges.current": ("gauge", "_all.primaries.merges.current"),
        "elasticsearch.primaries.merges.current.docs": ("gauge", "_all.primaries.merges.current_docs"),
        "elasticsearch.primaries.merges.current.size": ("gauge", "_all.primaries.merges.current_size_in_bytes"),
        "elasticsearch.primaries.merges.total": ("gauge", "_all.primaries.merges.total"),
        "elasticsearch.primaries.merges.total.time": ("gauge", "_all.primaries.merges.total_time_in_millis", div1000),
        "elasticsearch.primaries.merges.total.docs": ("gauge", "_all.primaries.merges.total_docs"),
        "elasticsearch.primaries.merges.total.size": ("gauge", "_all.primaries.merges.total_size_in_bytes"),
        "elasticsearch.primaries.refresh.total": ("gauge", "_all.primaries.refresh.total"),
        "elasticsearch.primaries.refresh.total.time": ("gauge", "_all.primaries.refresh.total_time_in_millis", div1000),
        "elasticsearch.primaries.flush.total": ("gauge", "_all.primaries.flush.total"),
        "elasticsearch.primaries.flush.total.time": ("gauge", "_all.primaries.flush.total_time_in_millis", div1000),
    }

    JVM_METRICS_POST_0_90_10 = {
        "jvm.gc.collectors.young.count": ("gauge", "jvm.gc.collectors.young.collection_count"),
        "jvm.gc.collectors.young.collection_time": ("gauge", "jvm.gc.collectors.young.collection_time_in_millis",
                                                    div1000),
        "jvm.gc.collectors.old.count": ("gauge", "jvm.gc.collectors.old.collection_count"),
        "jvm.gc.collectors.old.collection_time": ("gauge", "jvm.gc.collectors.old.collection_time_in_millis", div1000),
    }

    JVM_METRICS_PRE_0_90_10 = {
        "jvm.gc.concurrent_mark_sweep.count": ("gauge", "jvm.gc.collectors.ConcurrentMarkSweep.collection_count"),
        "jvm.gc.concurrent_mark_sweep.collection_time":
            ("gauge", "jvm.gc.collectors.ConcurrentMarkSweep.collection_time_in_millis", div1000),
        "jvm.gc.par_new.count": ("gauge", "jvm.gc.collectors.ParNew.collection_count"),
        "jvm.gc.par_new.collection_time": ("gauge", "jvm.gc.collectors.ParNew.collection_time_in_millis", div1000),
        "jvm.gc.collection_count": ("gauge", "jvm.gc.collection_count"),
        "jvm.gc.collection_time": ("gauge", "jvm.gc.collection_time_in_millis", div1000),
    }

    ADDITIONAL_METRICS_POST_0_90_5 = {
        "elasticsearch.search.fetch.open_contexts": ("gauge", "indices.search.open_contexts"),
        "elasticsearch.fielddata.size": ("gauge", "indices.fielddata.memory_size_in_bytes"),
        "elasticsearch.fielddata.evictions": ("gauge", "indices.fielddata.evictions"),
    }

    ADDITIONAL_METRICS_POST_0_90_5_PRE_2_0 = {
        "elasticsearch.cache.filter.evictions": ("gauge", "indices.filter_cache.evictions"),
        "elasticsearch.cache.filter.size": ("gauge", "indices.filter_cache.memory_size_in_bytes"),
        "elasticsearch.id_cache.size": ("gauge", "indices.id_cache.memory_size_in_bytes"),
    }

    ADDITIONAL_METRICS_PRE_0_90_5 = {
        "elasticsearch.cache.field.evictions": ("gauge", "indices.cache.field_evictions"),
        "elasticsearch.cache.field.size": ("gauge", "indices.cache.field_size_in_bytes"),
        "elasticsearch.cache.filter.count": ("gauge", "indices.cache.filter_count"),
        "elasticsearch.cache.filter.evictions": ("gauge", "indices.cache.filter_evictions"),
        "elasticsearch.cache.filter.size": ("gauge", "indices.cache.filter_size_in_bytes"),
    }

    ADDITIONAL_METRICS_POST_1_0_0 = {
        "elasticsearch.indices.translog.size_in_bytes": ("gauge", "indices.translog.size_in_bytes"),
        "elasticsearch.indices.translog.operations": ("gauge", "indices.translog.operations"),
    }

    ADDITIONAL_METRICS_1_x = {  # Stats are only valid for v1.x
        "elasticsearch.fs.total.disk_reads": ("rate", "fs.total.disk_reads"),
        "elasticsearch.fs.total.disk_writes": ("rate", "fs.total.disk_writes"),
        "elasticsearch.fs.total.disk_io_op": ("rate", "fs.total.disk_io_op"),
        "elasticsearch.fs.total.disk_read_size_in_bytes": ("gauge", "fs.total.disk_read_size_in_bytes"),
        "elasticsearch.fs.total.disk_write_size_in_bytes": ("gauge", "fs.total.disk_write_size_in_bytes"),
        "elasticsearch.fs.total.disk_io_size_in_bytes": ("gauge", "fs.total.disk_io_size_in_bytes"),
    }

    ADDITIONAL_METRICS_POST_1_3_0 = {
        "elasticsearch.indices.segments.index_writer_memory_in_bytes":
            ("gauge", "indices.segments.index_writer_memory_in_bytes"),
        "elasticsearch.indices.segments.version_map_memory_in_bytes":
            ("gauge", "indices.segments.version_map_memory_in_bytes"),
    }

    ADDITIONAL_METRICS_POST_1_4_0 = {
        "elasticsearch.indices.segments.index_writer_max_memory_in_bytes":
            ("gauge", "indices.segments.index_writer_max_memory_in_bytes"),
        "elasticsearch.indices.segments.fixed_bit_set_memory_in_bytes":
            ("gauge", "indices.segments.fixed_bit_set_memory_in_bytes"),
    }

    ADDITIONAL_METRICS_PRE_2_0 = {
        "elasticsearch.thread_pool.merge.active": ("gauge", "thread_pool.merge.active"),
        "elasticsearch.thread_pool.merge.threads": ("gauge", "thread_pool.merge.threads"),
        "elasticsearch.thread_pool.merge.queue": ("gauge", "thread_pool.merge.queue"),
    }

    CLUSTER_PENDING_TASKS = {
        "elasticsearch.pending_tasks_total": ("gauge", "pending_task_total"),
        "elasticsearch.pending_tasks_priority_high": ("gauge", "pending_tasks_priority_high"),
        "elasticsearch.pending_tasks_priority_urgent": ("gauge", "pending_tasks_priority_urgent")
    }

    def __init__(self, name, init_config, agent_config, instances):
        AgentCheck.__init__(self, name, init_config, agent_config, instances)

        # Host status needs to persist across all checks
        self.cluster_status = {}

    def check(self, instance):
        config_url = instance.get('url')
        if config_url is None:
            raise Exception("An url must be specified")

        # Load basic authentication configuration, if available.
        username, password = instance.get('username'), instance.get('password')
        if username and password:
            auth = (username, password)
        else:
            auth = None

        # Support URLs that have a path in them from the config, for
        # backwards-compatibility.
        parsed = urlparse.urlparse(config_url)
        if parsed[2] != "":
            config_url = "%s://%s" % (parsed[0], parsed[1])

        # Option: skip check if elasticsearch is not listening
        if instance.get('skip_unavail', False):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex((parsed.hostname, parsed.port))
            sock.close()
            if result != 0:
                self.log.info("ElasticSearch not available at %s:%d skipping check", parsed.hostname, parsed.port)
                return

        # Tag by URL so we can differentiate the metrics from multiple instances
        dimensions = self._set_dimensions({'url': config_url}, instance)

        # Check ES version for this instance and define parameters (URLs and metrics) accordingly
        version = self._get_es_version(config_url, auth)
        self._define_params(version)

        # Load stats data.
        url = urlparse.urljoin(config_url, self.STATS_URL)
        stats_data = self._get_data(url, auth)
#        self.log.debug("stats_data: %s" % stats_data)
        self._process_stats_data(config_url, stats_data, auth, dimensions=dimensions)

        # Load the health data.
        url = urlparse.urljoin(config_url, self.HEALTH_URL)
        health_data = self._get_data(url, auth)
#        self.log.debug("health_data: %s" % health_data)
        self._process_health_data(config_url, health_data, dimensions=dimensions)

        # Load the task data.
        url = urlparse.urljoin(config_url, self.TASK_URL)
        task_data = self._get_data(url, auth)
#        self.log.debug("task_data: %s" % task_data)
        self._process_task_data(task_data, dimensions=dimensions)

    def _get_es_version(self, config_url, auth=None):
        """Get the running version of Elastic Search.

        """

        try:
            data = self._get_data(config_url, auth)
            version = map(int, data['version']['number'].split('.'))
        except Exception as e:
            self.log.warn("Error while trying to get Elasticsearch version from %s %s" %
                          (config_url, str(e)))
            version = [0, 0, 0]

        self.log.debug("Elasticsearch version is %s" % version)
        return version

    def _define_params(self, version):
        """Define the set of URLs and METRICS to use depending on the running ES version.

        """

        if version >= [0, 90, 5]:
            # ES versions 0.90.5 and above
            additional_metrics = self.ADDITIONAL_METRICS_POST_0_90_5
        else:
            # ES version 0.90.4 and below
            additional_metrics = self.ADDITIONAL_METRICS_PRE_0_90_5

        self.METRICS.update(additional_metrics)
        
        if version <= [0, 90, 0]:
            # ES version 0.90.9 and below
            self.HEALTH_URL = "/_cluster/health?pretty=true"
            self.STATS_URL = "/_cluster/nodes/stats?all=true"
            self.NODES_URL = "/_cluster/nodes?network=true"

            additional_metrics = self.JVM_METRICS_PRE_0_90_10

        if version < [5, 0, 0]:
            # ES versions 0.90.10 and above
            # Metrics architecture changed starting with version 0.90.10
            self.HEALTH_URL = "/_cluster/health?pretty=true"
            self.STATS_URL = "/_nodes/stats?all=true"
            self.NODES_URL = "/_nodes?network=true"
            self.TASK_URL = "/_cluster/pending_tasks?pretty=true"

            additional_metrics = self.JVM_METRICS_POST_0_90_10

        else:
            # ES versions 5.0.0 and above
            # Metrics architecture changed starting with version 0.90.10
            self.HEALTH_URL = "/_cluster/health?pretty=true"
            self.STATS_URL = "/_nodes/stats"
            self.NODES_URL = "/_nodes?network=true"
            self.TASK_URL = "/_cluster/pending_tasks?pretty=true"

            additional_metrics = self.JVM_METRICS_POST_0_90_10

        self.METRICS.update(additional_metrics)

        if version >= [1, 0, 0]:
            self.METRICS.update(self.ADDITIONAL_METRICS_POST_1_0_0)

        if version < [2, 0, 0]:
            self.METRICS.update(self.ADDITIONAL_METRICS_PRE_2_0)
            if version >= [0, 90, 5]:
                self.METRICS.update(self.ADDITIONAL_METRICS_POST_0_90_5_PRE_2_0)
            if version >= [1, 0, 0]:
                self.METRICS.update(self.ADDITIONAL_METRICS_1_x)

        if version >= [1, 3, 0]:
            self.METRICS.update(self.ADDITIONAL_METRICS_POST_1_3_0)

        if version >= [1, 4, 0]:
            # ES versions 1.4 and above
            additional_metrics = self.ADDITIONAL_METRICS_POST_1_4_0
            self.METRICS.update(additional_metrics)

        # Version specific stats metrics about the primary shards
        additional_metrics = self.PRIMARY_SHARD_METRICS
        self.METRICS.update(additional_metrics)

        if version >= [1, 0, 0]:
            additional_metrics = self.PRIMARY_SHARD_METRICS_POST_1_0
            self.METRICS.update(additional_metrics)

#        self.log.debug("METRICS: %s" % self.METRICS)

    def _get_data(self, url, auth=None):
        """Hit a given URL and return the parsed json

        `auth` is a tuple of (username, password) or None
        """
        req = urllib2.Request(url, None, headers(self.agent_config))
        if auth:
            add_basic_auth(req, *auth)
        request = urllib2.urlopen(req)
        response = request.read()
        return json.loads(response)

    def _process_stats_data(self, config_url, data, auth, dimensions=None):
        for node in data['nodes']:
            node_data = data['nodes'][node]
#            self.log.debug("node_data: %s" % node_data)

            # noinspection PyUnusedLocal
            def process_metric(pmetric, xtype, path, xform=None):
                # closure over node_data
                self._process_metric(node_data, pmetric, path, xform, dimensions=dimensions)

            # On newer version of ES it's "host" not "hostname"
            node_hostname = node_data.get('hostname', node_data.get('host', None))
#            self.log.debug("node_hostname: %s" % node_hostname)

            if node_hostname is not None:
                # For ES >= 0.19
                hostnames = (
                    self.hostname.decode('utf-8'),
                    socket.gethostname().decode('utf-8'),
                    socket.getfqdn().decode('utf-8'),
                    socket.gethostbyname(socket.gethostname()).decode('utf-8')
                )
                self.log.debug("hostnames converted: %s", hostnames)
#                if node_hostname.decode('utf-8') in hostnames:
                if node_hostname in hostnames:
                    for metric in self.METRICS:
                        # metric description
                        desc = self.METRICS[metric]
                        process_metric(metric, *desc)
                else:
                    self.log.debug("metrics ignored: unknown host %s", node_hostname)

            else:
                # ES < 0.19
                # Fetch interface address from ifconfig or ip addr and check
                # against the primary IP from ES
                try:
                    nodes_url = urlparse.urljoin(config_url, self.NODES_URL)
                    primary_addr = self._get_primary_addr(nodes_url, node, auth)
                except NodeNotFound:
                    # Skip any nodes that aren't found
                    continue
                if self._host_matches_node(primary_addr):
                    for metric in self.METRICS:
                        # metric description
                        desc = self.METRICS[metric]
                        process_metric(metric, *desc)

    def _get_primary_addr(self, url, node_name, auth):
        """Returns a list of primary interface addresses as seen by ES.

        Used in ES < 0.19
        """
        req = urllib2.Request(url, None, headers(self.agent_config))
        # Load basic authentication configuration, if available.
        if auth:
            add_basic_auth(req, *auth)
        request = urllib2.urlopen(req)
        response = request.read()
        data = json.loads(response)

        if node_name in data['nodes']:
            node = data['nodes'][node_name]
            if 'network' in node\
                    and 'primary_interface' in node['network']\
                    and 'address' in node['network']['primary_interface']:
                return node['network']['primary_interface']['address']

        raise NodeNotFound()

    @staticmethod
    def _host_matches_node(primary_addrs):
        """For < 0.19, check if the current host matches the IP given in the

        cluster nodes check `/_cluster/nodes`. Uses `ip addr` on Linux and
        `ifconfig` on Mac
        """
        if sys.platform == 'darwin':
            ifaces = subprocess.Popen(['ifconfig'], stdout=subprocess.PIPE)
        else:
            ifaces = subprocess.Popen(['ip', 'addr'], stdout=subprocess.PIPE)
        grepper = subprocess.Popen(['grep', 'inet'], stdin=ifaces.stdout,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        ifaces.stdout.close()
        out, err = grepper.communicate()

        # Capture the list of interface IPs
        ips = []
        for iface in out.split("\n"):
            iface = iface.strip()
            if iface:
                ips.append(iface.split(' ')[1].split('/')[0])

        # Check the interface addresses against the primary address
        return primary_addrs in ips

    def _process_metric(self, data, metric, path, xform=None, dimensions=None):
        """data: dictionary containing all the stats

        metric: datadog metric
        path: corresponding path in data, flattened, e.g. thread_pool.bulk.queue
        xfom: a lambda to apply to the numerical value
        """
        value = data

        # Traverse the nested dictionaries
        for key in path.split('.'):
            if value is not None:
                value = value.get(key, None)
#                self.log.debug("value: %s" % value)
            else:
                break

        if value is not None:
            if xform:
                value = xform(value)
#                self.log.debug("valuexform: %s" % value)
            if self.METRICS[metric][0] == "gauge":
                self.gauge(metric, value, dimensions=dimensions)
            else:
                self.rate(metric, value, dimensions=dimensions)
        else:
            self._metric_not_found(metric, path)

    def _process_health_data(self, config_url, data, dimensions=None):
        if self.cluster_status.get(config_url, None) is None:
            self.cluster_status[config_url] = data['status']

        if data['status'] != self.cluster_status.get(config_url):
            self.cluster_status[config_url] = data['status']

        # noinspection PyUnusedLocal
        def process_metric(pmetric, xtype, path, xform=None):
            # closure over data
            self._process_metric(data, pmetric, path, xform, dimensions=dimensions)

        for metric in self.METRICS:
            # metric description
            desc = self.METRICS[metric]
            process_metric(metric, *desc)

    def _metric_not_found(self, metric, path):
        self.log.debug("Metric not found: %s -> %s", path, metric)

    def _process_task_data(self, data, dimensions=None):
        p_tasks = defaultdict(int)

        for task in data.get('tasks', []):
            p_tasks[task.get('priority')] += 1

        node_data = {
            'pending_task_total': sum(p_tasks.values()),
            'pending_tasks_priority_high': p_tasks['high'],
            'pending_tasks_priority_urgent': p_tasks['urgent'],
        }

        # noinspection PyUnusedLocal,PyUnusedLocal
        def process_metric(pmetric, xtype, path, xform=None):
            # closure over data
            self._process_metric(node_data, pmetric, path, dimensions=dimensions)

        for metric in self.CLUSTER_PENDING_TASKS:
            # metric description
            desc = self.CLUSTER_PENDING_TASKS[metric]
            process_metric(metric, *desc)

    def _create_event(self, status):
        hostname = self.hostname.decode('utf-8')
        if status == "red":
            alert_type = "error"
            msg_title = "%s is %s" % (hostname, status)

        elif status == "yellow":
            alert_type = "warning"
            msg_title = "%s is %s" % (hostname, status)

        else:
            # then it should be green
            alert_type = "success"
            msg_title = "%s recovered as %s" % (hostname, status)

        msg = "ElasticSearch: %s just reported as %s" % (hostname, status)

        return {'timestamp': int(time.time()),
                'event_type': 'elasticsearch',
                'host': hostname,
                'msg_text': msg,
                'msg_title': msg_title,
                "alert_type": alert_type,
                "source_type_name": "elasticsearch",
                "event_object": hostname
                }
