# Copyright 2016 SAP SE.
#
# Inspired by a similar DataDog plugin which is available under the Simplified
# BSD License (Copyright Datadog, Inc. 2010-2016), source at
# https://github.com/DataDog/dd-agent/blob/74e6fe99e9bed9ef063662ff0246c7a73249b1c5/checks.d/network.py

import logging
import subprocess

import monasca_agent.collector.checks as checks

log = logging.getLogger(__name__)


class NetworkConnections(checks.AgentCheck):

    GAUGES = [
        # .format(ip_version)
        'net.udp{0}.connections',
        'net.tcp{0}.established',
        'net.tcp{0}.opening',
        'net.tcp{0}.closing',
        'net.tcp{0}.listening',
        'net.tcp{0}.time_wait',
    ]

    TCP_STATES = {
        "ESTAB": "established",
        "SYN-SENT": "opening",
        "SYN-RECV": "opening",
        "FIN-WAIT-1": "closing",
        "FIN-WAIT-2": "closing",
        "TIME-WAIT": "time_wait",
        "UNCONN": "closing",
        "CLOSE-WAIT": "closing",
        "LAST-ACK": "closing",
        "LISTEN": "listening",
        "CLOSING": "closing",
    }

    def check(self, instance):
        dimensions = self._set_dimensions(None, instance)
        count = 0
        try:
            for ip_version in [4, 6]:
                count += self._collect_metrics(ip_version, dimensions)
        except OSError:
            log.error("Error collecting connection stats. Is `ss` installed?")
        log.info("Reported {} network connection counts.".format(count))

    def _collect_metrics(self, ip_version, dimensions):
        cmd = ["ss", "-nuta{}".format(ip_version)]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, close_fds=True)
        stdout = proc.communicate()[0].strip()

        # Netid  State      Recv-Q Send-Q Local Address:Port               Peer Address:Port
        # udp    UNCONN     0      0         *:5353                  *:*
        # udp    UNCONN     0      0         *:35109                 *:*
        # udp    UNCONN     0      0      10.0.2.15:68                    *:*
        # tcp    LISTEN     0      128       *:22                    *:*
        # tcp    LISTEN     0      128    127.0.0.1:5432                  *:*

        gauges = [gauge.format(ip_version) for gauge in self.GAUGES]
        metrics = dict.fromkeys(gauges, 0)

        for line in stdout.split("\n"):
            words = line.split()
            if words[0] == 'udp':
                # UDP connections have no state
                metric = "net.udp{0}.connections".format(ip_version)
                metrics[metric] += 1
            elif words[0] == 'tcp' and words[1] in self.TCP_STATES:
                # map state of TCP connection to what we're counting
                metric = "net.tcp{0}.{1}".format(ip_version, self.TCP_STATES[words[1]])
                metrics[metric] += 1
            else:
                pass  # ignore header line

        for key, value in metrics.iteritems():
            log.debug("Reporting {}={}".format(key, value))
            self.gauge(key, value, dimensions=dimensions)

        return len(metrics)
