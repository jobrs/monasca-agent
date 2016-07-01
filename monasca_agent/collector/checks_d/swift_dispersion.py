import json
import logging
import subprocess
import types

import monasca_agent.collector.checks as checks

log = logging.getLogger(__name__)


class SwiftDispersion(checks.AgentCheck):

    GAUGES = [
        "object.copies_expected",
        "object.copies_found",
        "object.copies_missing",
        "object.overlapping",
        "container.copies_expected",
        "container.copies_found",
        "container.copies_missing",
        "container.overlapping",
    ]

    def swift_dispersion(self):
        executable = 'swift-dispersion-report'
        if 'swift_dispersion' in self.init_config:
            executable = self.init_config['swift_dispersion']

        cmd = " ".join((executable, '-j'))
        pipe = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                universal_newlines=True)
        out = "".join(pipe.stdout.readlines())

        # swift-dispersion-report on Liberty prints an initial line "Using
        # storage policy: default", so look for the first line that contains
        # the actual JSON
        while "\n" in out and not out.lstrip().startswith('{'):
            # remove first line
            out = out.split("\n", 1).pop()

        return out

    def check(self, instance):
        dimensions = self._set_dimensions(None, instance)

        swift_dispersion = self.swift_dispersion()
        assert(swift_dispersion)

        dispersion = json.loads(swift_dispersion)
        for metric in self.GAUGES:
            log.debug("Checking metric {0}".format(metric))
            disp_metric = metric.split('.', 1)

            if disp_metric[1] == 'copies_missing':
                value = (dispersion[disp_metric[0]]['copies_expected'] -
                         dispersion[disp_metric[0]]['copies_found'])
            else:
                value = dispersion[disp_metric[0]][disp_metric[1]]

            assert(type(value) in (types.IntType, types.LongType,
                                   types.FloatType))

            metric = self.normalize(metric.lower(), 'swift.dispersion')
            log.debug("Sending {0}={1}".format(metric, value))
            self.gauge(metric, value, dimensions=dimensions)
