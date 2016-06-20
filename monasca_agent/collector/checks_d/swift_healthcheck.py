# Copyright 2016 SAP SE.

import datetime
import logging
import sys
import StringIO
import urllib2

import monasca_agent.collector.checks as checks
import swiftclient.service as swift

log = logging.getLogger(__name__)


class SwiftHealthcheck(checks.AgentCheck):
    """ Check the basic availability of a Swift cluster.
        This check should run outside of the cluster.
    """

    METRIC = "swift.available"
    CHECKS = [
        "health", "info",
        "container_post", "container_stat",
        "object_upload", "object_download",
    ]
    # interpolate the date here to ensure that the swiftclient really uploads this file
    OBJECT_CONTENT = "This file was uploaded by the Monasca agent on {}." \
        .format(str(datetime.datetime.now()))

    def check(self, instance):
        cfg = self.init_config
        self.connection     = swift.SwiftService(cfg['connection_options'])
        self.swift_url      = cfg['swift_url']
        self.container_name = cfg.get("container_name", "healthcheck")
        self.object_name    = cfg.get("object_name", "healthcheck.txt")

        dimensions = self._set_dimensions(None, instance)
        for check_name in self.CHECKS:
            check = getattr(self, "check_" + check_name)
            try:
                success = check()
            except Exception as e:
                success = False
                log.warn("SwiftHealthcheck {} failed with {}: {}"
                         .format(check_name, type(e), str(e)))

            log.debug("Reporting {}={} for test '{}'"
                      .format(self.METRIC, success, check_name))

            dimensions["check"] = check_name
            value = 1 if success else 0
            self.gauge(self.METRIC, value, dimensions=dimensions)

    ############################################################################
    # individual checks -- Each of these is submitted as a metric (0 or 1).
    #                      They run in the order defined by the CHECKS array.

    def check_health(self):
        content = urllib2.urlopen(self.swift_url + "/healthcheck").read()
        return content == "OK"

    def check_info(self):
        content = urllib2.urlopen(self.swift_url + "/info").read().strip()
        # expecting a JSON object
        return content.startswith("{") and content.endswith("}")

    def check_container_post(self):
        result = self.connection.post(self.container_name)
        return self.handle_swift_result(result)

    def check_container_stat(self):
        result = self.connection.stat(self.container_name)
        return self.handle_swift_result(result)

    def check_object_upload(self):
        content = StringIO.StringIO(self.OBJECT_CONTENT)
        result = list(self.connection.upload(
            self.container_name,
            [swift.SwiftUploadObject(content, object_name=self.object_name)],
        ))[0]
        return self.handle_swift_result(result)

    def check_object_download(self):
        result = list(self.connection.download(
            self.container_name, [self.object_name],
            { "no_download": True },
        ))[0]
        # `result` is a generator; fetch the first element
        return self.handle_swift_result(result) \
            and result["read_length"] == len(self.OBJECT_CONTENT)

    ############################################################################
    # helper functions for checks

    def handle_swift_result(self, result):
        if "traceback" in result:
            log.error(result["traceback"])
        return result["success"]

