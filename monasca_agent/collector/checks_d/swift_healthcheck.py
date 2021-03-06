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

    METRIC_SINGLE = "swift.available"
    METRIC_TOTAL  = "swift.available_sum"
    CHECKS = [
        "health", "info",
        "container_post", "container_stat",
        "object_upload", "object_download",
    ]
    # interpolate the date here to ensure that the swiftclient really uploads this file
    OBJECT_CONTENT = "This file was uploaded by the Monasca agent on {}." \
        .format(str(datetime.datetime.now()))

    def check(self, instance):
        options = instance['connection_options']
        # swiftclient gets confused if the auth_version is an int, not a string
        # (which is bound to happen since YAML parses "3" as an int)
        if 'auth_version' in options:
            options['auth_version'] = str(options['auth_version'])

        self.connection     = swift.SwiftService(options)
        self.swift_url      = self.init_config['swift_url']
        self.container_name = instance.get("container_name", "healthcheck")
        self.object_name    = instance.get("object_name", "healthcheck.txt")

        dimensions = self._set_dimensions(None, instance)
        successful_count = 0

        for check_name in self.CHECKS:
            check = getattr(self, "check_" + check_name)
            try:
                success = check()
            except Exception as e:
                success = False
                log.warn("SwiftHealthcheck {} failed with {}: {}"
                         .format(check_name, type(e), str(e)))

            log.debug("Reporting {}={} for test '{}'"
                      .format(self.METRIC_SINGLE, success, check_name))

            dim = dimensions.copy()
            dim["check"] = check_name
            value = 1 if success else 0
            self.gauge(self.METRIC_SINGLE, value, dimensions=dim)

            if success:
                successful_count += 1

        log.debug("Reporting {}={}".format(self.METRIC_TOTAL, successful_count))
        self.gauge(self.METRIC_TOTAL, successful_count, dimensions=dimensions)

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

