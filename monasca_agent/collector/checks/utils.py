# (C) Copyright 2015 Hewlett Packard Enterprise Development Company LP

import base64
import logging
import re
from numbers import Number

from monasca_agent.collector.checks.check import Check
from monasca_agent.common.exceptions import CheckException

log = logging.getLogger(__name__)


def add_basic_auth(request, username, password):
    """A helper to add basic authentication to a urllib2 request.

    We do this across a variety of checks so it's good to have this in one place.
    """
    auth_str = base64.encodestring('%s:%s' % (username, password)).strip()
    request.add_header('Authorization', 'Basic %s' % auth_str)
    return request

def get_keystone_client(config):
    import keystoneclient.v2_0.client as kc
    kwargs = {
        'username': config.get('admin_user'),
        'project_name': config.get('admin_tenant_name'),
        'password': config.get('admin_password'),
        'auth_url': config.get('identity_uri'),
        'endpoint_type': 'internalURL',
        'region_name': config.get('region_name'),
    }

    return kc.Client(**kwargs)


def get_tenant_name(tenants, tenant_id):
    tenant_name = None
    for tenant in tenants:
        if tenant.id == tenant_id:
            tenant_name = tenant.name
            break
    return tenant_name


def get_tenant_list(config, log):
    tenants = []
    try:
        log.debug("Retrieving Keystone tenant list")
        keystone = get_keystone_client(config)
        tenants = keystone.tenants.list()
    except Exception as e:
        msg = "Unable to get tenant list from keystone: {0}"
        log.error(msg.format(e))

    return tenants


class DynamicCheckHelper:
    """
    Supplements existing check class with reusable functionality to transform third-party metrics into Monasca ones
    in a configurable way
    """

    DEFAULT_GROUP = ""

    class MetricSpec:
        """
        Describes how to filter and map input metrics to Monasca metrics
        """

        GAUGE = 1
        RATE = 2
        SKIP = 0

        def __init__(self, metric_type, metric_name):
            """
            :param metric_type: one of GAUGE, RATE, SKIP
            :param metric_name: normalized name of the metric as reported to Monasca
            """
            self.metric_type = metric_type
            self.metric_name = metric_name

    @staticmethod
    def _normalize_dim_value(value):
        """
        :param value:
        :return:
        Replace \\x?? values with _
        Replace illegal characters
         - according to ANTLR grammar: ( '}' | '{' | '&' | '|' | '>' | '<' | '=' | ',' | ')' | '(' | ' ' | '"' )
         - according to Python API validation: "<>={}(),\"\\\\|;&"
        Truncate to 255 chars
        """

        return re.sub(r'[|\\;,&=\']', '-', re.sub(r'[(}>]', ']', re.sub(r'[({<]', '[', value.replace(r'\x2d', '-').
                                                                        replace(r'\x7e', '~'))))[:255]

    class DimMapping:
        """
        Describes how to transform dictionary like metadata attached to a metric into Monasca dimensions
        """

        def __init__(self, dimension, regex='(.*)', separator=None):
            """
            :param dimension to be mapped to
            :param regex: regular expression used to extract value from source value
            """
            self.dimension = dimension
            self.regex = regex
            self.separator = separator
            self.cregex = re.compile(regex) if regex != '(.*)' else None

        def map_value(self, source_value):
            """
            transform source value into target dimension value
            :param source_value: label value to transform
            :return: transformed dimension value or None if the regular expression did not match
            """
            if self.cregex:
                match_groups = self.cregex.match(source_value)
                if match_groups:
                    return DynamicCheckHelper._normalize_dim_value(self.separator.join(match_groups.groups()))
                else:
                    return None
            else:
                return DynamicCheckHelper._normalize_dim_value(source_value)

    @staticmethod
    def _build_dimension_map(config):
        """
        Builds dimension mappings for the given configuration element
        :param config: 'mappings' element of config
        :return: dictionary mapping source labels to applicable DimMapping objects
        """
        result = {}
        for dim, spec in config.get('dimensions', {}).iteritems():
            if isinstance(spec, dict):
                label = spec.get('source_key', dim)
                sepa = spec.get('separator', '-')
                regex = spec.get('regex', '(.*)')
            else:
                label = spec
                regex = '(.*)'
                sepa = None

            # note: source keys can be mapped to multiple dimensions
            arr = result.get(label, [])
            mapping = DynamicCheckHelper.DimMapping(dimension=dim, regex=regex, separator=sepa)
            arr.append(mapping)
            result[label] = arr

        return result

    def __init__(self, check, prefix=None, default_mapping=None):
        """
        :param check: Target check instance to filter and map metrics from a separate data source. The mapping
        procedure involves a filtering, renaming and classification of metrics and as part of this also filtering
        and mapping of the labels attached to the input metrics.

        To support all these capabilities, an element 'mapping' needs to be added to the instance config or a
        default_mapping has to be supplied.

        Filtering and renaming of input metrics is performed through regular expressions. Metrics not matching
        the regular expression are ignored.
         with zero or more match groups.
        If match groups are specified, the match group values are
        concatenated with '_'. If no match group is specified, the name is taken as is. The resulting name is
        normalized according to Monasca naming standards for metrics. This implies that dots are replaced by
        underscores and *CamelCase* is transformed into *lower_case*. Special characters are eliminated, too.

        a) Simple mapping:

           rates: [ 'FilesystemUsage' ]             # map metric 'FileystemUsage' to 'filesystem_usage'

        b) Mapping with simple regular expression

           rates: [ '.*Usage' ]                     # map metrics ending with 'Usage' to '..._usage'

        b) Mapping with regular expression and match-groups

           rates: [ '(.*Usage)\.stats\.(total)' ]   # map metrics ending with 'Usage.stats.total' to '..._usage_total'

        Mapping of labels to dimensions is a little more complex. For each dimension, an
        entry of the following format is required:

        a) Simple mapping

            <dimension>: <source_key>                # map key <source_key> to dimension <dimension>

        b) Complex mapping:

            <dimension>:
               source_key: <source_key>             # key as provided by metric source (default: <dimension>)
               regex: <mapping_pattern>             # regular expression (default: '(.*)' = identity)
               separator: <match_group_separator>   # concatenate match-groups \1, \2, ... in regex with separator
               (default: '-')


            The regex is applied to the dimension value. If the regular expression does not match, then the metric
            is ignored. If match-groups are part of the regular expression then the regex is used for value
            transformation: The resulting dimension value is created by concatenating all match groups (in braces),
            using the specified separator (default: '-'). If not match-group is specified, then the value is passed
            through unchanged.

        Both metrics and dimension can be defined globally or as part of a group. When a metric is specified in a group,
        then the group name is used as a prefix to the metric and the group-specific dimension mappings take precedence
        over the global ones. When several groups or the global mapping refer to the same input metric, then the caller
        needs to specify which groups to select for mapping.

        Example:

        instances:
            - name: kubernetes
              mapping
                dimensions:
                    pod_name: io.kubernetes.pod.name    # simple mapping
                    pod_basename:
                        source_key: label_name
                        regex: 'k8s_.*_.*\._(.*)_[0-9a-z\-]*'
                rates:
                - io.*
                gauges:
                - .*_avg
                - .*_max
                groups:
                    engine:
                        dimensions:

        """
        self._check = check
        self._prefix = prefix
        self._groups = {}
        self._metric_map = {}
        self._dimension_map = {}
        self._metric_cache = {}
        self._grp_metric_map = {}
        self._grp_dimension_map = {}
        self._grp_metric_cache = {}
        self._metric_to_group = {}
        for inst in self._check.instances:
            iname = inst['name']
            mappings = inst.get('mapping', default_mapping)
            if mappings:
                # build global name filter and rate/gauge assignment
                self._metric_map[iname] = mappings
                self._metric_cache[iname] = {}
                # build global dimension map
                self._dimension_map[iname] = DynamicCheckHelper._build_dimension_map(mappings)
                # check if groups are used
                groups = mappings.get('groups')
                self._metric_to_group[iname] = {}
                self._groups[iname] = []
                if groups:
                    self._groups[iname] = groups.keys()
                    self._grp_metric_map[iname] = {}
                    self._grp_metric_cache[iname] = {}
                    self._grp_dimension_map[iname] = {}
                    for grp, gspec in groups.iteritems():
                        self._grp_metric_map[iname][grp] = gspec
                        self._grp_metric_cache[iname][grp] = {}
                        self._grp_dimension_map[iname][grp] = DynamicCheckHelper._build_dimension_map(gspec)
                    # add the global mappings as pseudo group, so that it is considered when searching for metrics
                    self._groups[iname].append(DynamicCheckHelper.DEFAULT_GROUP)
                    self._grp_metric_map[iname][DynamicCheckHelper.DEFAULT_GROUP] = self._metric_map[iname]
                    self._grp_metric_cache[iname][DynamicCheckHelper.DEFAULT_GROUP] = self._metric_cache[iname]
                    self._grp_dimension_map[iname][DynamicCheckHelper.DEFAULT_GROUP] = self._dimension_map[iname]

            else:
                raise CheckException('instance %s is not supported: no element "mapping" found!', iname)

    def _get_group(self, instance, metric):
        """
        Search the group for a metric. Can be used only when metric names unambiguous across groups.

        :param metric: input metric
        :return: group name or None (if no group matches)
        """
        iname = instance['name']
        group = self._metric_to_group[iname].get(metric)
        if group is None:
            for g in self._groups[iname]:
                spec = self._fetch_metric_spec(instance, metric, g)
                if spec and spec.metric_type != DynamicCheckHelper.MetricSpec.SKIP:
                    self._metric_to_group[iname][metric] = g
                    return g

        return group

    def _fetch_metric_spec(self, instance, metric, group=None):
        """
        check whether a metric is enabled by the instance configuration

        :param instance: instance containing the check configuration
        :param metric: metric as reported from metric data source (before mapping)
        :param group: optional metric group, will be used as dot-separated prefix
        """

        instance_name = instance['name']

        # filter and classify the metric

        if group is not None:
            metric_cache = self._grp_metric_cache[instance_name].get(group, {})
            metric_map = self._grp_metric_map[instance_name].get(group, {})
            return DynamicCheckHelper._lookup_metric(metric, metric_cache, metric_map)
        else:
            metric_cache = self._metric_cache[instance_name]
            metric_map = self._metric_map[instance_name]
            return DynamicCheckHelper._lookup_metric(metric, metric_cache, metric_map)

    def is_enabled_metric(self, instance, metric, group=None):
        return self._fetch_metric_spec(instance, metric, group).metric_type != DynamicCheckHelper.MetricSpec.SKIP

    def push_metric_dict(self, instance, metric_dict, labels={}, group=None, timestamp=None, fixed_dimensions={},
                         default_dimensions={}, max_depth=0, curr_depth=0, prefix='', index=-1):
        """
        This will extract metrics and dimensions from a dictionary.

        The following mappings are applied:

        Simple recursive composition of metric names:

            Input:

                {
                    'server': {
                        'requests': 12
                    }
                }

            Configuration:

                mapping:
                    rates:
                        - server_requests

            Output:

                server_requests=12

        Mapping of textual values to dimensions to distinguish array elements. Make sure that tests attributes
        are sufficient to distinguish the array elements. If not use the build-in 'index' dimension.

            Input:

            {
                'server': [
                    {
                        'role': 'master,
                        'node_name': 'server0',
                        'requests': 1500
                    },
                    {
                        'role': 'slave',
                        'node_name': 'server1',
                        'requests': 1000
                    },
                    {
                        'role': 'slave',
                        'node_name': 'server2',
                        'requests': 500
                    }
                }
            }

            Configuration:

                mapping:
                    dimensions:
                        server_role: role
                        node_name: node_name
                    rates:
                        - requests

            Output:

                server_requests{server_role=master, node_name=server0} = 1500.0
                server_requests{server_role=slave, node_name=server1} = 1000.0
                server_requests{server_role=slave, node_name=server2} = 500.0


        Distinguish array elements where no textual attribute are available or no mapping has been configured for them.
        In that case an 'index' dimension will be attached to the metric which has to be mapped properly.

            Input:

                {
                    'server': [
                        {
                            'requests': 1500
                        },
                        {
                            'requests': 1000
                        },
                        {
                            'requests': 500
                        }
                    }
                }

            Configuration:

                mapping:
                    dimensions:
                        server_no: index          # index is a predefined label
                    rates:
                        - server_requests

            Result:

                server_requests{server_no=0} = 1500.0
                server_requests{server_no=1} = 1000.0
                server_requests{server_no=2} = 500.0


        :param instance:
        :param metric_dict:
        :param labels: labels to be mapped to dimensions
        :param group: group to use for mapping labels and prefixing
        :param timestamp: timestamp to report for the measurement
        :param fixed_dimensions: dimensions which are always added with fixed values
        :param default_dimensions: dimensions to be added, can be overwritten by actual data in metric_dict
        :param max_depth: max. depth to recurse
        :param curr_depth: depth of recursion
        :param prefix: prefix to prepend to any metric
        :param index: current index when traversing through a list
        :return:
        """

        # when traversing through an array, each element must be distinguished with dimensions
        # therefore additional dimensions need to be calculated from the siblings of the actual number valued fields
        if index != -1:
            ext_labels = self.extract_dist_labels(instance['name'], group, metric_dict, labels, index)
            if not ext_labels:
                log.debug(
                    "skipping array due to lack of mapped dimensions for group %s "
                    "(at least 'index' should be supported)",
                    group if group else '<root>')
                return

        else:
            ext_labels = labels

        for element, child in metric_dict.iteritems():
            # if child is a dictionary, then recurse
            if isinstance(child, dict) and curr_depth < max_depth:
                self.push_metric_dict(instance, child, ext_labels, group, timestamp, fixed_dimensions,
                                      default_dimensions, max_depth, curr_depth + 1, prefix + element + '_')
            # if child is a number, assume that it is a metric (it will be filtered out by the rate/gauge names)
            elif isinstance(child, Number):
                self.push_metric(instance, prefix + element, float(child), ext_labels, group, timestamp,
                                 fixed_dimensions,
                                 default_dimensions)
            # if it is a list, then each array needs to be added. Additional dimensions must be found in order to
            # distinguish the measurements.
            elif isinstance(child, list):
                for i, child_element in enumerate(child):
                    if isinstance(child_element, dict):
                        if curr_depth < max_depth:
                            self.push_metric_dict(instance, child_element, ext_labels, group, timestamp,
                                                  fixed_dimensions, default_dimensions, max_depth, curr_depth + 1,
                                                  prefix + element + '_', index=i)
                    elif isinstance(child_element, Number):
                        if len(self._get_mappings(instance['name'], group, 'index')) > 0:
                            idx_labels = ext_labels.copy()
                            idx_labels['index'] = str(i)
                            self.push_metric(instance, prefix + element, float(child_element), idx_labels, group,
                                             timestamp, fixed_dimensions, default_dimensions)
                        else:
                            log.debug("skipping array due to lack of mapped 'index' dimensions for group %s",
                                      group if group else '<root>')
                    else:
                        log.debug('nested arrays are not supported for configurable extraction of element %s', element)

    def extract_dist_labels(self, instance_name, group, metric_dict, labels, index):
        """
        Extract additional distinguishing labels from metric dictionary. All top-level attributes which are
        strings and for which a dimension mapping is available will be transformed into dimensions.
        :param instance_name: instance to be used
        :param group: metric group or None for root/unspecified group
        :param metric_dict: input dictionary containing the metric at the top-level
        :param labels: labels dictionary to extend with the additional found metrics
        :param index: index value to be used as fallback if no labels can be derived from string-valued attributes
            or the derived labels are not mapped in the config.
        :return: Extended labels, already including the 'labels' passed into this method
        """
        ext_labels = None
        # collect additional dimensions first from non-metrics
        for element, child in metric_dict.iteritems():
            if isinstance(child, str) and len(self._get_mappings(instance_name, group, element)) > 0:
                if not ext_labels:
                    ext_labels = labels.copy()
                ext_labels[element] = child
        # if no additional labels supplied just take the index (if it is mapped)
        if not ext_labels and len(self._get_mappings(instance_name, group, 'index')) > 0:
            if not ext_labels:
                ext_labels = labels.copy()
            ext_labels['index'] = str(index)

        return ext_labels

    def push_metric(self, instance, metric, value, labels={}, group=None, timestamp=None, fixed_dimensions={},
                    default_dimensions={}):
        """
        push a meter using the configured mapping information to determine metric_type and map the name and dimensions

        :param instance: instance containing the check configuration
        :param value: metric value (float)
        :param metric: metric as reported from metric data source (before mapping)
        :param labels: labels/tags as reported from the metric data source (before mapping)
        :param timestamp: optional timestamp to handle rates properly
        :param group: specify the metric group, otherwise it will be determined from the metric name
        :param fixed_dimensions:
        :param default_dimensions:
        """

        # determine group automatically if not specified
        if group is None:
            group = self._get_group(instance, metric)

        metric_entry = self._fetch_metric_spec(instance, metric, group)
        if metric_entry.metric_type == DynamicCheckHelper.MetricSpec.SKIP:
            return False

        if self._prefix:
            metric_prefix = self._prefix + '.'
        else:
            metric_prefix = ''

        if group:
            metric_prefix += group + '.'

        # determine the metric name
        metric_name = metric_prefix + metric_entry.metric_name
        # determine the target dimensions
        dims = self._map_dimensions(instance['name'], labels, group, default_dimensions)
        if dims is None:
            # regex for at least one dimension filtered the metric out
            return True

        # apply fixed default dimensions
        if fixed_dimensions:
            dims.update(fixed_dimensions)

        log.debug('push %s %s = %s {%s}', metric_entry.metric_type, metric_entry.metric_name, value, dims)

        if metric_entry.metric_type == DynamicCheckHelper.MetricSpec.RATE:
            self._check.rate(metric_name, float(value), dimensions=dims)
        elif metric_entry.metric_type == DynamicCheckHelper.MetricSpec.GAUGE:
            self._check.gauge(metric_name, float(value), timestamp=timestamp, dimensions=dims)

        return True

    def get_mapped_metrics(self, instance):
        """
        Return input metric names or regex for which a mapping has been defined
        :param instance: instance to consider
        :return: array of metrics
        """
        metric_list = []
        iname = instance['name']
        # collect level-0 metrics
        metric_map = self._metric_map[iname]
        metric_list.extend(metric_map.get('gauges', []))
        metric_list.extend(metric_map.get('rates', []))
        # collect group specific metrics
        grp_metric_map = self._grp_metric_map.get(iname, {})
        for gname, gmmap in grp_metric_map.iteritems():
            metric_list.extend(gmmap.get('gauges', []))
            metric_list.extend(gmmap.get('rates', []))

        return metric_list

    def _map_dimensions(self, instance_name, labels, group, default_dimensions):
        """
        Transform labels attached to input metrics into Monasca dimensions
        :param default_dimensions:
        :param group:
        :param instance_name:
        :param labels:
        :return: mapped dimensions or None if the dimensions filter did not match and the metric needs to be filtered
        """
        dims = default_dimensions.copy()
        #  map all specified dimension all keys
        for labelname, labelvalue in labels.iteritems():
            mapping_arr = self._get_mappings(instance_name, group, labelname)

            target_dim = None
            for map_spec in mapping_arr:
                try:
                    # map the dimension name
                    target_dim = map_spec.dimension
                    # apply the mapping function to the value
                    if target_dim not in dims:  # do not overwrite
                        mapped_value = map_spec.map_value(labelvalue)
                        if mapped_value is None:
                            # None means: filter it out based on dimension value
                            return None
                        dims[target_dim] = mapped_value
                except (IndexError, AttributeError):  # probably the regex was faulty
                    log.exception(
                        'dimension %s value could not be mapped from %s: regex for mapped dimension %s '
                        'does not match %s',
                        target_dim, labelvalue, labelname, map_spec.regex)
                    return None

        return dims

    def _get_mappings(self, instance_name, group, labelname):
        # obtain mappings
        # check group-specific ones first
        if group:
            mapping_arr = self._grp_dimension_map[instance_name].get(group, {}).get(labelname, [])
        else:
            mapping_arr = []
        # fall-back to global ones
        mapping_arr.extend(self._dimension_map[instance_name].get(labelname, []))
        return mapping_arr

    @staticmethod
    def _lookup_metric(metric, metric_cache, metric_map):
        """
        Search cache for a MetricSpec and create if missing
        :param metric: input metric name
        :param metric_cache: cache to use
        :param metric_map: mapping config element to consider
        :return: MetricSpec for the output metric
        """
        metric_entry = metric_cache.get(metric)
        if metric_entry is None:
            re_list = metric_map.get('gauges', [])
            for rx in re_list:
                match_groups = re.match(rx, metric)
                if match_groups:
                    metric_entry = DynamicCheckHelper.MetricSpec(metric_type=DynamicCheckHelper.MetricSpec.GAUGE,
                                                                 metric_name=DynamicCheckHelper._normalize_metricname(
                                                                     metric,
                                                                     match_groups))
                    metric_cache[metric] = metric_entry
                    return metric_entry
            re_list = metric_map.get('rates', [])
            for rx in re_list:
                match_groups = re.match(rx, metric)
                if match_groups:
                    metric_entry = DynamicCheckHelper.MetricSpec(metric_type=DynamicCheckHelper.MetricSpec.RATE,
                                                                 metric_name=DynamicCheckHelper._normalize_metricname(
                                                                     metric,
                                                                     match_groups))
                    metric_cache[metric] = metric_entry
                    return metric_entry
            # fall-through
            metric_entry = DynamicCheckHelper.MetricSpec(metric_type=DynamicCheckHelper.MetricSpec.SKIP,
                                                         metric_name=DynamicCheckHelper._normalize_metricname(metric))
            metric_cache[metric] = metric_entry

        return metric_entry

    @staticmethod
    def _normalize_metricname(metric, match_groups=None):
        # map metric name first
        if match_groups and match_groups.lastindex > 0:
            metric = '_'.join(match_groups.groups())

        metric = re.sub('(?!^)([A-Z]+)', r'_\1', metric.replace('.', '_')).replace('__', '_').lower()
        metric = re.sub(r"[,\+\*\-/()\[\]{}]", "_", metric)
        # Eliminate multiple _
        metric = re.sub(r"__+", "_", metric)
        # Don't start/end with _
        metric = re.sub(r"^_", "", metric)
        metric = re.sub(r"_$", "", metric)
        # Drop ._ and _.
        metric = re.sub(r"\._", ".", metric)
        metric = re.sub(r"_\.", ".", metric)

        return metric
