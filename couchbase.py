#!/usr/bin/env python
# Copyright (C) 2016 SignalFx, Inc.

import json
import pprint
import urllib2

import collectd
import metric_info

# Global constants
DEFAULT_API_TIMEOUT = 60  # Seconds to wait for the Couchbase API to respond
DEFAULT_FIELD_LENGTH = 63  # From the collectd "Naming schema" doc
DEFAULT_METRIC_TYPE = 'gauge'
DIMENSION_NAMES = frozenset(('hostHasService', 'cluster', 'bucket', 'host'))
PLUGIN_NAME = 'couchbase'
DEFAULT_INTERVAL = 10  # Default interval
DEFAULT_COLLECT_MODE = 'default'
DETAILED_COLLECT_MODE = 'detailed'
TARGET_NODE = 'NODE'
TARGET_BUCKET = 'BUCKET'
CLUSTER_DEFAULT = 'default'
REQUEST_TYPE_NODE = "node"
REQUEST_TYPE_NODE_STAT = "node_stat"
REQUEST_TYPE_BUCKET = "bucket"
REQUEST_TYPE_BUCKET_STAT = "bucket_stat"

# These are determined by the plugin config settings and are set by config()
http_timeout = DEFAULT_API_TIMEOUT


class Metric:

    def __init__(self, name, value, dimensions=None):
        self.name = name
        self.value = value
        if dimensions is None:
            self.dimensions = {}
        else:
            self.dimensions = dimensions

    def __str__(self):
        return "Metric { name: %s, value: %s, dimensions: %s}" % (
            self.name, self.value, self.dimensions)


def _api_call(url, opener):
    """
    Makes a REST call against the Couchbase API.
    Args:
    url (str): The URL to get, including endpoint
    Returns:
    list: The JSON response
    """
    try:
        urllib2.install_opener(opener)
        resp = urllib2.urlopen(url, timeout=http_timeout)
    except (urllib2.HTTPError, urllib2.URLError) as e:
        collectd.error("Error making API call (%s) %s" % (e, url))
        return None
    try:
        return json.load(resp)
    except ValueError, e:
        collectd.error("Error parsing JSON for API call (%s) %s" % (e, url))
        return None


def config(config_values, testing="no"):
    """
    Loads information from the Couchbase collectd plugin config file.
    Args:
    :param config_values: Object containing config values
    :param testing: Used by test script to test the plugin
    """

    plugin_config = {}
    interval = DEFAULT_INTERVAL
    collect_mode = DEFAULT_COLLECT_MODE
    collect_bucket = None
    username = None
    password = None
    api_urls = {}
    field_length = DEFAULT_FIELD_LENGTH
    cluster_name = CLUSTER_DEFAULT
    extra_dimensions = ''

    required_keys = ('CollectTarget', 'Host', 'Port')
    opt_keys = ('Interval', 'CollectMode', 'ClusterName', 'Dimensions')
    bucket_specific_keys = ('CollectBucket', 'Username', 'Password')

    for val in config_values.children:
        if val.key in required_keys:
            plugin_config[val.key] = val.values[0]
        # Read optional parameters
        elif val.key in opt_keys and val.key == 'Interval' and val.values[0]:
            interval = val.values[0]
        elif val.key in opt_keys and val.key == 'CollectMode'\
                and val.values[0]:
            collect_mode = val.values[0]
        # Read bucket specific parameters
        elif val.key in bucket_specific_keys and val.key == 'CollectBucket'\
                and val.values[0]:
            collect_bucket = val.values[0]
        elif val.key in bucket_specific_keys and val.key == 'Username' and \
                val.values[0]:
            username = val.values[0]
        elif val.key in bucket_specific_keys and val.key == 'Password' and \
                val.values[0]:
            password = val.values[0]
        elif val.key == 'FieldLength' and val.values[0]:
            field_length = int(val.values[0])
        elif val.key in opt_keys and val.key == 'ClusterName'\
                and val.values[0]:
            cluster_name = val.values[0]
        elif val.key in opt_keys and val.key == 'Dimensions'\
                and val.values[0]:
            extra_dimensions = val.values[0]

    # Make sure all required config settings are present, and log them
    collectd.info("Using config settings:")
    for key in required_keys:
        val = plugin_config.get(key)
        if val is None:
            raise ValueError("Missing required config setting: %s" % key)
        collectd.info("%s=%s" % (key, val))

    # If CollectTarget is bucket, make sure collect_bucket is set
    if plugin_config.get("CollectTarget") == TARGET_NODE:
        pass
    elif plugin_config.get("CollectTarget") == TARGET_BUCKET:
        if collect_bucket is None:
            raise ValueError("Missing required config setting for bucket " +
                             "CollectBucket")
        collectd.info("%s=%s" % ('CollectBucket', collect_bucket))
    else:
        raise ValueError('Invalid CollectTarget parameter')

    # Populate the API URLs now that we have the config
    base_url = ("http://%s:%s" %
                (plugin_config['Host'], plugin_config['Port']))

    auth = urllib2.HTTPPasswordMgrWithDefaultRealm()
    if username is None and password is None:
        username = password = ''
    auth.add_password(None,
                      user=username,
                      passwd=password,
                      uri=base_url)
    handler = urllib2.HTTPBasicAuthHandler(auth)
    opener = urllib2.build_opener(handler)

    # Log registered api urls
    for key in api_urls:
        val = api_urls.get(key)
        collectd.info("%s=%s" % (key, val))

    module_config = {
        'plugin_config': plugin_config,
        'interval': interval,
        'collect_mode': collect_mode,
        'collect_bucket': collect_bucket,
        'username': username,
        'password': password,
        'opener': opener,
        'field_length': field_length,
        'base_url': base_url,
        'cluster_name': cluster_name,
        'extra_dimensions': extra_dimensions,
    }

    # Prepare dimensions list
    module_config['dimensions'] = _build_dimensions(module_config)

    collectd.info("Using dimensions:")
    collectd.info(pprint.pformat(module_config['dimensions']))

    if testing == "yes":
        # for testing purposes
        return module_config

    # register read callbacks
    if plugin_config['CollectTarget'] == TARGET_NODE:
        collectd.register_read(read_node_stats, interval,
                               data=module_config,
                               name='node_{0}:{1}'.format(
                                        plugin_config['Host'],
                                        plugin_config['Port']))
    else:
        collectd.register_read(read_bucket_stats, interval,
                               data=module_config,
                               name='bucket_{0}_{1}:{2}'.format(
                                        collect_bucket,
                                        plugin_config['Host'],
                                        plugin_config['Port']))


def _build_dimensions(module_config):
    collect_target = module_config['plugin_config'].get('CollectTarget')
    cluster_name = module_config['cluster_name']
    dimensions = {'hostHasService': 'couchbase', 'cluster': cluster_name}
    if collect_target == TARGET_BUCKET:
        dimensions['bucket'] = module_config['collect_bucket']

    # Go ahead and parse the extra dimension string and add it to the dict of
    # dimensions
    if module_config['extra_dimensions']:
        try:
            dimensions.update({
                p[0]: p[1]
                for p in [t.split('=')
                          for t in
                          module_config['extra_dimensions'].split(',')]
            })
        except IndexError:
            collectd.error("Dimensions config option is invalid: %s" %
                           module_config['extra_dimensions'])
            raise
    return dimensions


def _parse_with_prefix(metric_name_pref, obj, dimensions, module_config):
    metrics = []
    for key, value in obj.iteritems():
        if isinstance(value, dict):
            new_metric_pref = metric_name_pref
            if key == 'systemStats':
                new_metric_pref += '.system'
            elif key == 'interestingStats':
                pass
            else:
                new_metric_pref += '.' + key
            metrics.extend(
                _parse_with_prefix(new_metric_pref, value, dimensions,
                                   module_config))
        else:
            metric = _process_metric(metric_name_pref, key, value, dimensions,
                                     module_config)
            if metric:
                metrics.append(metric)
    return metrics


def _is_metric_name_allowed(metric_name, module_config):
    if metric_name in metric_info.metric_default:
        return True
    if module_config['collect_mode'] == DETAILED_COLLECT_MODE:
        return metric_name in metric_info.metric_detailed
    return False


def _process_metric(metric_name_pref, metric_name, value, dimensions,
                    module_config):
    metric_name = metric_name_pref + "." + metric_name
    if _is_metric_name_allowed(metric_name, module_config):
        return Metric(metric_name, value, dimensions)
    return None


def _parse_metrics(obj_to_parse, dimensions, request_type, module_config):
    metrics = []
    if request_type == REQUEST_TYPE_NODE:
        if 'storageTotals' in obj_to_parse:
            value = obj_to_parse['storageTotals']
            metric_name_pref = 'storage'
            metrics.extend(
                _parse_with_prefix(metric_name_pref, value, dimensions,
                                   module_config))
    elif request_type == REQUEST_TYPE_NODE_STAT:
        if 'nodes' in obj_to_parse:
            value = obj_to_parse['nodes']
            metric_name_pref = 'nodes'
            for node in value:
                if 'thisNode' in node and node['thisNode'] is True:
                    dimensions = dict(dimensions)
                    dimensions['node'] = node.get('hostname')
                    metrics.extend(_parse_with_prefix(metric_name_pref, node,
                                                      dimensions,
                                                      module_config))
    elif request_type == REQUEST_TYPE_BUCKET:
        if 'quota' in obj_to_parse:
            value = obj_to_parse['quota']
            metric_name_pref = 'bucket.quota'
            metrics.extend(
                _parse_with_prefix(metric_name_pref, value, dimensions,
                                   module_config))
        if 'basicStats' in obj_to_parse:
            value = obj_to_parse['basicStats']
            metric_name_pref = 'bucket.basic'
            metrics.extend(
                _parse_with_prefix(metric_name_pref, value, dimensions,
                                   module_config))
    elif request_type == REQUEST_TYPE_BUCKET_STAT:
        if 'op' in obj_to_parse:
            value = obj_to_parse['op']
            samples = value.get('samples')
            metric_name_pref = 'bucket.op'
            for key_sample, value_sample in samples.iteritems():
                if isinstance(value_sample, list):
                    metric_value = value_sample[-1]
                    metric = _process_metric(metric_name_pref, key_sample,
                                             metric_value, dimensions,
                                             module_config)
                    if metric:
                        metrics.append(metric)

    collectd.debug("End parsing: " + str(len(metrics)))
    for metric in metrics:
        collectd.debug(str(metric))
    return metrics


def _format_dimensions(dimensions, field_length=DEFAULT_FIELD_LENGTH):
    """
    Formats a dictionary of dimensions to a format that enables them to be
    specified as key, value pairs in plugin_instance to signalfx. E.g.
    dimensions = {'a': 'foo', 'b': 'bar'}
    _format_dimensions(dimensions)
    "[a=foo,b=bar]"
    Args:
    dimensions (dict): Mapping of {dimension_name: value, ...}
    Returns:
    str: Comma-separated list of dimensions
    """
    # Collectd limits the plugin_instance field size, so truncate anything
    # longer than that.
    trunc_len = field_length - 2  # account for the 2 brackets at either end
    dim_pairs = []
    # Put the bucket and node dimensions first because it is more likely
    # to be unique and we don't want it to get truncated.
    if 'node' in dimensions:
        dim_pairs.append('node=%s' % dimensions['node'])
    if 'bucket' in dimensions:
        dim_pairs.append('bucket=%s' % dimensions['bucket'])
    dim_pairs.extend("%s=%s" % (k, v) for k, v in dimensions.iteritems() if
                     k != 'node' and k != 'bucket')
    dim_str = ",".join(dim_pairs)[:trunc_len]
    return "[%s]" % dim_str


def _post_metrics(metrics, module_config):
    """
    Posts metrics to collectd.
    Args:
    :param metrics : Array of Metrics objects
    """
    for metric in metrics:
        datapoint = collectd.Values()
        datapoint.type = DEFAULT_METRIC_TYPE
        datapoint.type_instance = metric.name
        datapoint.plugin = PLUGIN_NAME
        datapoint.plugin_instance = _format_dimensions(metric.dimensions,
                                                       module_config[
                                                           'field_length'])
        datapoint.values = (metric.value,)

        # With some versions of CollectD, a dummy metadata map must be added
        # to each value for it to be correctly serialized to JSON by the
        # write_http plugin. See
        # https://github.com/collectd/collectd/issues/716
        datapoint.meta = {'0': True}

        pprint_dict = {
            'plugin': datapoint.plugin,
            'plugin_instance': datapoint.plugin_instance,
            'type': datapoint.type,
            'type_instance': datapoint.type_instance,
            'values': datapoint.values,
            'interval': module_config['interval']
        }
        collectd.debug(pprint.pformat(pprint_dict))
        datapoint.dispatch()


def _first_in_sorted_nodes_list(base_url, opener, resp_obj=None):
    if resp_obj is None:
        api_url = '%s/%s' % (base_url, 'pools/default')
        collectd.debug('GET ' + api_url)
        resp_obj = _api_call(api_url, opener)
        if resp_obj is None:
            collectd.error('Unable to get list of nodes in the cluster')

    nodes = resp_obj['nodes']
    current_node = None
    nodes_list = []
    for node in nodes:
        if 'thisNode' in node and node['thisNode'] is True:
            current_node = node['hostname']
        nodes_list.append(node['hostname'])
    nodes_list.sort()
    if current_node == nodes_list[0]:
        return current_node, True
    return current_node, False


def read_node_stats(module_config):
    """
    Collect cluster-wide node stats and per-node stats
    :param module_config: Configuration from the plugin file
    :return: None
    """
    collectd.debug('Executing read_node_stats callback')

    api_url = '%s/%s' % (module_config['base_url'], 'pools/default')
    collectd.debug('GET ' + api_url)
    resp_obj = _api_call(api_url, module_config['opener'])
    if resp_obj is None:
        collectd.error('Unable to get list of nodes in the cluster')

    # Send cluster-wide node statistics only from one node
    if _first_in_sorted_nodes_list(base_url=module_config['base_url'],
                                   opener=module_config['opener'],
                                   resp_obj=resp_obj):
        _parse_and_post_metrics(resp_obj, REQUEST_TYPE_NODE, module_config)

    # Send per-node metrics for all other nodes
    _parse_and_post_metrics(resp_obj, REQUEST_TYPE_NODE_STAT, module_config)


def read_bucket_stats(module_config):
    """
    Collect cluster-wide and per-node bucket stats
    :param module_config: Configuration from the plugin file
    :return: None
    """
    collectd.debug('Executing read_bucket_stats callback')

    bucket_name = module_config['collect_bucket']

    # Send cluster-wide bucket statistics only from one node
    current_node, is_first_node = _first_in_sorted_nodes_list(
        base_url=module_config['base_url'],
        opener=module_config['opener'])
    if is_first_node:
        api_url = '%s/%s/%s' % (module_config['base_url'],
                                'pools/default/buckets', bucket_name)
        collectd.debug('GET ' + api_url)
        resp_obj = _api_call(api_url, module_config['opener'])
        if resp_obj is None:
            collectd.error('Unable to get bucket statistics')
        _parse_and_post_metrics(resp_obj, REQUEST_TYPE_BUCKET, module_config)

    # Collect per-node bucket stats
    # Get list of nodes containing the bucket
    api_url = '%s/%s/%s/%s' % (module_config['base_url'],
                               'pools/default/buckets', bucket_name, 'nodes')
    collectd.debug('GET ' + api_url)
    resp_obj = _api_call(api_url, module_config['opener'])
    if resp_obj is None:
        collectd.error(
            'Unable to get nodes containing the bucket ' + bucket_name)

    # Send per-node bucket stats
    for server in resp_obj['servers']:
        if server['hostname'] == current_node:
            api_url = '%s/%s' % (module_config['base_url'],
                                 server['stats']['uri'])
            resp_obj = _api_call(api_url, module_config['opener'])
            if resp_obj is None:
                collectd.error('Unable to get per-node bucket stats from ' +
                               api_url)
            module_config['dimensions']['node'] = server['hostname']
            _parse_and_post_metrics(resp_obj, REQUEST_TYPE_BUCKET_STAT,
                                    module_config)


def _parse_and_post_metrics(resp_obj, request_type, module_config):
    dimensions = module_config['dimensions']

    # 1. Parse metrics
    metrics = _parse_metrics(resp_obj, dimensions, request_type,
                             module_config)

    collectd.debug('Interval: ' + str(module_config['interval']))
    # 2. Post metrics
    _post_metrics(metrics, module_config)


def init():
    """
    The initialization callback is essentially a no-op for this plugin.
    """
    collectd.info("Initializing Couchbase plugin")


def shutdown():
    """
    The shutdown callback is essentially a no-op for this plugin.
    """
    collectd.info("Stopping Couchbase plugin")


def setup_collectd():
    """
    Registers callback functions with collectd
    """
    collectd.register_init(init)
    collectd.register_config(config)
    collectd.register_shutdown(shutdown)


setup_collectd()
