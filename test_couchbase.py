#!/usr/bin/env python
"""
Unit test for the Couchbase collectd plugin. Meant to be run with pytest.
"""
# Copyright (C) 2016 SignalFx, Inc.

import collections
import mock
import sys
import pytest

import sample_responses


class MockCollectd(mock.MagicMock):
    """
    Mocks the functions and objects provided by the collectd module
    """

    @staticmethod
    def log(log_str):
        print log_str

    debug = log
    info = log
    warning = log
    error = log


def mock_api_call(url, opener):
    """
    Returns example statistics from the sample_responses module.

    Args:
    :param url: (str) The URL whose results to mock
    """
    key = ''
    parsed_url = url.split('/')

    if parsed_url[-2] == 'pools' and parsed_url[-1] == 'default':
        key = 'node'

    if parsed_url[-2] == 'buckets' and parsed_url[-1] == 'default':
        key = 'bucket'

    if parsed_url[-1] == 'stats':
        key = 'bucket_stat'

    return getattr(sample_responses, key)


sys.modules['collectd'] = MockCollectd()
import couchbase

ConfigOption = collections.namedtuple('ConfigOption', ['key', 'values'])

fail_mock_config_required_params = mock.Mock()
fail_mock_config_required_params.children = [
    ConfigOption('Host', ('localhost',)),
    ConfigOption('Port', (8091,)),
    ConfigOption('CollectMode', ('default',)),
    ConfigOption('Interval', (10,)),
]

fail_mock_config_required_params_for_bucket = mock.Mock()
fail_mock_config_required_params_for_bucket.children = [
    ConfigOption('CollectTarget', ('BUCKET',)),
    ConfigOption('Host', ('localhost',)),
    ConfigOption('Port', ('8091',)),
    ConfigOption('CollectMode', ('default',)),
    ConfigOption('Interval', ('10',)),
]

fail_mock_config_unsupported_collect_target = mock.Mock()
fail_mock_config_unsupported_collect_target.children = [
    ConfigOption('CollectTarget', ('OTHER',)),
    ConfigOption('Host', ('localhost',)),
    ConfigOption('Port', ('8091',)),
    ConfigOption('CollectMode', ('default',)),
    ConfigOption('Interval', ('10',)),
]

mock_config_nodes = mock.Mock()
mock_config_nodes.children = [
    ConfigOption('CollectTarget', ('NODE',)),
    ConfigOption('Host', ('localhost',)),
    ConfigOption('Port', ('8091',)),
    ConfigOption('CollectMode', ('detailed',)),
    ConfigOption('Interval', ('10',)),
    ConfigOption('FieldLength', ('1024',)),
]

mock_config_bucket = mock.Mock()
mock_config_bucket.children = [
    ConfigOption('CollectTarget', ('BUCKET',)),
    ConfigOption('Host', ('localhost',)),
    ConfigOption('Port', ('8091',)),
    ConfigOption('Username', ('username',)),
    ConfigOption('Password', ('password',)),
    ConfigOption('CollectBucket', ('default',)),
    ConfigOption('CollectMode', ('detailed',)),
    ConfigOption('Interval', ('10',)),
]

mock_config_field_length = mock.Mock()
mock_config_field_length.children = [
    ConfigOption('CollectTarget', ('BUCKET',)),
    ConfigOption('Host', ('localhost',)),
    ConfigOption('Port', ('8091',)),
    ConfigOption('Username', ('username',)),
    ConfigOption('Password', ('password',)),
    ConfigOption('CollectBucket', ('default',)),
    ConfigOption('CollectMode', ('detailed',)),
    ConfigOption('Interval', ('10',)),
    ConfigOption('FieldLength', ('1024',)),
]


def test_config_node():
    """
    Check read params from config
    """

    couchbase.config(mock_config_nodes)
    assert couchbase.CONFIGS[0]['plugin_config']['CollectTarget'] == 'NODE'
    assert couchbase.CONFIGS[0]['plugin_config']['Host'] == 'localhost'
    assert couchbase.CONFIGS[0]['plugin_config']['Port'] == '8091'
    assert couchbase.CONFIGS[0]['collect_mode'] == 'detailed'
    assert couchbase.CONFIGS[0]['interval'] == '10'
    assert couchbase.CONFIGS[0]['collect_bucket'] is None
    assert couchbase.CONFIGS[0]['username'] == ''
    assert couchbase.CONFIGS[0]['password'] == ''

    assert couchbase.CONFIGS[0]['api_urls']['node'] is not None

    couchbase.CONFIGS = []


def test_config_bucket():
    """
    Check read params from config included bucket configuration
    """

    couchbase.config(mock_config_bucket)
    assert couchbase.CONFIGS[0]['plugin_config']['CollectTarget'] == 'BUCKET'
    assert couchbase.CONFIGS[0]['plugin_config']['Host'] == 'localhost'
    assert couchbase.CONFIGS[0]['plugin_config']['Port'] == '8091'
    assert couchbase.CONFIGS[0]['collect_mode'] == 'detailed'
    assert couchbase.CONFIGS[0]['interval'] == '10'
    assert couchbase.CONFIGS[0]['collect_bucket'] == 'default'
    assert couchbase.CONFIGS[0]['username'] == 'username'
    assert couchbase.CONFIGS[0]['password'] == 'password'

    assert couchbase.CONFIGS[0]['api_urls']['bucket'] is not None
    assert couchbase.CONFIGS[0]['api_urls']['bucket_stat'] is not None
    couchbase.CONFIGS = []


def test_config_field_length():
    """
    Check read params from config included bucket configuration
    """

    couchbase.config(mock_config_field_length)
    assert couchbase.CONFIGS[0]['plugin_config']['CollectTarget'] == 'BUCKET'
    assert couchbase.CONFIGS[0]['plugin_config']['Host'] == 'localhost'
    assert couchbase.CONFIGS[0]['plugin_config']['Port'] == '8091'
    assert couchbase.CONFIGS[0]['collect_mode'] == 'detailed'
    assert couchbase.CONFIGS[0]['interval'] == '10'
    assert couchbase.CONFIGS[0]['collect_bucket'] == 'default'
    assert couchbase.CONFIGS[0]['username'] == 'username'
    assert couchbase.CONFIGS[0]['password'] == 'password'

    assert couchbase.CONFIGS[0]['field_length'] == 1024

    couchbase.CONFIGS = []


def test_config_nodes_fail():
    """
    Check for exception when required params are not specified
    """
    with pytest.raises(ValueError):
        couchbase.config(fail_mock_config_required_params)

    couchbase.CONFIGS = []


def test_config_unsupported_collect_target_fail():
    """
    Check for exception when required params are not specified
    """
    with pytest.raises(ValueError):
        couchbase.config(fail_mock_config_unsupported_collect_target)

    couchbase.CONFIGS = []


def test_config_bucket_fail():
    """
    Check for exception when bucket name param (CollectBucket)
    are not specified
    """
    with pytest.raises(ValueError):
        couchbase.config(fail_mock_config_required_params_for_bucket)

    couchbase.CONFIGS = []


@mock.patch('couchbase._api_call', mock_api_call)
def test_read():
    """
    Tests the read() method of the collectd plugin. This codepath exercises
    most of the code in the plugin.
    """
    couchbase.config(mock_config_nodes)
    couchbase.config(mock_config_bucket)
    couchbase.read()
