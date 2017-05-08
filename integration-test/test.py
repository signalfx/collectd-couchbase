#!/usr/bin/env python

import httplib
import json
from time import time, sleep

# Quick and dirty integration test for multi-cluster support in one collectd
# instance. This test script is intended to be run with docker-compose with the
# provided docker-compose.yml configuration.

# This is not very flexible but could be expanded to support other types of
# integration tests if so desired.

SENTINAL_VALUES = {
    'db1': 'CB1TEST',
    'db2': 'CB2TEST',
}

TIMEOUT_SECS = 60


def get_metric_data():
    # Use httplib instead of requests so we don't have to install stuff with pip
    conn = httplib.HTTPConnection("fake_sfx", 8080)
    conn.request("GET", "/")
    resp = conn.getresponse()
    conn.close()
    return json.loads(resp.read())


def wait_for_metrics_from_each_cluster():
    start = time()
    for host in ['db1', 'db2']:
        print 'Waiting for metrics from couchbase instance %s...' % (host,)

        s = SENTINAL_VALUES[host]
        eventually_true(lambda: any([s in m.get('plugin_instance') for m in get_metric_data()]),
                        TIMEOUT_SECS - (time() - start))
        print 'Found!'


def eventually_true(f, timeout_secs):
    start = time()
    while True:
        try:
            assert f()
        except AssertionError:
            if time() - start > timeout_secs:
                raise
            sleep(0.5)
        else:
            break


if __name__ == "__main__":
    wait_for_metrics_from_each_cluster()
