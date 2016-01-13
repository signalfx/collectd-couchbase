#!/bin/bash
set -ex

flake8 couchbase.py test_couchbase.py metric_info.py
py.test test_couchbase.py