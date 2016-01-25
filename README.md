# Collectd Couchbase Plugin

## Introduction

collectd-couchbase is a [collectd](http://www.collectd.org/) plugin that
collects statistics from Couchbase.


## Requirements

* Collectd 4.9 or later (for the Python plugin)
* Python 2.7 or later
* Couchbase 4.1 or later

## Configuration

The following mandatory configuration options describe how the plugin will
connect to the Couchbase Server:

* CollectTarget - Required option. Define the plugin running instance, has two options: 'NODE' - 
get nodes related metrics or 'BUCKET' - get bucket related metrics. Target 'BUCKET' requires to have username and
password to connect to selected bucket
* Host - Required option. Hostname or IP address of the Couchbase server, default is 'localhost'
* Port - Required option. The port of the Couchbase server, default is '8091'

The following optional settings may be specified to control the behavior of the plugin:

* Username - the username for authentication to selected bucket, default is None. 
If your bucket has not set up username and password just ignore this parameter otherwise define them
* Password - the password for authentication to selected bucket, default is None
If your bucket has not set up username and password just ignore this parameter otherwise define them
* Interval - interval between sync metrics calls, default is 10 seconds
* CollectMode - define the mode of plugin running, has two options: 'default' - 
get basics metrics or 'detailed' - get all available metrics. See details in `metric_info.py`
* CollectBucket - bucket name for retrieving metrics. 


The following is an example Collectd configuration for this plugin:

```
    LoadPlugin python
    <Plugin python>
      ModulePath "/opt/collectd-couchbase"

      Import couchbase
      <Module couchbase>
        CollectTarget "NODE"
        Host "localhost"
        Port "8091"
        CollectMode "default"
        Interval 10
      </Module>
      <Module couchbase>
        CollectTarget "BUCKET"
        Host "localhost"
        Port "8091"
        Username "USERNAME"
        Password "PASSWORD"
        Interval 10
        CollectMode "default"
        CollectBucket "default"
      </Module>
    </Plugin>
```
