---
title: Service Monitoring
language: en
sidebar_label: Service Monitoring
pagination_label: Service Monitoring
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
  - service monitoring
draft: false
last_update:
  date: 08/12/2022
---

Kylin provides the service monitoring for main components to help administrators obtain the service status and maintain instances.

Currently, we provide the following methods to monitor the core components in Kylin:

1. Query: each Query node will records its service status in InfluxDB
2. Build: each All node will records the service status and job status in InfluxDB

Two Rest APIs are provided to monitor and obtain the service status so that customers can integrate it with their own monitor platform.

- Get the Kylin cluster status by monitor query and building services. If the status is `WARNING` or `CRASH`, it means the cluster is unstable.
- Get the service unavailable time with the specified time range and some detailed monitor data to help admins to track and retrospect.

### How to Use

**Get Cluster Status**

`GET http://host:port/kylin/api/monitor/status`

- HTTP Header

  - Accept: application/vnd.apache.kylin-v4-public+json
  - Accept-Language: en
  - Content-Type: application/json;charset=utf-8

- Curl Request Example

  ```
  curl -X GET \
  'http://host:port/kylin/api/monitor/status' \
  -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
  -H 'Accept-Language: en' \
  -H 'Authorization: Basic QURNSU46S1lMSU4=' \
  -H 'Content-Type: application/json;charset=utf-8' 
  ```

- Response Details

  - `active_instances` number of active instances in current cluster.
  - `query_status` query service status. It could be GOOD / WARNING / CRASH
  - `job_status` building service status. It could be GOOD / WARNING / CRASH.
  - `Job` job instance status. It will show the instance details and status.
  - `query` query instance status. It will show the instance details and status.

- Response Example

  ```json
  {
      "code": "000",
      "data": {
          "active_instances": 1,
          "query_status": "GOOD",
          "job_status": "GOOD",
          "job": [
              {
                  "instance": "sandbox.hortonworks.com:7070",
                  "status": "GOOD"
              }
          ],
          "query": [
              {
                  "instance": "sandbox.hortonworks.com:7070",
                  "status": "GOOD"
              }
          ]
      },
      "msg": ""
  }
  ```

  

**Get Cluster Status with Specific Time Range**

`GET http://host:port/kylin/api/monitor/status/statistics`

- HTTP Header

  - Accept: application/vnd.apache.kylin-v4-public+json
  - Accept-Language: en
  - Content-Type: application/json;charset=utf-8

- URL Parameters

  - `start` - `required` `Long` timestamp. Get the monitor data greater than or equal to the timestamp.
  - `end` - `reuquired` `Long` timestamp. Get the monitor data smaller than the timestamp.

- Curl Example

  ```
  curl -X GET \
    'http://host:port/kylin/api/monitor/status/statistics?start=1583562358466&end=1583562358466' \
    -H 'Accept: application/vnd.apache.kylin-v4-public+json' \
    -H 'Accept-Language: en' \
    -H 'Authorization: Basic QURNSU46S1lMSU4=' \
    -H 'Content-Type: application/json;charset=utf-8'
  ```

- Response Details

  - `Start` start time of monitoring. It will be rounded down based on the interval of monitoring data. If the interval is 1 minute, it will only record data in minute level. For example, if the argument is `1587353550000`, it will be recognized as `1587353520000`. Therefore, the data might be inaccurate.
  - `end` end time of monitoring. It will be rounded down based on the interval of monitoring data. If the interval is 1 minute, it will only record data in minute level. For example, if the argument is `1587353550000`, it will be recognized as `1587353520000`. Therefore, the data might be inaccurate.
  - `interval` interval of monitor data, default value is 60000 ms (1 min)
  - `job` job instance status. It will show the instance details and status, which includes unavailable time and counts. The time unit of unavailable time is ms.
  - `query` query instance status. It will show the instance details and status, which includes unavailable time and counts. The time unit of unavailable time is ms.

- Response Example

  ```
  {
      "code":"000",
      "data":{
          "start":1584151560000,
          "end":1584151680000,
          "interval":60000,
          "job":[
              {
                  "instance":"sandbox.hortonworks.com:7070",
                  "details":[
                      {
                          "time":1584151572650,
                          "status":"GOOD"
                      },
                      {
                          "time":1584151632770,
                          "status":"GOOD"
                      }
                  ],
                  "unavailable_time":0,
                  "unavailable_count":0
              }
          ],
          "query":[
              {
                  "instance":"sandbox.hortonworks.com:7070",
                  "details":[
                      {
                          "time":1584151609142,
                          "status":"GOOD"
                      },
                      {
                          "time":1584151669142,
                          "status":"GOOD"
                      }
                  ],
                  "unavailable_time":0,
                  "unavailable_count":0
              }
          ]
      },
      "msg":""
  }
  ```




### Know Limitation

1. The detected query is constant query which will not scan HDFS files.
2. InfluxDB is not high available now. Hence, some monitor data will be lost if the InfluxDB service is down. 
3. The job status will be inaccurate if deleting or discarding plenty of jobs.
4. Since system monitoring depends on InfluxDB, if the system monitoring is still enabled (enabled by default) when InfluxDB is not configured, some useless errors may appear in the log. So when InfluxDB is not configured, it is recommended to configure `kylin.monitor.enabled = false` in `kylin.properties` to turn off the system monitoring function.

