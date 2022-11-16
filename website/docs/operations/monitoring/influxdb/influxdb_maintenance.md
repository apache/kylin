---
title: InfluxDB Maintenance
language: en
sidebar_label: InfluxDB Maintenance
pagination_label: InfluxDB Maintenance
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
	- influxdb
draft: false
last_update:
    date: 08/12/2022
---

This chapter introduces the basic maintenance of InfluxDB.

### Connectivity

When InfluxDB is not accessible, you can locate the problem from the following aspects:

1. Check if InfluxDB is running normally by executing `service influxdb status`. If it is not running, please check log files of `/var/log/influxdb/influxd.log` or `/var/log/messages` to find out the reason, at the same time, run `service influxdb restart` to restart InfluxDB service and make sure the service can be launched normally by observing the logs. (You should be able to login InfluxDB via `influx -host ? -port ?` command)

2. If you find the port has been taken in the starting process, run `netstat -anp | grep influxdb_port` to get the process id, and execute `ps -ef | grep pid` to get the specific process. You can choose to kill the process if you do not need it or to change InfluxDB's server port to another.

3. If you are having your Kylin and InfluxDB installed in different nodes, please execute `telnet influxdb_ip influxdb_port` on Kylin node to check if two nodes can communicate normally, if not, please make sure the Firewall service is not turned on on InfluxDB node via `service iptables status` command or contact the system admin to check the network condition.

### Log Management

- **Log Configuration**

	- By default, InfluxDB writes standard error to log. InfluxDB redirects stderr to `/var/log/influxdb/influxd.log` file when it is started. If you would like to change the log path, please modify the property in the configuration file `/etc/default/influxdb` to `STDERR=/path/to/influxdb.log`, and restart the service via `service influxdb restart` command.
	- InfluxDB enables HTTP access log by default.  Generally, HTTP access log is quite large, you can modify the property `[http] log-enabled=false` to disable the log output.

- **Log Clean**

	InfluxDB itself does not clean its log regularly, it uses **logrotate** to manage log, which is installed on Linux system by default. The configuration file of **logrotate** is located at `/etc/logrotate.d/influxdb`, the log rotates by day, and the retention is 7 days.

### Backup and Restore

InfluxDB provides the availability to do backup and restore.

- **Backup**

	```sh
	influxd backup -portable -database KYLIN_METRIC -host 127.0.0.1:8089 /path/to/backup
	```

- **Restore**

	Please make sure that the database exists, otherwise the restore will be failed.

	```sh
	influxd restore -portable -database KYLIN_METRIC -host 127.0.0.1:8089 /path/to/backup
	```

> **note:** Please replace KYLIN_METRIC with the actual database name, replace 127.0.0.1:8089 with the actual IP and port, replace `/path/to/backup` with the path you would like to set.

### Monitoring and Diagnosis

- **Memory Monitoring**

	- Check runtime

	  Run following command to check GC, memory usage, etc.
	  `influx -database KYLIN_METRIC -execute "show stats for 'runtime'"`
	  
	  Please focus on these important arguments:
	  - *HeapAlloc* -> Heap allocation size
	  - *Sys* -> The total number of bytes of memory obtained from the system
	  - *NumGC* -> GC times
	  - *PauseTotalNs* -> The total GC pause time

	- Check the memory usage of InfluxDB index

	  `show stats for 'indexes'`
	  
	- Monitor InfluxDB memory usage

	  Run following command:
	  
	  `pidstat -rh -p PID 5`
	  
	  If the memory usage is too high or GC is too  frequent, please increase memory.
	  
	  > **tips:** It is recommended to install InfluxDB on a separate machine with high memory allocation, because data read and write speed are dependent on the indexes, and the indexes are stored in memory.
	
- **Disk Monitoring**

  Run following command to check disk situation:

  ```sh
  pidstat -d -p PID 5
  ```

  When the disk read/write load is found to be too high, you can consider mapping the WAL directory and the data directory to different disks to reduce the interaction between read and write operations.

  1. Run `vi /etc/default/influxdb` to edit the configuration file.
  2. Modify the properties `[data] dir = "/var/lib/influxdb/data"` and `wal-dir = "/var/lib/influxdb/wal"` to point WAL directory and data directory to different disk.

- **Read/Write Response Time**

	1. Write: 
	
	   ```sql
	   SELECT non_negative_derivative(percentile("writeReqDurationNs", 99)) / non_negative_derivative(max("writeReq")) / (1000 * 1000) AS "Write Request" 
	   FROM "_internal".."httpd" 
	   WHERE time > now() - 10d 
	   GROUP BY time(1h) fill(0)
	   ```
	
	2. Read: 
	
     ```sql
	   SELECT non_negative_derivative(percentile("queryReqDurationNs", 99)) / non_negative_derivative(max("queryReq")) / (1000 * 1000) AS "Query Request" 
	   FROM "_internal".."httpd" 
	   WHERE time > now() - 10d 
	   GROUP BY time(1h)
	   ```
