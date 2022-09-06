---
title: Use InfluxDB as Time-Series Database
language: en
sidebar_label: Use InfluxDB as Time-Series Database
pagination_label: Use InfluxDB as Time-Series Database
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


### <span id="preparation">Preparation</span>

Starting with Kylin 5.0, the system uses RDBMS to store query history, which only use InfluxDB to record the monitoring information of the system. 

If you need this information, you need to configure the time series database InfluxDB in advance to store data such as the monitoring information of the system.

We recommend you to use InfluxDB v1.6.4, which can download by the script `$KYLIN_HOME/sbin/download-influxdb.sh`. 

The InfluxDB installation package, `influxdb-1.6.4.x86_64.rpm` will be under the `influxdb` directory in the installation directory of Kylin.

If you need to use the existed InfluxDB database in your environment, please use the versions below:

- InfluxDB 1.6.4 or above

You can use the following command to check the version of InfluxDB in your current environment.

```shell
service influxdb version
```

### <span id="root">Installation and Configuration for `root` User</span>

The following steps are using InfluxDB 1.6.4 as an example.

1. Run command to check if InfluxDB is installed already.

      ```shell
      service influxdb status
      ```

   If not, you can download the influxdb and go to the directory where the InfluxDB installation package is located and install InfluxDB.

   ```shell
   # download influxDB
   $KYLIN_HOME/sbin/download-influxdb.sh
  
   # go to influxDB directory and install it
   cd $KYLIN_HOME/influxdb
   rpm -ivh influxdb-1.6.4.x86_64.rpm
   ```

2. Launch InfluxDB. 

   ```sh
   service influxdb start
   ```

   By default, you can find InfluxDB's log at `/var/log/influxdb`.

3. If your InfluxDB server port is in use, you can modify the InfluxDB configuration file to change the server port.

   ```sh
   vi /etc/influxdb/influxdb.conf
   ```

   Please note the following three points:

   - Modify RPC port: The initial property is `bind-address = "127.0.0.1:8088"`, you can change `8088` to an available port, for instance, `18087`.
   - Modify HTTP port: The initial property is `bind-address = ":8086"`, you can change `8086` to available port, for instance, `18086`.
   - Set `reporting-disabled = true`, which means that the InfluxDB will not send reports to [influxdata.com](https://www.influxdata.com/) regularly.

4. InfluxDB is accessible without a user name and password by default. If you want to strengthen the security level, you can set a password with the following steps:

  1. log in InfluxDB.

  	```sh
  	influx -port 18086 
  	```
  	
  	**Tips:** Please replace `18086` with an actually available port number.
  	
  2. Manage admin user and password.

     ```mariadb
     CREATE USER admin WITH PASSWORD 'admin' WITH ALL PRIVILEGES
     ```

  3. Open the configuration file and modify ` [http] auth-enabled = true` to enable authorization.

     ```sh
     vi /etc/influxdb/influxdb.conf 
     ```

  4. Restart InfluxDB to take effect and login InfluxDB.

     ```sh
     service influxdb restart
     influx -port 18086 -username admin -password admin 
     ```

5. Open the property file `kylin.properties` and modify the InfluxDB configurations. Please replace `ip:http_port`, `user`, `pwd`, `ip:rpc_port` with real values.

  ```properties
   vi $KYLIN_HOME/conf/kylin.properties 
   
   ### Modify the following properties
   
   kylin.influxdb.address=ip:http_port
   kylin.influxdb.username=user
   kylin.influxdb.password=pwd
   kylin.metrics.influx-rpc-service-bind-address=ip:rpc_port
  ```


  **Note**: If more than one Kylin instances are deployed, you should configure the above configurations in `kylin.properties` for each Kylin node and let them point to the same Influxdb instance.

6. Encrypt influxdb password

   If you need to encrypt influxdb's password, you can do it like this：
   
   **i.** run following commands in ${KYLIN_HOME}, it will print encrypted password
   ```shell
   ./bin/kylin.sh org.apache.kylin.tool.general.CryptTool -e AES -s <password>
   ```

   **ii.** config kylin.influxdb.password like this
   ```properties
   kylin.influxdb.password=ENC('${encrypted_password}')
   ```
   
   **iii.** Here is an example, assuming influxdb's password is kylin
   
   First, we need to encrypt kylin using the following command   
    ```shell
    ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.general.CryptTool -e AES -s kylin
    AES encrypted password is:
    YeqVr9MakSFbgxEec9sBwg==
    ```
    Then, config kylin.metadata.url like this：
    ```properties
   kylin.influxdb.password=ENC('YeqVr9MakSFbgxEec9sBwg==')
    ```

7. start Kylin.

### <span id="not_root">Installation and Configuration for `Non root` User </span>

The following steps are using InfluxDB 1.6.4 as an example.


1. Suppose you install as user `abc` . Then create a directory `home/abc/influx` to copy the InfluxDB installation package, `influxdb-1.6.4.x86_64.rpm` ,from `$KYLIN_HOME/influxdb` to this directory after executing download influxDB script.

   ```sh
   # download influxDB
   $KYLIN_HOME/sbin/download-influxdb.sh
   
   mkdir /home/abc/influx
   cp $KYLIN_HOME/influxdb/influxdb-1.6.4.x86_64.rpm /home/abc/influx
   cd /home/abc/influx
   rpm2cpio influxdb-1.6.4.x86_64.rpm | cpio -idmv
   ```

2. Edit the InfluxDB configuration file and replace `/var/lib` with `/home/abc/influx` globally. Also, you can modify `bind-address` property according to your case.

   ```sh
   vi /home/abc/influx/etc/influxdb/influxdb.conf
   ```

3. Run following command to launch InfluxDB.

  ```sh
   nohup ./usr/bin/influxd run -config /home/abc/influx/etc/influxdb/influxdb.conf &
  ```
  By default, you can find InfluxDB's log at `/home/abc/influx/var/log/influxdb`.

4. As for other configurations, please refer to the second part in this section [`root` User Installation And Configuration](#root). Note that if you want to restart influxdb, you need to execute the following commands. Using `service influxdb restart` will not work since it requires root permission.

   ```sh
   ps -ef | grep influxdb
   kill {pid}
   ```

5. Launch Kylin.


### <span id="service">InfluxDB Connectivity</span>     
To ensure the connectivity of InfluxDB service, it is recommended that you perform some tests after starting InfluxDB.
- Log in to InfluxDB by entering the command line in the terminal:
  ```sh
  /home/abc/influx/usr/bin/influx -port 18086 -username ${username} -password ${pwd}
  ```
  If the login fails, you can set `auth-enabled = true` in the configuration file `influxdb.conf` and try to login again.
- After successful login , you can execute some simple queries to check if InfluxDB is configured correctly:
  ```sql
  show databases;
  ```
  If the query fails and the message `authorization failed` is displayed, please confirm whether the user has sufficient permissions.

For more information about InfluxDB connectivity, please refer to the [InfluxDB Maintenance](influxdb_maintenance.md) section.



### <span id="https">（optional）Configure HTTPS connection to InfluxDB </span>  

Before using HTTPS to connect to InfluxDB, you need to enable InfluxDB's HTTPS connection. To enable HTTPS for InfluxDB please refer to the official documentation: [Enabling HTTPS with InfluxDB](https://docs.influxdata.com/influxdb/v1.6/administration/https_setup/)。

If the InfluxDB you are using has enabled HTTPS connection, please set the following parameters in the `$KYLIN_HOME/conf/kylin.properties` configuration file:

```
kylin.influxdb.https.enabled=true
```
