---
title: Uninstall
language: en
sidebar_label: Uninstall
pagination_label: Uninstall
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
    - uninstall
draft: false
last_update:
    date: 08/12/2022
---

In this section, we will show you how to uninstall Kylin.

The steps to uninstall Kylin and remove all relevant data are as follows:

1. Run the following command on all Kylin nodes to stop the Kylin instance:

   ```shell
   $KYLIN_HOME/bin/kylin.sh stop
   ```

2. Data backup (optional):

   - Backup metadata before full unloading so that it can be restored when needed.

     ```shell
     $KYLIN_HOME/bin/metastore.sh backup
     ```

     > Notice: We recommend that you copy metadata to more reliable storage devices later.

3. Please check the configuration file `$KYLIN_HOME/conf/kylin.properties`  to determine the name of the working directory. Suppose your item is:

   ```properties
   kylin.hdfs.working.dir=/kylin
   ```

   Please run the following command to delete the working directory:

   ```shell
   hdfs dfs -rm -r /kylin
   ```

4. Please check the configuration file `$KYLIN_HOME/conf/kylin.properties` to confirm the name of the metadata table. Suppose your item is:

   ```properties
   kylin.metadata.url=kylin_metadata@jdbc
   ```

   Please run following commands to delete metadata tables:

   - If you are using PostgreSQL as your metastore:

     - Set environment variable of PostgreSQL user password. Say your PostgreSQL user password is `kylin`:
     
       ```
       export PGPASSWORD=kylin
       ```
     
     - Delete metadata tables:
     
       ```shell
       /usr/pgsql-10/bin/psql -h {hostname} -p {port} -U {user} -d {database} -c "drop table if exists {metadataUrl}"
       ```
     
       Below is a description of the fields:
     
       - hostname: PostgreSQL host address;
       - port: PostgreSQL server port;
       - user: PostgreSQL user password;
       - database: PostgreSQL database name;
       - metadataUrl: PostgreSQL metadata table name, it is `kylin_metadata` in this example.
     
     You can also log in to PostgreSQL and query `drop table if exists {metadataUrl}` to delete your metadata table.
     
   - If you are using MySQL as your metastore, run the following command to delete your metadata tables:

     ```shell
     mysql -h{hostname} -u {root} -p{password} -D {database} -e "drop table if exists {metadataUrl}"
     ```

       Below is a description of the fields:

     - hostname: MySQL host address;
     - user: MySQL user name;
     - password: MySQL user password, please note that there is no space between `-p` and password;
     - database: MySQL metadata database name;
     - metadataUrl: MySQL metadata table name, it is `kylin_metadata` in this example.

     You can also log in to MySQL and query `drop table if exists {metadataUrl}` to delete your metadata table.
	
5. Run the following commands on all Kylin nodes to delete the Kylin installation directory:

   ```shell
   rm -rf $KYLIN_HOME
   ```

At this point, the Kylin uninstall is complete.
