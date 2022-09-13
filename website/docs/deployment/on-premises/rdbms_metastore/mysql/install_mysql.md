---
title: Install MySQL
language: en
sidebar_label: Install MySQL
pagination_label: Install MySQL
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
   - mysql
   - install mysql
   - install
draft: false
last_update:
   date: 09/13/2022
---

### Prerequisite

1. Supported MySQL versions are:

   - MySQL 5.1 to 5.7, MySQL 5.7 is recommended
   - MySQL 8

2. JDBC driver of MySQL is needed in the Kylin running environment.

3. You can download the JDBC driver jar package of MySQL 8 via the link below, that compatible with the version after 5.6:

   https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar

   For other versions, you will have to prepare independently.

4. Please put the corresponding MySQL's JDBC driver to directory `$KYLIN_HOME/lib/ext`. 

### <span id ="not_root">`Non root` User Installation and Configuration</span>

The followings are the steps for a `non root` user `abc` installing MySQL 5.7 on CentOS 7( apply to `root` users as well).

1. Create a new directory `/home/abc/mysql`, and locate MySQL intallation package in the directory, excute the following command to unzip the package of `rpm`:

   ```shell
   cd /home/abc/mysql
   tar -xvf mysql-5.7.37-1.el7.x86_64.rpm-bundle.tar
   ```
   Then you will have the RPM installment package:

   `mysql-community-common-5.7.37-1.el7.x86_64.rpm`
   `mysql-community-libs-5.7.37-1.el7.x86_64.rpm`
   `mysql-community-client-5.7.37-1.el7.x86_64.rpm`
   `mysql-community-server-5.7.37-1.el7.x86_64.rpm`
   `mysql-community-devel-5.7.37-1.el7.x86_64.rpm`

   > **Note:** please prepare MySQL installaion package by yourself 

2. To check if the other version MySQL was already installed in your system environment
   
   ```shell
   For example 1: 
   rpm -qa | grep mysql
   yum -y remove MySQL-server-5.5.61-1.el6.x86_64
   
   For example 2:
   rpm -qa | grep mariadb
   yum -y remove mariadb-libs-5.5.68-1.el7.x86_64
   ```

3. Excute the command as the following order to Unzip package of `rpm` following

   ```shell
   rpm2cpio mysql-community-common-5.7.37-1.el7.x86_64.rpm | cpio -idmv
   rpm2cpio mysql-community-libs-5.7.37-1.el7.x86_64.rpm | cpio -idmv
   rpm2cpio mysql-community-client-5.7.37-1.el7.x86_64.rpm | cpio -idmv
   rpm2cpio mysql-community-server-5.7.37-1.el7.x86_64.rpm | cpio -idmv
   ```

4. Excute `vi ~/mysql/etc/my.cnf` to edit configuration file, and please add the configuration informationn as follows 

   ```properties
   [client]
   port = 3306
   socket=/home/abc/socket/mysql.sock
   [mysql]
   no-auto-rehash
   socket=/home/abc/socket/mysql.sock
   [mysqld]
   user=abc
   basedir=/home/abc/mysql/usr
   datadir=/home/abc/sql_data
   socket=/home/abc/socket/mysql.sock
   secure-file-priv=/home/abc/mysql_files
   port=3306
   ```

   Please create folders corresponding to the configuration informantion above :

   - Create folder `usr` in the path of `/home/abc/mysql`
   - Create folder `sql_data` in the path of `/home/abc` 
   - Create folder `socket` in the path of `/home/abc`
   - Create folder `mysql_files` in the path of `/home/abc` 

   Then, excute the following command in the path of `/home/abc/mysql`

   ```sh
   ./usr/bin/mysql_install_db --defaults-file=etc/my.cnf --user=abc --basedir=/home/abc/mysql/usr --datadir=/home/abc/sql_data
   ```

5. Excute following command to start MySQL in the path of `/home/abc/mysql`:

   ```sh
   ./usr/sbin/mysqld --defaults-file=etc/my.cnf &
   ```

6. To check the default password of MySQL 5.7

   ```sh
   cat ./home/abc/.mysql_secret
   ```
   Login MySQL 5.7 by using default password
   ```sh
   usr/bin/mysql -u root -p
   ```
