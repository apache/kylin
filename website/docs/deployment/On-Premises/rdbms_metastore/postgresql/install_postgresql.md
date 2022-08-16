---
title: Install PostgreSQL
language: en
sidebar_label: Install PostgreSQL
pagination_label: Install PostgreSQL
toc_min_heading_level: 2
toc_max_heading_level: 6
pagination_prev: null
pagination_next: null
keywords:
   - mysql 
draft: true
last_update:
  date: 08/11/2022
---

### <span id="preparation">Prerequisite</span>

1. For Kylin, we recommend using PostgreSQL as the default metastore database. The PostgreSQL 10.7 installation package is located in the  product package root directory `postgresql`.

2. If using other versions of PostgreSQL, please choose a version above PostgreSQL 9.1.

3. The PostgreSQL installation package currently supports installation in (#TODO) system, the correspondence is as follows:

   - `rhel6.x86_64.rpm` -> CentOS 6
   - `rhel7.x86_64.rpm` -> CentOS 7
   - `rhel8.x86_64.rpm` -> CentOS 8

   Please check out Linux version before choosing the installation package. You should be able to see your Linux core version by running `uname -a` or `cat /etc/issue`.

4. In this section, we will go through a PostgreSQL installation and configuration on CentOS 6.



### <span id="root">`root` User Installation and Configuration</span>

1. After unzipping the Kylin package, enter the root directory `postgresql` and run following commands in order to install PostgreSQL.(#TODO)

   ```shell
   rpm -ivh postgresql10-libs-10.7-1PGDG.rhel6.x86_64.rpm
   rpm -ivh postgresql10-10.7-1PGDG.rhel6.x86_64.rpm
   rpm -ivh postgresql10-server-10.7-1PGDG.rhel6.x86_64.rpm
   ```

2. Initialize PostgreSQL

   The OS has installed Initscripts services, Please run:
   ```sh
   service postgresql-10 initdb
   ```

   The OS not has installed Initscripts services, Please run in the PostgreSQL bin directory:
   ```sh
   $PGSQL_HOME/pgsql-10/bin/postgresql-10-setup initdb
   for example: /user/pgsql-10/bin/postgresql-10-setup initdb
   ```

3. Modify two PostgreSQL configuration files, the files are in `/var/lib/pgsql/10/data/`:

   - `pg_hba.conf`: mainly used to store the authentication information of the client.
   - `postgresql.conf`

   **i.** Run `vi pg_hba.conf` to open the file and you can see  the following initial setting:

   ```properties
   host    all             all             127.0.0.1/32            ident
   ```

   Please the change the above setting to the following:

   ```properties
   host    all             all             127.0.0.1/32            md5
   ```

   > **tips**: The above modification makes you match any users in localhost (IP address is `localhost` or `127.0.0.1`) to connect any databases and validate user password via `md5`.

   At the same time, please append a new line at the end of this file:

   ```properties
   host    all             all             0.0.0.0/0               md5
   ```

   > **tips**: The above modification makes you match any user in any IPV4 address to connect any databases, and validate user password via `md5`.

   **Fields Explanation:**

   - `host`: The connect way, `host` means connecting via TCP / IP;
   - First `all`: Match all databases;
   - Second `all`: Match all users;
   - `0.0.0.0/0`: Match all IPV4 address;
   - `md5`: Validate via `md5`.

   > **tips**: You can set corresponding match rules according to your cases.

   **ii.** Run `vi postgresql.conf` to open another configuration file and modify the following properties:

   ```sh
   listen_addresses = '*'
   ```

   **Field Explanation:**

   - `listen_addresses`: Specify the TCP / IP address listened by server. It is represented by multiple hostnames seperated by comma, for intance, `listen_addresses = host1,host2,host3` or `listen_address = 10.1.1.1,10.1.1.2,10.1.1.3`. The special symbol `*` matches all IP addresses. You can modify the property on demands.
   - `port`: The default value is `5432`. If `5432` is taken, please replace it with an avaliable port.

4. Run `service postgresql-10 start` to launch PostgreSQL

5. Log in to PostgreSQL and create the database

   **i.** Run `su - postgres` to switch to `postgres` user.

   > **Tip:** `postgres` is automatically created by Linux user in the process of PostgreSQL installation.

   **ii.** Run `/usr/pgsql-10/bin/psql` to connect PostgreSQL server.

   The command above will connect to port `5432` by default. If you have changed port number in configuration file `postgresql.conf`, please use `-p` option indicating the port number you set before. For instance, say you set port number as `5433` in `postgresql.conf` file, please run as `/usr/pgsql-10/bin/psql -p 5433`.	

   **iii.** Kylin uses `postgres` as user name to connect PostgreSQL by default, you are required to set password for user `postgres`. Run `ALTER USER postgres PASSWORD 'kylin';` to set user password to `kylin`.

   > **Note:** Please do not forget `;` at the end of the command.

   **iv.** Run `create database kylin;` to create the metadata database, named as `kylin` by default.

   > **Note:** Please do not forget `;` at the end of the command.

   **v.** <span id="metadata">Run `\l` to check if the database was created successfully. If you see picture as below, you have just created a database named `kylin`.</span>

   ![check kylin database](../images/installation_create_postgresqldb.jpg)

   

### <span id="not_root">Non `root` User Installation and Configuration</span>

The following example is that Linux user `abc` installs and configures PostgreSQL.

1. Create a new directory `/home/abc/postgresql`, then unzip the PostgreSQL installation package.(#TODO)

   ```sh
   rpm2cpio postgresql10-libs-10.7-1PGDG.rhel6.x86_64.rpm | cpio -idmv
   rpm2cpio postgresql10-10.7-1PGDG.rhel6.x86_64.rpm | cpio -idmv
   rpm2cpio postgresql10-server-10.7-1PGDG.rhel6.x86_64.rpm | cpio -idmv
   ```

   > **Note**: please make sure user `abc` has **read** and **write** privileges.

2. Edit `~/.bash_profile` file, append `export LD_LIBRARY_PATH=/home/abc/postgresql/usr/pgsql-10/lib` at the end of the file, then run `source ~/.bash_profile` to make it take effect.

3. Configure database

   **i.** Run the following command to initialize database:

   ```sh
   ~/postgresql/usr/pgsql-10/bin/initdb -A md5 -U postgres -W -D ~/postgresql/var/lib/pgsql/10/data/
   ```

   **Fields explanation:**

   - **-A md5**: validate user password via `md5`
   - **-U postgres**: specify user `postgres`
   - **-W**: set password for user `postgres`
   - **-D ~/postgresql/var/lib/pgsql/10/data/**: specify the path where the configuration file is located

   As the picture shows below, input password after run the command above, the password is the password for user `postgres`, say the password is `kylin`.

   ![initialize postgresql](../images/install_initialize_postgresql.png)

   **ii.** Edit configuration file

   **Step 1:** Create the directory for Unix Socket communication via the command below:

   ```sh
   mkdir ~/postgresql/socket
   ```

   **Step 2:** Modify the configuration file `~/postgresql/var/lib/pgsql/10/data/postgresql.conf`:

   ```properties
   listen_addresses = '*'
   unix_socket_directories = '/home/abc/postgresql'
   #port = 5432
   ```

   > **Note**: please make sure current user has **read** and **write** privileges on Unit Socket communication directory `/home/abc/postgresql`.

   **Step 3:** Please append the following line at the end of `~/postgresql/var/lib/pgsql/10/data/pg_hba.conf ` configuration file:

   ```properties
   	host    all             all             0.0.0.0/0               md5
   ```

4. Run the following command to launch PostgreSQL:

   ```sh
   ~/postgresql/usr/pgsql-10/bin/pg_ctl -D ~/postgresql/var/lib/pgsql/10/data/ -l ~/postgresql/var/lib/pgsql/10/pgstartup.log start
   ```

5. Run the following command to connect PostgreSQL:

   ```sh
   ~/postgresql/usr/pgsql-10/bin/psql -U postgres -h localhost
   ```

   The above command will connects to `5432` port. If you modified the setting in configuration, please add `-p` option and set the port. Say you set the port number in `postgresql.conf` to `5436`, please run following command:

   ```sh
   ~/postgresql/usr/pgsql-10/bin/psql -U postgres -h localhost -p 5436
   ```

    After that, please input password as prompted.

6. Run the following command to create a database named `kylin`:

   ```sql
   create database kylin;
   ```

   > **Note:** 
   >
   > - Please do not forget to append `;` at the end of the command.
   > - You can check if `kylin` database was created successfully via `\l` command in PostgreSQL client.

### <span id="faq">FAQ</span>

**Q: How to solve the error `libicu18n.so.42: cannot open shared object file: no such file or directory`  when a non-root user initializes PostgreSQL?**

There are two solutions:

Solution 1: Make sure that the node installing PostgreSQL can access the external network, and then enter the command `yum install libicu-devel` in the terminal to download libicui18n.

Solution 2: Visit the website https://pkgs.org/download/libicu and download the required packages. Please choose the appropriate version according to the system kernel, such as `libicu-4.2.1-1.el6.x86_64.rpm` for CentOS 6. Then use the command `rpm2cpio libicu-4.2.1-14.el6.x86_64.rpm | cpio -idmv` to decompress the binary package and place the decompressed content in ` $LD_LIBRARY_PATH`. If you don't know `$LD_LIBRARY_PATH`, please refer to the second step of [Non `root` User Installation And Configuration](#not_root) above.
