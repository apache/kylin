On Hadoop CLI installation
===
On-Hadoop-CLI installation is the most common way of installing Kylin. It can be used for demo use, or for those who want to host their own web site to provide Kylin service. The scenario is depicted as:

![On-Hadoop-CLI-installation](on_cli_install_scene.png)

For normal use cases, the application in the above picture means Kylin Web, which contains a web interface for cube building, querying and all sorts of management. Kylin Web launches a query engine for querying and a cube build engine for building cubes. These two engines interact with the components in Hadoop CLI, like hive and hbase.

Except for some prerequisite software installations, the core of Kylin installation is accomplished by running a single script. After running the script, you will be able to build sample cube and query the tables behind the cubes via a unified web interface.

### Environment

Kylin Rquires a properly setup hadoop environment to run. Please take a look [this](Hadoop Environment.md).

### Install Kylin

1. Download latest Kylin binaries at http://kylin.incubator.apache.org/download/
2. export KYLIN_HOME pointing to the extracted Kylin folder
3. Make sure the user has the privilege to run hadoop, hive and hbase cmd in shell. If you are not so sure, you can just run **bin/check-env.sh**, it will print out the detail information if you have some environment issues.
4. To start Kylin, simply run **bin/kylin.sh start**
5. To stop Kylin, simply run **bin/kylin.sh stop**

> If you want to have multiple Kylin instances please refer to [this](Multiple Kylin REST servers.md)

After Kylin started you can visit <http://your_sandbox_ip:7070/kylin>. The username/password is ADMIN/KYLIN. It's a clean Kylin homepage with nothing in there. To start with you can:

1. [Quick play with a sample cube](../Tutorial/Quick play with a sample cube.md)
2. [Create and Build your own cube](../Tutorial/Kylin Cube Creation Tutorial.md)

Here's also a overview introduction on the website [Kylin Web Tutorial](../Tutorial/Kylin Web Tutorial.md)
