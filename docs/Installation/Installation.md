Installation 
------------

### Prerequisites on hadoop###

- Hadoop: 2.2.0.2.0.6.0-61 or above
- Hive: 0.12.0.2.0.6.0-61 or above
- HBase: 0.96.0.2.0.6.0-61-hadoop2

_Tested with Hortonworks HDP 2.1.3 and Cloudera Quickstart VM 5.1._


It is very easy to install Kylin for exploration/development. There are 3 installation scenarios:

#### On-Hadoop-CLI installation ####

If you are free to install Kylin on your hadoop CLI machine or Hadoop sandbox, this is the most convenient scenario, for it puts everything in a single machine.

For a hands-on tutorial please visit [On-Hadoop-CLI installation](On Hadoop CLI installation.md).

#### Dev Environment (Off-Hadoop-CLI Installation) ####

This is typically for development environment setup.

For a hands-on tutorial please visit [Off Hadoop CLI Installation (Dev Env Setup)](Off Hadoop CLI Installation.md).
#### Docker Container ####
With help from [SequenceIQ](http://sequenceiq.com/), there's docker container for Kylin (along with Hadoop, HBase and Hive) available now:[sequenceiq/docker-kylin](https://github.com/sequenceiq/docker-kylin).  The only thing you will need to do is to pull the container from the official Docker repository to be up and running in few minutes. 

Features:

- Size            - Light weight compared to downloading and setting up HDP or CDH sandbox.
- Cluster support - Supports multi node installation. 
- Fully Automated - No manual steps. One command does it all 

For a hands-on tutorial please visit [Kylin Docker installation](On Hadoop Kylin installation using Docker.md). 



