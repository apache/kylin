from enum import Enum


class File(Enum):
    CONFIG_YAML = 'kylin_configs.yaml'
    VPC_YAML = 'ec2-or-emr-vpc.yaml'
    RDS_YAML = 'ec2-cluster-rds.yaml'
    STATIC_SERVICE_YAML = 'ec2-cluster-static-services.yaml'
    ZOOKEEPERS_SERVICE_YAML = 'ec2-cluster-zk.yaml'
    KYLIN4_YAML = 'ec2-cluster-kylin4.yaml'
    KYLIN_SCALE_YAML = 'ec2-cluster-kylin4-template.yaml'
    SPARK_MASTER_YAML = 'ec2-cluster-spark-master.yaml'
    SPARK_WORKER_YAML = 'ec2-cluster-spark-slave.yaml'
    SPARK_WORKER_SCALE_YAML = 'ec2-cluster-spark-slave-template.yaml'


class Tar(Enum):
    KYLIN = 'apache-kylin-{KYLIN_VERSION}-bin-spark3.tar.gz'
    KYLIN_WITH_SOFT = 'apache-kylin-{KYLIN_VERSION}-bin-spark3-soft.tar.gz'
    HIVE = 'apache-hive-{HIVE_VERSION}-bin.tar.gz'
    HADOOP = 'hadoop-{HADOOP_VERSION}.tar.gz'
    JDK = 'jdk-8u301-linux-x64.tar.gz'
    NODE = 'node_exporter-{NODE_EXPORTER_VERSION}.linux-amd64.tar.gz'
    PROMETHEUS = 'prometheus-{PROMETHEUS_VERSION}.linux-amd64.tar.gz'
    SPARK = 'spark-{SPARK_VERSION}-bin-hadoop{HADOOP_VERSION!s:3.3s}.tgz'
    ZOOKEEPER = 'zookeeper-{ZOOKEEPER_VERSION}.tar.gz'
