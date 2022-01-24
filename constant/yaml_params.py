from enum import Enum


class Params(Enum):
    # global params
    ASSOSICATED_PUBLIC_IP = 'ASSOSICATED_PUBLIC_IP'
    ALWAYS_DESTROY_ALL = 'ALWAYS_DESTROY_ALL'
    S3_URI = 'S3_URI'
    INSTANCE_ID = 'IdOfInstance'
    CLUSTER_NUM = 'ClusterNum'
    CIDR_IP = 'CidrIp'
    IS_SCALED = 'IsScaled'

    # bucket path of stack
    BUCKET_FULL_PATH = 'BucketFullPath'
    BUCKET_PATH = 'BucketPath'

    # Vpc params
    VPC_ID = 'VpcId'
    SUBNET_ID = 'SubnetId'
    SECURITY_GROUP = 'SecurityGroupId'
    INSTANCE_PROFILE = 'InstanceProfileId'
    SUBNET_GROUP_NAME = 'SubnetGroupName'

    # Rds params
    DB_HOST = 'DbHost'

    # Static services params
    STATIC_SERVICES_PRIVATE_IP = 'StaticServicesNodePrivateIp'
    STATIC_SERVICES_PUBLIC_IP = 'StaticServicesNodePublicIp'
    STATIC_SERVICES_NAME = 'StaticServices Node'

    # Zookeeper params
    ZOOKEEPER_IP = 'ZookeeperNode0{num}PrivateIp'
    ZOOKEEPER_PUB_IP = 'ZookeeperNode0{num}PublicIp'
    ZOOKEEPER_INSTANCE_ID = 'IdOfInstanceZookeeper0{num}'
    ZOOKEEPER_HOSTS = 'ZookeepersHost'
    # Prometheus params of Zookeeper
    ZOOKEEPER_NAME = 'Zookeeper_Node_0{num}'
    ZOOKEEPER_NAME_IN_CLUSTER = 'Zookeeper_Node0{num}_In_Cluster_{cluster}'

    # Kylin 4 params
    KYLIN4_PUBLIC_IP = 'Kylin4Ec2InstancePublicIp'
    KYLIN4_PRIVATE_IP = 'Kylin4Ec2InstancePrivateIp'

    # Prometheus params of Kylin 4
    KYLIN_NAME = 'Kylin_Node'
    KYLIN_NAME_IN_CLUSTER = 'Kylin_Node_In_Cluster_{cluster}'
    KYLIN_SPARK_DIRVER_IN_CLUSTER = 'Spark_Driver_Of_Kylin_Node_In_Cluster_{cluster}'
    KYLIN_SCALE_STACK_NAME = 'ec2-kylin-scale-{num}'
    KYLIN_SCALE_NODE_NAME_IN_CLUSTER = 'Kylin_Node_Scale_{num}_In_Cluster_{cluster}'
    KYLIN_SCALE_TARGET_CLUSTER_STACK_NAME = 'ec2-kylin-scale-{num}-in-cluster-{cluster}'

    # Spark master params
    SPARK_MASTER_HOST = 'SparkMasterNodeHost'
    SPARK_PUB_IP = 'SparkMasterEc2InstancePublicIp'

    # Prometheus params of Spark master
    SPARK_MASTER_NAME = 'Spark_Master_Node'
    SPARK_MASTER_NAME_IN_CLUSTER = 'Spark_Master_Node_In_Cluster_{cluster}'
    SPARK_MASTER_METRIC_IN_CLUSTER = 'Metric_Of_Spark_Master_Node_In_Cluster_{cluster}'

    # Spark workers params
    SPARK_WORKER_NUM = 'WorkerNum'
    SPARK_WORKER_ID = 'IdOfInstanceSlave0{num}'
    SPARK_WORKER_PRIVATE_IP = 'Slave0{num}Ec2InstancePrivateIp'
    SPARK_WORKER_PUBLIC_IP = 'Slave0{num}Ec2InstancePublicIp'
    SPARK_SCALED_WORKER_PRIVATE_IP = 'SlaveEc2InstancePrivateIp'
    SPARK_SCALED_WORKER_PUBLIC_IP = 'SlaveEc2InstancePublicIp'

    # Prometheus params of Spark workers
    SPARK_WORKER_NAME = 'Spark_Worker_{num}'
    SPARK_WORKER_NAME_IN_CLUSTER = 'Spark_Worker_{num}_In_Cluster_{cluster}'
    SPARK_SCALE_WORKER_NAME_IN_CLUSTER = 'Spark_Worker_Node_Scale_{num}_In_Cluster_{cluster}'
    SPARK_WORKER_SCALE_TARGET_CLUSTER_STACK_NAME = 'ec2-spark-worker-scale-{num}-in-cluster-{cluster}'
