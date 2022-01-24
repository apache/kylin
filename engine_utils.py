import logging
from typing import List

from clouds.aws import AWS
from constant.config import Config
from constant.deployment import NodeType, ScaleType
from constant.yaml_files import Tar
from instances.kylin_utils import KylinUtils
from utils import Utils

logger = logging.getLogger(__name__)


class EngineUtils:

    def __init__(self, config):
        self.aws = AWS(config)
        self.config = config
        
    @property
    def cloud_address(self) -> str:
        return self.config[Config.CLOUD_ADDR.value]

    def needed_tars(self) -> List:
        jdk_package = Tar.JDK.value
        kylin_package = Tar.KYLIN.value.format(KYLIN_VERSION=self.config['KYLIN_VERSION'])
        if self.config[Config.ENABLE_SOFT_AFFINITY.value] == 'true':
            kylin_package = Tar.KYLIN_WITH_SOFT.value.format(KYLIN_VERSION=self.config['KYLIN_VERSION'])
        hive_package = Tar.HIVE.value.format(HIVE_VERSION=self.config['HIVE_VERSION'])
        hadoop_package = Tar.HADOOP.value.format(HADOOP_VERSION=self.config['HADOOP_VERSION'])
        node_exporter_package = Tar.NODE.value.format(NODE_EXPORTER_VERSION=self.config['NODE_EXPORTER_VERSION'])
        prometheus_package = Tar.PROMETHEUS.value.format(PROMETHEUS_VERSION=self.config['PROMETHEUS_VERSION'])
        spark_package = Tar.SPARK.value.format(SPARK_VERSION=self.config['SPARK_VERSION'],
                                               HADOOP_VERSION=self.config['HADOOP_VERSION'])
        zookeeper_package = Tar.ZOOKEEPER.value.format(ZOOKEEPER_VERSION=self.config['ZOOKEEPER_VERSION'])
        packages = [jdk_package, kylin_package, hive_package, hadoop_package, node_exporter_package,
                    prometheus_package, spark_package, zookeeper_package]
        return packages

    def needed_jars(self) -> List:
        # FIXME: hard version of jars
        jars = []
        commons_configuration = 'commons-configuration-1.3.jar'
        mysql_connector = 'mysql-connector-java-5.1.40.jar'
        jars.append(commons_configuration)
        jars.append(mysql_connector)
        if self.config[Config.ENABLE_SOFT_AFFINITY.value] == 'true':
            kylin_soft_affinity_cache = 'kylin-soft-affinity-cache-4.0.0-SNAPSHOT.jar'
            alluxio_client = 'alluxio-2.6.1-client.jar'
            jars.append(kylin_soft_affinity_cache)
            jars.append(alluxio_client)
        return jars

    @staticmethod
    def needed_scripts() -> List:
        kylin = 'prepare-ec2-env-for-kylin4.sh'
        spark_master = 'prepare-ec2-env-for-spark-master.sh'
        spark_slave = 'prepare-ec2-env-for-spark-slave.sh'
        static_services = 'prepare-ec2-env-for-static-services.sh'
        zookeeper = 'prepare-ec2-env-for-zk.sh'
        return [kylin, spark_master, spark_slave, static_services, zookeeper]

    def launch_default_cluster(self):
        cloud_addr = self.get_kylin_address()
        # check kylin status
        KylinUtils.is_kylin_accessible(cloud_addr)

    def destroy_default_cluster(self):
        # Before destroy default cluster, scale down all the scaled nodes
        self.scale_nodes(scale_type=ScaleType.DOWN.value, node_type=NodeType.KYLIN.value)
        self.scale_nodes(scale_type=ScaleType.DOWN.value, node_type=NodeType.SPARK_WORKER.value)

        self.aws.destroy_default_cluster()

    def alive_nodes(self):
        self.aws.alive_nodes()

    def scale_nodes(self, scale_type: str, node_type: str) -> None:
        logger.info(f'Current scaling {scale_type} {node_type} nodes.')
        self.validate_scale_type(scale_type)

        if scale_type == ScaleType.UP.value:
            assert self.is_cluster_ready() is True, 'Cluster nodes must be ready.'
            self.aws.scale_up(node_type=node_type)
            self.aws.after_scale_up(node_type=node_type)

        elif scale_type == ScaleType.DOWN.value:
            self.aws.after_scale_down(node_type=node_type)
            self.aws.scale_down(node_type=node_type)

        self.aws.restart_prometheus_server()

    def scale_nodes_in_cluster(
            self,
            scale_type: str,
            node_type: str,
            cluster_num: int,
            is_destroy: bool = False) -> None:
        logger.info(f"Current scaling {scale_type} {node_type} nodes "
                    f"in cluster {cluster_num if cluster_num else 'default'}.")
        self.validate_scale_type(scale_type)

        if scale_type == ScaleType.UP.value:
            assert self.is_target_cluster_ready(cluster_num) is True, 'Cluster nodes must be ready.'
            self.aws.scale_up(node_type=node_type, cluster_num=cluster_num, is_destroy=is_destroy)
            self.aws.after_scale_up(node_type=node_type, cluster_num=cluster_num)
        else:
            self.aws.after_scale_down(node_type=node_type, cluster_num=cluster_num)
            self.aws.scale_down(node_type=node_type, cluster_num=cluster_num, is_destroy=is_destroy)

        self.aws.restart_prometheus_server()

    def prepare_for_cluster(self) -> None:
        # create vpc, rds and monitor node for whole cluster
        if not self.is_prepared_for_scale_cluster():
            self.aws.prepare_for_whole_cluster()

    def launch_all_cluster(self) -> None:
        self.aws.launch_clusters()
        self.aws.after_launch_clusters()
        self.aws.restart_prometheus_server()

    def launch_cluster(self, cluster_num: int) -> None:
        self.aws.launch_clusters(cluster_nums=[cluster_num])
        self.aws.after_launch_clusters(cluster_nums=[cluster_num])
        self.aws.restart_prometheus_server()

    def destroy_all_cluster(self) -> None:
        # Before destroy all nodes, scale down the nodes in the cluster
        cluster_nums: List[int] = self.aws.generate_scaled_cluster_nums()
        for num in cluster_nums:
            self.scale_nodes_in_cluster(
                scale_type=ScaleType.DOWN.value,
                node_type=NodeType.KYLIN.value,
                cluster_num=num, is_destroy=True)
            self.scale_nodes_in_cluster(
                scale_type=ScaleType.DOWN.value,
                node_type=NodeType.SPARK_WORKER.value,
                cluster_num=num, is_destroy=True)

        self.aws.after_destroy_clusters(cluster_nums=cluster_nums)
        self.aws.destroy_clusters(cluster_nums=cluster_nums)
        self.aws.restart_prometheus_server()

    def destroy_cluster(self, cluster_num: int) -> None:
        self.scale_nodes_in_cluster(
            scale_type=ScaleType.DOWN.value,
            node_type=NodeType.KYLIN.value,
            cluster_num=cluster_num,
            is_destroy=True)

        self.aws.after_destroy_clusters(cluster_nums=[cluster_num])
        self.aws.destroy_clusters(cluster_nums=[cluster_num])
        self.aws.restart_prometheus_server()

    def destroy_rds_and_vpc(self) -> None:
        self.aws.destroy_rds_and_vpc()

    def is_cluster_ready(self) -> bool:
        if self.cloud_address:
            return True
        return self.aws.is_cluster_ready

    def is_target_cluster_ready(self, cluster_num: int) -> bool:
        return self.aws.is_target_cluster_instances_ready(cluster_num)

    def is_prepared_for_scale_cluster(self) -> bool:
        return self.aws.is_prepared_for_scale_cluster

    def get_kylin_address(self) -> str:
        kylin_address = self.cloud_address
        if not kylin_address:
            kylin_address = self.aws.get_kylin_address()
        assert kylin_address, f'kylin address is None, please check.'
        return kylin_address

    def download_tars(self) -> None:
        logger.info("Downloading packages.")
        packages = self.needed_tars()
        for package in packages:
            Utils.download_tar(filename=package)

    def download_jars(self) -> None:
        logger.info("Downloading jars.")
        jars = self.needed_jars()
        for jar in jars:
            Utils.download_jar(jar)
        if self.config[Config.ENABLE_SOFT_AFFINITY.value] == 'true':
            assert Utils.files_in_jars() == 4, f"Needed jars must be 4, not {Utils.files_in_jars()}, " \
                                         f"which contains {jars}."
        else:
            assert Utils.files_in_jars() == 2, f"Needed jars must be 2, not {Utils.files_in_jars()}, " \
                                         f"which contains {jars}."

    def upload_needed_files(self) -> None:
        logger.info("Start to uploading tars.")
        self.aws.upload_needed_files(self.needed_tars(), self.needed_jars(), self.needed_scripts())
        logger.info("Uploaded tars successfully.")

    def check_needed_files(self) -> None:
        self.aws.check_needed_files(self.needed_tars(), self.needed_jars(), self.needed_scripts())

    def upload_kylin_properties(self, cluster_num: int = None) -> None:
        logger.info(f"Start to uploading kylin.properties for cluster {cluster_num if cluster_num else 'default'}.")
        self.aws.upload_kylin_properties(cluster_num=cluster_num)
        logger.info(f"Uploaded kylin.properties for cluster {cluster_num if cluster_num else 'default'}.")

    def validate_s3_bucket(self) -> None:
        self.aws.validate_s3_bucket()

    def validate_scale_type(self, scale_type: str) -> None:
        self.aws.validate_scale_type(scale_type=scale_type)

    def validate_scale_range(self) -> None:
        self.aws.validate_scale_range()

    def init_kylin_properties(self, cluster_num: int = None) -> None:
        logger.info(f"Start to init kylin properties for cluster {cluster_num if cluster_num else 'default'}.")
        self.aws.init_kylin_properties(cluster_num)
        logger.info(f"Inited kylin properties for cluster {cluster_num if cluster_num else 'default'}.")

    @staticmethod
    def refresh_kylin_properties() -> None:
        logger.info('Start to refresh kylin.poperties in `kylin-tpch/properties/default`.')
        Utils.refresh_kylin_properties()
        logger.info('Refresh kylin.poperties in `kylin-tpch/properties/default` successfully.')

    @staticmethod
    def refresh_kylin_properties_in_clusters(cluster_nums: List) -> None:
        logger.info('Start to refresh kylin.poperties.')
        Utils.refresh_kylin_properties_in_clusters(cluster_nums=cluster_nums)
        logger.info('Refresh kylin.poperties successfully.')

    @staticmethod
    def refresh_kylin_properties_in_default() -> None:
        logger.info('Start to refresh kylin.poperties.')
        Utils.refresh_kylin_properties_in_default()
        logger.info('Refresh kylin.poperties successfully.')
