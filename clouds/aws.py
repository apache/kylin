import logging
import os
from ast import literal_eval
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat
from typing import Optional, Dict, List

from constant.config import Config
from constant.deployment import ScaleType, NodeType
from constant.kylin_properties_params import KylinProperties
from constant.path import KYLIN_PROPERTIES_TEMPLATE_DIR
from constant.yaml_params import Params
from instances.aws_instance import AWSInstance
from utils import Utils

logger = logging.getLogger(__name__)


class AWS:

    def __init__(self, config) -> None:
        self.cloud_instance = AWSInstance(config)
        self.config = config

    @property
    def is_cluster_ready(self) -> bool:
        if self.is_instances_ready:
            return True
        msg = f'Current cluster is not ready.'
        logger.warning(msg)
        return False

    @property
    def is_prepared_for_scale_cluster(self) -> bool:
        if self.is_prepared_for_cluster:
            return True
        msg = f'Current env for deploying a cluster is not ready.'
        logger.warning(msg)
        return False

    @property
    def is_instances_ready(self) -> bool:
        return self.cloud_instance.is_ec2_stacks_ready()

    @property
    def is_prepared_for_cluster(self) -> bool:
        return self.cloud_instance.is_prepared_for_scale_cluster()

    @property
    def is_cluster_terminated(self) -> bool:
        if self.is_instances_terminated:
            return True
        msg = 'Current cluster is alive, please destroy cluster first.'
        logger.warning(msg)
        return False

    @property
    def is_instances_terminated(self) -> bool:
        return self.cloud_instance.is_ec2_stacks_terminated()

    @property
    def kylin_stack_name(self) -> str:
        return self.cloud_instance.kylin_stack_name

    @property
    def is_associated_public_ip(self) -> bool:
        return self.config[Params.ASSOSICATED_PUBLIC_IP.value] == 'true'

    @property
    def is_destroy_all(self) -> bool:
        return self.config[Params.ALWAYS_DESTROY_ALL.value] is True

    def is_target_cluster_ready(self, cluster_num: int) -> bool:
        if self.is_target_cluster_instances_ready(cluster_num):
            return True
        msg = f'Current cluster {cluster_num} is not ready, please deploy the cluster first.'
        logger.warning(msg)
        return False
    
    def is_target_cluster_instances_ready(self, cluster_num: int) -> bool:
        return self.cloud_instance.is_target_ec2_stacks_ready(cluster_num)

    def validate_params(self) -> None:
        self.cloud_instance.valid_s3_bucket()
        self.cloud_instance.valid_iam_role()
        self.cloud_instance.valid_key_pair()
        self.cloud_instance.valid_cidr_ip()

    def get_resources(self, stack_name: str) -> Dict:
        return self.cloud_instance.get_stack_output(stack_name)

    def prepare_for_whole_cluster(self) -> None:
        self.validate_params()
        self.cloud_instance.create_vpc_stack()
        self.cloud_instance.create_rds_stack()
        self.cloud_instance.create_static_service_stack()

    def init_cluster(self) -> None:
        if not self.is_instances_ready:
            self.cloud_instance.create_zk_stack()
            # Need to refresh its config and start them after created zks
            self.cloud_instance.after_create_zk_cluster()
            self.cloud_instance.create_spark_master_stack()
            self.cloud_instance.create_spark_slave_stack()
            # Before to create a Kylin node, init local kylin.properties and upload it.
            self.init_kylin_properties()
            self.upload_kylin_properties()
            # # Then create a kylin node
            self.cloud_instance.create_kylin_stack()
        self.cloud_instance.start_prometheus_first_time()
        logger.info('Cluster start successfully.')

    def get_kylin_address(self):
        kylin_resources = self.get_kylin_resources()
        kylin4_address = kylin_resources.get(Params.KYLIN4_PRIVATE_IP.value)
        if self.is_associated_public_ip:
            kylin4_address = kylin_resources.get(Params.KYLIN4_PUBLIC_IP.value)
        return kylin4_address

    def get_kylin_resources(self):
        if not self.is_cluster_ready:
            self.init_cluster()
        kylin_resources = self.get_resources(self.kylin_stack_name)
        return kylin_resources

    def destroy_default_cluster(self):
        self.terminate_ec2_cluster()

    def terminate_ec2_cluster(self) -> Optional[Dict]:
        if self.is_cluster_terminated:
            return
        self.cloud_instance.terminate_spark_slave_stack()
        self.cloud_instance.terminate_spark_master_stack()
        self.cloud_instance.terminate_kylin_stack()
        self.cloud_instance.terminate_zk_stack()
        logger.info('Cluster terminated useless nodes.')

    def destroy_rds_and_vpc(self) -> None:
        if not self.is_destroy_all:
            return
        logger.info('Prepare to destroy RDS and VPC and monitor node.')
        # destroy all will delete rds, please be careful.
        self.cloud_instance.terminate_static_service_stack()
        self.cloud_instance.terminate_rds_stack()
        self.cloud_instance.terminate_vpc_stack()
        logger.info('Destroy RDS and VPC and monitor node done.')

    def alive_nodes(self):
        cluster_nums = self.generate_scaled_cluster_nums()
        self.cloud_instance.alive_nodes(cluster_nums=cluster_nums)

    def scale_up(self, node_type: str, cluster_num: int = None, is_destroy: bool = False) -> None:
        # validate scale_type & node_type
        self.validate_node_type(node_type)
        node_nums = self.generate_scaled_up_list(node_type)

        if node_type == NodeType.KYLIN.value:
            if self.is_kylin_properties_exists(cluster_num=cluster_num):
                self.init_kylin_properties(cluster_num=cluster_num)
                self.upload_kylin_properties(cluster_num=cluster_num)

        exec_pool = ThreadPoolExecutor(max_workers=10)
        with exec_pool as pool:
            if node_type == NodeType.KYLIN.value:
                pool.map(self.cloud_instance.scale_up_kylin, node_nums, repeat(cluster_num, len(node_nums)))
            else:
                pool.map(self.cloud_instance.scale_up_worker, node_nums, repeat(cluster_num, len(node_nums)))

    def scale_down(self, node_type: str, cluster_num: int = None, is_destroy: bool = False) -> None:
        self.validate_node_type(node_type)
        node_nums = self.generate_scaled_down_list(node_type, is_destroy=is_destroy)

        exec_pool = ThreadPoolExecutor(max_workers=10)
        with exec_pool as pool:
            if node_type == NodeType.KYLIN.value:
                pool.map(self.cloud_instance.scale_down_kylin, node_nums, repeat(cluster_num, len(node_nums)))
            else:
                pool.map(self.cloud_instance.scale_down_worker, node_nums, repeat(cluster_num, len(node_nums)))

    def launch_clusters(self, cluster_nums: List[int] = None) -> None:
        if not cluster_nums:
            cluster_nums = self.generate_scaled_cluster_nums()

        for num in cluster_nums:
            self.launch_cluster(cluster_num=num)

    def launch_cluster(self, cluster_num: int) -> None:
        # Before scale up cluster, check the related kylin.properties must exists.
        self.validate_cluster(cluster_num=cluster_num)
        self.is_kylin_properties_exists(cluster_num=cluster_num)
        self.cloud_instance.scale_up_basic_services_for_cluster(cluster_num=cluster_num)
        self.init_kylin_properties(cluster_num=cluster_num)
        self.upload_kylin_properties(cluster_num=cluster_num)
        self.cloud_instance.scale_up_kylin_for_cluster(cluster_num=cluster_num)

    def validate_cluster(self, cluster_num: int) -> None:
        cluster_nums = self.generate_scaled_cluster_nums()
        if cluster_num not in cluster_nums:
            msg = f'Cluster index: {cluster_num} not in the ' \
                  f'range of {self.config[Config.CLUSTER_INDEXES.value]}, please check.'
            raise Exception(msg)

    def destroy_clusters(self, cluster_nums: List) -> None:
        if not cluster_nums:
            cluster_nums = self.generate_scaled_cluster_nums()

        for num in cluster_nums:
            self.cloud_instance.destroy_cluster(num)

    def is_kylin_properties_exists_in_clusters(self, cluster_nums: List) -> None:
        for num in cluster_nums:
            self.is_kylin_properties_exists(cluster_num=num)

    @staticmethod
    def is_kylin_properties_exists(cluster_num: int = None, properties_file: str = 'kylin.properties') -> None:
        dest_path = os.path.join(KYLIN_PROPERTIES_TEMPLATE_DIR.format(
            cluster_num=cluster_num if cluster_num else 'default'), properties_file)
        if not os.path.exists(dest_path):
            raise Exception(f'Expected {dest_path} not exists, please check.')

    def restart_prometheus_server(self) -> None:
        self.cloud_instance.restart_prometheus_server()

    def after_scale_up(self, node_type: str, cluster_num: int = None) -> None:
        logger.info(f"Checking exists prometheus config after scale up.")
        not_exists_nodes = self.cloud_instance.check_prometheus_config_after_scale(node_type, cluster_num)
        logger.info("Refresh prometheus config after scale up.")
        if not_exists_nodes:
            self.cloud_instance.refresh_prometheus_config_after_scale_up(not_exists_nodes)
            if node_type == NodeType.KYLIN.value:
                self.cloud_instance.refresh_prometheus_spark_driver_of_kylin_after_scale_up(not_exists_nodes)

    def after_scale_down(self, node_type: str, cluster_num: int = None) -> None:
        logger.info(f"Checking exists prometheus config after scale down.")
        exists_nodes = self.cloud_instance.check_prometheus_config_after_scale_down(node_type, cluster_num)
        logger.info("Refresh prometheus config after scale down.")
        if exists_nodes:
            self.cloud_instance.refresh_prometheus_config_after_scale_down(exists_nodes)
            if node_type == NodeType.KYLIN.value:
                self.cloud_instance.refresh_prometheus_spark_driver_of_kylin_after_scale_down(exists_nodes)

    def after_launch_clusters(self, cluster_nums: List[int] = None) -> None:
        # cluster_nums is none means deploy all cluster
        if not cluster_nums:
            cluster_nums = self.generate_scaled_cluster_nums()

        logger.info(f"Checking exists prometheus config after launch clusters.")
        not_exists_nodes = self.cloud_instance.check_prometheus_config_after_launch_clusters(cluster_nums)
        not_exists_kylin_nodes, not_exists_spark_nodes = self.cloud_instance\
            .check_spark_metric_config_after_launch_clusters(cluster_nums)
        logger.info("Refresh prometheus config after launch clusters.")
        if not_exists_nodes:
            self.cloud_instance.refresh_prometheus_config_after_launch_clusters(not_exists_nodes)
        if not_exists_kylin_nodes:
            self.cloud_instance.refresh_prometheus_spark_metrics_of_kylin_in_cluster_after_scale_up(
                not_exists_kylin_nodes)
        if not_exists_spark_nodes:
            self.cloud_instance.refresh_prometheus_spark_master_in_cluster_after_scale(not_exists_spark_nodes)

    def after_destroy_clusters(self, cluster_nums: List[int] = None) -> None:
        if not cluster_nums:
            cluster_nums = self.generate_scaled_cluster_nums()
        logger.info(f"Checking exists prometheus config after destroy cluster.")
        exists_nodes = self.cloud_instance.check_prometheus_config_after_destroy_clusters(cluster_nums)
        exists_kylin_nodes, exists_spark_nodes = self.cloud_instance \
            .check_spark_metric_config_after_destroy_clusters(cluster_nums)
        logger.info("Refresh prometheus config after destroy cluster.")
        if exists_nodes:
            self.cloud_instance.refresh_prometheus_config_after_destroy_clusters(exists_nodes)
        if exists_kylin_nodes:
            self.cloud_instance.refresh_prometheus_config_after_scale_down(exists_kylin_nodes)
        if exists_spark_nodes:
            self.cloud_instance.refresh_prometheus_config_after_scale_down(exists_spark_nodes)

    @staticmethod
    def validate_scale_type(scale_type: str) -> None:
        if scale_type is None or scale_type not in [e.value for e in ScaleType]:
            msg = f'Not supported scale type: {scale_type}.'
            logger.error(msg)
            raise Exception(msg)

    def validate_scale_range(self) -> None:
        if not self.cloud_instance.is_valid_scaled_range_of_kylin_nodes:
            msg = 'Scale down range must be less than scale up range in KYLIN_SCALE_*_NODES.'
            raise Exception(msg)

        if not self.cloud_instance.is_valid_scaled_range_of_spark_worker_nodes:
            msg = 'Scale down range must be less than scale up range in SPARK_WORKER_SCALE_*_NODES.'
            raise Exception(msg)

    @staticmethod
    def validate_node_type(node_type: str) -> None:
        if node_type is None or node_type not in [e.value for e in NodeType]:
            msg = f'Not supported None node type.'
            logger.error(msg)
            raise Exception(msg)

    def generate_scaled_up_list(self, node_type: str) -> List:
        if node_type == NodeType.KYLIN.value:
            kylin_nodes = literal_eval(self.config[Config.KYLIN_SCALE_UP_NODES.value])
            return Utils.generate_nodes(kylin_nodes)
        else:
            worker_nodes = literal_eval(self.config[Config.SPARK_WORKER_SCALE_UP_NODES.value])
            return Utils.generate_nodes(worker_nodes)

    def generate_scaled_down_list(self, node_type: str, is_destroy: bool = False) -> List:
        if node_type == NodeType.KYLIN.value:
            if is_destroy:
                scaled_range = self.config[Config.KYLIN_SCALE_UP_NODES.value]
            else:
                scaled_range = self.config[Config.KYLIN_SCALE_DOWN_NODES.value]
            kylin_nodes = literal_eval(scaled_range)
            return Utils.generate_nodes(kylin_nodes)
        else:
            if is_destroy:
                scaled_range = self.config[Config.SPARK_WORKER_SCALE_UP_NODES.value]
            else:
                scaled_range = self.config[Config.SPARK_WORKER_SCALE_DOWN_NODES.value]
            worker_nodes = literal_eval(scaled_range)
            return Utils.generate_nodes(worker_nodes)

    def generate_scaled_cluster_nums(self) -> List:
        cluster_nums = literal_eval(self.config[Config.CLUSTER_INDEXES.value])
        return Utils.generate_nodes(cluster_nums)

    def validate_s3_bucket(self) -> None:
        self.cloud_instance.valid_s3_bucket()

    def init_kylin_properties_of_clusters(self, cluster_nums: List) -> None:
        for num in cluster_nums:
            self.init_kylin_properties(num)

    def init_kylin_properties(self, cluster_num: int = None) -> None:
        params: Dict = {
            # DB params
            KylinProperties.DB_HOST.value: self.cloud_instance.get_db_host(),
            KylinProperties.DB_PORT.value: self.cloud_instance.db_port,
            KylinProperties.DB_USER.value: self.cloud_instance.db_user,
            KylinProperties.DB_PASSWORD.value: self.cloud_instance.db_password,

            # Zookeeper
            KylinProperties.ZOOKEEPER_HOST.value: self.cloud_instance.get_target_cluster_zk_host(cluster_num),

            # S3 bucket path
            KylinProperties.S3_BUCKET_PATH.value: self.cloud_instance.bucket_path,

            # Spark Master
            KylinProperties.SPARK_MASTER.value: self.cloud_instance.get_target_cluster_spark_master_host(cluster_num),
        }
        Utils.render_properties(params=params, cluster_num=cluster_num)

    def upload_needed_files(self, tars: List, jars: List, scripts: List) -> None:
        self.cloud_instance.upload_tars_to_s3(tars)
        self.cloud_instance.upload_jars_to_s3(jars)
        self.cloud_instance.upload_scripts_to_s3(scripts)

    def check_needed_files(self, tars: List, jars: List, scripts: List) -> None:
        self.cloud_instance.check_tars_on_s3(tars)
        self.cloud_instance.check_jars_on_s3(jars)
        self.cloud_instance.check_scripts_on_s3(scripts)

    def upload_kylin_properties_to_clusters(self, cluster_nums: List) -> None:
        for num in cluster_nums:
            self.upload_kylin_properties(cluster_num=num)

    def upload_kylin_properties(self, cluster_num: int = None) -> None:
        # kylin.properties always need to upload
        self.cloud_instance.upload_kylin_properties_to_s3(cluster_num=cluster_num)
