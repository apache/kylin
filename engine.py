import logging
import os
from typing import List

import yaml

from constant.client import Client
from constant.config import Config
from constant.deployment import ScaleType, NodeType, Cluster
from constant.yaml_files import File
from engine_utils import EngineUtils

logger = logging.getLogger(__name__)


class Engine:

    def __init__(self) -> None:
        d = os.path.dirname(__file__)
        with open(os.path.join(d, File.CONFIG_YAML.value)) as stream:
            config = yaml.safe_load(stream)
        self.config = config
        self.is_ec2_cluster = self.config[Config.DEPLOY_PLATFORM.value] == Client.EC2.value
        self.server_mode = None
        self.engine_utils = EngineUtils(self.config)

    def launch_default_cluster(self):
        logger.info('First launch default Kylin Cluster.')
        self.engine_utils.launch_default_cluster()
        logger.info('Default Kylin Cluster already start successfully.')

    def destroy_default_cluster(self):
        logger.info('Start to destroy default Kylin Cluster.')
        self.engine_utils.destroy_default_cluster()
        logger.info('Destroy default Kylin Cluster successfully.')

    def destroy_rds_and_vpc(self) -> None:
        self.engine_utils.destroy_rds_and_vpc()

    def list_alive_nodes(self) -> None:
        logger.info('Ec2: list alive nodes.')
        self.engine_utils.alive_nodes()
        logger.info('Ec2: list alive nodes successfully.')

    def scale_nodes(self, scale_type: str, node_type: str, cluster_num: int = None) -> None:
        self.engine_utils.validate_scale_range()
        if cluster_num == Cluster.ALL.value:
            raise Exception('Current scale nodes not support to scale for `all` clusters.')

        if cluster_num:
            self.engine_utils.scale_nodes_in_cluster(
                scale_type=scale_type,
                node_type=node_type,
                cluster_num=cluster_num
            )
        else:
            self.engine_utils.scale_nodes(scale_type=scale_type, node_type=node_type)

        if node_type == NodeType.KYLIN.value and scale_type == ScaleType.UP.value:
            logger.info(f'Note: Current Kylin Node already scaled, please wait a moment to access it.')
        logger.info(f"Current scaling {scale_type} {node_type} nodes "
                    f"in {cluster_num if cluster_num else 'default'} cluster successfully.")

    def launch_all_clusters(self) -> None:
        logger.info(f'Current launch other clusters.')
        self.engine_utils.launch_all_cluster()
        logger.info(f'Current launch other clusters successfully.')

    def launch_cluster(self, cluster_num: int) -> None:
        logger.info(f'Current launch cluster {cluster_num}.')
        self.engine_utils.launch_cluster(cluster_num=cluster_num)
        logger.info(f'Current launch cluster {cluster_num} successfully.')
        logger.info(f'Please note that access to Kylin may wait a moment.')

    def destroy_all_cluster(self) -> None:
        logger.info(f'Current destroy all scaled cluster nodes.')
        self.engine_utils.destroy_all_cluster()
        logger.info(f'Current destroy all scaled cluster nodes.')

    def destroy_cluster(self, cluster_num: int) -> None:
        logger.info(f'Current destroy launch cluster {cluster_num}.')
        self.engine_utils.destroy_cluster(cluster_num=cluster_num)
        logger.info(f'Current destroy cluster {cluster_num} successfully.')

    def is_inited_env(self) -> bool:
        try:
            self.engine_utils.check_needed_files()
            return True
        except AssertionError:
            return False

    def init_env(self) -> None:
        self._prepare_files()
        self._prepare_services()

    def _prepare_files(self) -> None:
        self.engine_utils.validate_s3_bucket()
        if self.is_inited_env():
            logger.info('Env already inited, skip init again.')
            return
        self.engine_utils.download_tars()
        self.engine_utils.download_jars()
        self.engine_utils.upload_needed_files()
        # check again
        assert self.is_inited_env()

    def _prepare_services(self) -> None:
        # create vpc, rds and monitor node for whole cluster
        self.engine_utils.prepare_for_cluster()

    def refresh_kylin_properties(self) -> None:
        self.engine_utils.refresh_kylin_properties()

    def refresh_kylin_properties_in_clusters(self, cluster_nums: List[int]) -> None:
        self.engine_utils.refresh_kylin_properties_in_clusters(cluster_nums)

    def refresh_kylin_properties_in_default(self) -> None:
        self.engine_utils.refresh_kylin_properties_in_default()
