import logging
import os
import re
import time
from ast import literal_eval
from typing import Optional, Dict, List, Tuple

import boto3
import botocore
from botocore.exceptions import ClientError, WaiterError, ParamValidationError

from constant.client import Client
from constant.commands import Commands
from constant.config import Config
from constant.deployment import NodeType, Cluster
from constant.path import JARS_PATH, TARS_PATH, SCRIPTS_PATH, KYLIN_PROPERTIES_TEMPLATE_DIR
from constant.yaml_files import File
from constant.yaml_params import Params
from utils import Utils

logger = logging.getLogger(__name__)


class AWSInstance:

    def __init__(self, config):
        self.config = config
        # global params
        self.vpc_id = None
        self.instance_profile = None
        self.security_group = None
        self.subnet_group = None
        self.subnet_id = None
        self.db_host = None

    @property
    def is_associated_public_ip(self) -> bool:
        return self.config[Params.ASSOSICATED_PUBLIC_IP.value] == 'true'

    @property
    def scaled_spark_workers(self) -> Optional[Tuple]:
        if self.config[Config.SPARK_WORKER_SCALE_UP_NODES.value]:
            return literal_eval(self.config[Config.SPARK_WORKER_SCALE_UP_NODES.value])
        return ()

    @property
    def scaled_down_spark_workers(self) -> Optional[Tuple]:
        if self.config[Config.SPARK_WORKER_SCALE_UP_NODES.value]:
            return literal_eval(self.config[Config.SPARK_WORKER_SCALE_UP_NODES.value])
        return ()

    @property
    def scaled_spark_workers_stacks(self) -> List:
        return self.scaled_target_spark_workers_stacks()

    def scaled_target_spark_workers_stacks(self, cluster_num: int = None) -> List:
        return [
            Params.SPARK_WORKER_SCALE_TARGET_CLUSTER_STACK_NAME.value.format(
                num=i, cluster=cluster_num if cluster_num else 'default')
            for i in Utils.generate_nodes(self.scaled_spark_workers)]

    @property
    def scaled_kylin_nodes(self) -> Optional[Tuple]:
        if self.config[Config.KYLIN_SCALE_UP_NODES.value]:
            return literal_eval(self.config[Config.KYLIN_SCALE_UP_NODES.value])
        return ()

    @property
    def scaled_down_kylin_nodes(self) -> Optional[Tuple]:
        if self.config[Config.KYLIN_SCALE_UP_NODES.value]:
            return literal_eval(self.config[Config.KYLIN_SCALE_DOWN_NODES.value])
        return ()

    @property
    def scaled_kylin_stacks(self) -> List:
        return self.scaled_target_kylin_stacks()

    def scaled_target_kylin_stacks(self, cluster_num: int = None) -> List:
        return [
            Params.KYLIN_SCALE_TARGET_CLUSTER_STACK_NAME.value.format(
                num=i, cluster=cluster_num if cluster_num else 'default')
            for i in Utils.generate_nodes(self.scaled_kylin_nodes)]

    @property
    def region(self) -> str:
        return self.config[Config.AWS_REGION.value]

    @property
    def cf_client(self):
        return boto3.client(Client.CLOUD_FORMATION.value, region_name=self.region)

    @property
    def rds_client(self):
        return boto3.client(Client.RDS.value, region_name=self.region)

    @property
    def ec2_client(self):
        return boto3.client(Client.EC2.value, region_name=self.region)

    @property
    def ssm_client(self):
        return boto3.client('ssm', region_name=self.region)

    @property
    def s3_client(self):
        return boto3.client('s3', region_name=self.region)

    @property
    def iam_client(self):
        return boto3.client('iam', region_name=self.region)

    @property
    def create_complete_waiter(self):
        return self.cf_client.get_waiter('stack_create_complete')

    @property
    def delete_complete_waiter(self):
        return self.cf_client.get_waiter('stack_delete_complete')

    @property
    def exists_waiter(self):
        return self.cf_client.get_waiter('stack_exists')

    @property
    def db_port(self) -> str:
        return self.config[Config.DB_PORT.value]

    @property
    def db_user(self) -> str:
        return self.config[Config.DB_USER.value]

    @property
    def db_password(self) -> str:
        return self.config[Config.DB_PASSWORD.value]

    @property
    def db_identifier(self) -> str:
        return self.config[Config.DB_IDENTIFIER.value]

    @property
    def vpc_stack_name(self) -> str:
        return self.config[Config.VPC_STACK.value]

    @property
    def rds_stack_name(self) -> str:
        return self.config[Config.RDS_STACK.value]

    @property
    def static_service_stack_name(self) -> str:
        return self.config[Config.STATIC_SERVICE_STACK.value]

    @property
    def zk_stack_name(self) -> str:
        return self.config[Config.ZOOKEEPERS_STACK.value]

    @property
    def kylin_stack_name(self) -> str:
        return self.config[Config.KYLIN_STACK.value]

    @property
    def spark_master_stack_name(self) -> str:
        return self.config[Config.SPARK_MASTER_STACK.value]

    @property
    def spark_slave_stack_name(self) -> str:
        return self.config[Config.SPARK_WORKER_STACK.value]

    @property
    def path_of_vpc_stack(self) -> str:
        return Utils.full_path_of_yaml(File.VPC_YAML.value)

    @property
    def path_of_rds_stack(self) -> str:
        return Utils.full_path_of_yaml(File.RDS_YAML.value)

    @property
    def path_of_static_service_stack(self) -> str:
        return Utils.full_path_of_yaml(File.STATIC_SERVICE_YAML.value)

    @property
    def path_of_zk_stack(self) -> str:
        return Utils.full_path_of_yaml(File.ZOOKEEPERS_SERVICE_YAML.value)

    @property
    def path_of_kylin_stack(self) -> str:
        return Utils.full_path_of_yaml(File.KYLIN4_YAML.value)

    @property
    def path_of_kylin_scale_stack(self) -> str:
        return Utils.full_path_of_yaml(File.KYLIN_SCALE_YAML.value)

    @property
    def path_of_spark_master_stack(self) -> str:
        return Utils.full_path_of_yaml(File.SPARK_MASTER_YAML.value)

    @property
    def path_of_spark_slave_stack(self) -> str:
        return Utils.full_path_of_yaml(File.SPARK_WORKER_YAML.value)

    @property
    def path_of_spark_slave_scaled_stack(self) -> str:
        return Utils.full_path_of_yaml(File.SPARK_WORKER_SCALE_YAML.value)

    @property
    def s3_path_match(self) -> re.Match:
        original_path: str = self.config[Params.S3_URI.value]
        match = re.match(pattern=r'^s3://([^/]+)/(.*?([^/]+)/?)$', string=original_path)
        if not match:
            raise Exception(f"Invalid S3 bucket path: {original_path}, please check.")
        return match

    @property
    def is_valid_scaled_range(self) -> bool:
        return self.is_valid_scaled_range_of_kylin_nodes and self.is_valid_scaled_range_of_spark_worker_nodes

    @property
    def is_valid_scaled_range_of_kylin_nodes(self) -> bool:
        return set(self.scaled_down_kylin_nodes).issubset(self.scaled_kylin_nodes)

    @property
    def is_valid_scaled_range_of_spark_worker_nodes(self) -> bool:
        return set(self.scaled_down_spark_workers).issubset(self.scaled_spark_workers)

    @property
    def bucket_full_path(self) -> str:
        full_path = self.s3_path_match.group(0)
        if full_path.endswith('/'):
            full_path = full_path.rstrip('/')
        return full_path

    @property
    def bucket_path(self) -> str:
        # remove the prefix of 's3:/'
        path = self.bucket_full_path[len('s3:/'):]
        return path

    @property
    def bucket(self) -> str:
        # get the bucket
        return self.s3_path_match.group(1)

    @property
    def bucket_dir(self) -> str:
        # Note: this dir is for
        directory = self.s3_path_match.group(2)
        if directory.endswith('/'):
            return directory
        return directory + '/'

    @property
    def bucket_tars_dir(self) -> str:
        return self.bucket_dir + 'tar/'

    @property
    def bucket_scripts_dir(self) -> str:
        return self.bucket_dir + 'scripts/'

    @property
    def bucket_kylin_properties_of_default_cluster_dir(self) -> str:
        return self.bucket_dir + 'properties/default/'

    @property
    def bucket_kylin_properties_of_other_cluster_dir(self) -> str:
        return self.bucket_dir + 'properties/{cluster_num}/'

    @property
    def bucket_jars_dir(self) -> str:
        return self.bucket_dir + 'jars/'

    @property
    def iam_role(self) -> str:
        return self.config[Config.IAM.value]

    @property
    def key_pair(self) -> str:
        return self.config[Config.KEY_PAIR.value]

    @property
    def instance_id_of_static_services(self) -> str:
        return self.get_instance_id(self.static_service_stack_name)

    @property
    def cidr_ip(self) -> str:
        return self.config[Config.CIDR_IP.value]

    def get_vpc_id(self) -> str:
        if not self.vpc_id:
            self.vpc_id = self.get_specify_resource_from_output(
                self.vpc_stack_name, Params.VPC_ID.value
            )
            assert self.vpc_id, f'vpc id must not be empty or None.'
        return self.vpc_id

    def get_instance_profile(self) -> str:
        if not self.instance_profile:
            self.instance_profile = self.get_specify_resource_from_output(
                self.vpc_stack_name, Params.INSTANCE_PROFILE.value
            )
        return self.instance_profile

    def get_subnet_id(self) -> str:
        if not self.subnet_id:
            self.subnet_id = self.get_specify_resource_from_output(self.vpc_stack_name, Params.SUBNET_ID.value)
            assert self.subnet_id, 'subnet id must not be empty or None.'
        return self.subnet_id

    def get_subnet_group(self) -> str:
        if not self.subnet_group:
            self.subnet_group = self.get_specify_resource_from_output(
                self.vpc_stack_name, Params.SUBNET_GROUP_NAME.value
            )
            assert self.subnet_group, 'subnet group must not be empty or None.'
        return self.subnet_group

    def get_db_host(self) -> str:
        if not self.db_host:
            self.db_host = self.get_rds_describe()['Endpoint']['Address']
            assert self.db_host, 'db_host must not be empty or None.'
        return self.db_host

    def get_security_group_id(self) -> str:
        if not self.security_group:
            self.security_group = self.get_specify_resource_from_output(
                self.vpc_stack_name, Params.SECURITY_GROUP.value
            )
            assert self.security_group, f'security_group_id must not be empty or None.'
        return self.security_group

    def get_instance_id(self, stack_name: str) -> str:
        if not self.is_stack_complete(stack_name):
            return ""
        return self.get_specify_resource_from_output(stack_name, Params.INSTANCE_ID.value)

    def get_instance_ids_of_slaves_stack(self) -> List:
        return [
            self.get_specify_resource_from_output(
                self.spark_slave_stack_name, f'IdOfInstanceOfSlave0{i}'
            )
            for i in range(1, 4)
        ]

    # ============ VPC Services Start ============
    def create_vpc_stack(self) -> Optional[Dict]:
        if self.is_stack_complete(self.vpc_stack_name):
            return
        params: Dict = self.config[Config.EC2_VPC_PARAMS.value]
        params[Params.CIDR_IP.value] = self.cidr_ip
        resp = self.create_stack(
            stack_name=self.vpc_stack_name,
            file_path=self.path_of_vpc_stack,
            params=params,
            is_capability=True
        )
        return resp

    def terminate_vpc_stack(self) -> Optional[Dict]:
        resp = self.terminate_stack_by_name(self.vpc_stack_name)
        # Make sure that vpc stack deleted successfully.
        assert self.is_stack_deleted_complete(self.vpc_stack_name), \
            f'{self.vpc_stack_name} deleted failed, please check.'
        return resp

    # ============ VPC Services End ============

    # ============ RDS Services Start ============
    def get_rds_describe(self) -> Optional[Dict]:
        if not self.db_identifier:
            raise Exception(f'{Config.DB_IDENTIFIER.value} must not be empty or None.')
        if not self.is_rds_exists():
            return
        describe_rds: Dict = self.rds_client.describe_db_instances(DBInstanceIdentifier=self.db_identifier)
        db_instances: List = describe_rds['DBInstances']
        assert len(db_instances) == 1, f'the identifier of RDS must exist only one.'
        return db_instances[0]

    def is_rds_exists(self) -> bool:
        try:
            self.rds_client.describe_db_instances(DBInstanceIdentifier=self.db_identifier)
        except self.rds_client.exceptions.DBInstanceNotFoundFault as ex:
            logger.warning(ex.response['Error']['Message'])
            logger.info(f'Now creating {self.db_identifier}.')
            return False
        return True

    def create_rds_stack(self) -> Optional[Dict]:
        if self.is_stack_complete(self.rds_stack_name):
            return
        if self.is_rds_exists():
            logger.warning(f'db {self.db_identifier} already exists.')
            return
        params: Dict = self.config[Config.EC2_RDS_PARAMS.value]
        # update needed params
        params[Params.SUBNET_GROUP_NAME.value] = self.get_subnet_group()
        params[Params.SECURITY_GROUP.value] = self.get_security_group_id()
        resp = self.create_stack(
            stack_name=self.rds_stack_name,
            file_path=self.path_of_rds_stack,
            params=params,
        )
        # Make sure that rds create successfully.
        assert self.is_stack_complete(self.rds_stack_name), f'Rds {self.db_identifier} create failed, please check.'
        return resp

    def terminate_rds_stack(self) -> Optional[Dict]:
        # Note: terminated rds will not delete it, user can delete db manually.
        resp = self.terminate_stack_by_name(self.rds_stack_name)
        return resp

    # ============ RDS Services End ============

    # ============ Static Services Start ============
    def create_static_service_stack(self) -> Optional[Dict]:
        if not self.is_rds_ready():
            msg = f'rds {self.db_identifier} is not ready, please check.'
            logger.warning(msg)
            raise Exception(msg)

        if self.is_stack_complete(self.static_service_stack_name):
            return
        params: Dict = self.config[Config.EC2_STATIC_SERVICES_PARAMS.value]
        # update needed params
        params = self.update_basic_params(params)
        params[Params.DB_HOST.value] = self.get_db_host()

        resp = self.create_stack(
            stack_name=self.static_service_stack_name,
            file_path=self.path_of_static_service_stack,
            params=params
        )
        return resp

    def terminate_static_service_stack(self) -> Optional[Dict]:
        resp = self.terminate_stack_by_name(self.static_service_stack_name)
        return resp

    def get_static_services_instance_id(self) -> str:
        if not self.is_stack_complete(self.static_service_stack_name):
            return ''
        return self.get_specify_resource_from_output(self.static_service_stack_name, Params.INSTANCE_ID.value)

    def get_static_services_private_ip(self) -> str:
        if not self.is_stack_complete(self.static_service_stack_name):
            return ''
        return self.get_specify_resource_from_output(
            self.static_service_stack_name, Params.STATIC_SERVICES_PRIVATE_IP.value)

    def get_static_services_public_ip(self) -> str:
        if not self.is_stack_complete(self.static_service_stack_name):
            return ''
        if not self.is_associated_public_ip:
            logger.warning('Current static services was associated to a public ip.')
            return ''
        return self.get_specify_resource_from_output(
            self.static_service_stack_name, Params.STATIC_SERVICES_PUBLIC_IP.value)

    def get_static_services_basic_msg(self) -> str:
        if not self.is_stack_complete(self.static_service_stack_name):
            return ''
        res = Params.STATIC_SERVICES_NAME.value + '\t' \
              + self.instance_id_of_static_services + '\t' \
              + self.get_static_services_private_ip() + '\t' \
              + self.get_static_services_public_ip()
        return res

    # ============ Static Services End ============

    # ============ Zookeeper Services Start ============
    def get_instance_ids_of_zks_stack(self) -> List:
        return self.get_instance_ids_of_zks_stack_by_name(self.zk_stack_name)

    def get_instance_ids_of_zks_stack_by_name(self, zk_stack_name: str) -> List:
        if not self.is_stack_complete(zk_stack_name):
            return []

        return [
            self.get_specify_resource_from_output(
                zk_stack_name, Params.ZOOKEEPER_INSTANCE_ID.value.format(num=i)
            )
            for i in range(1, 4)
        ]

    def get_instance_ips_of_zks_stack(self) -> List:
        return self.get_instance_ips_of_zks_stack_by_name(self.zk_stack_name)

    def get_instance_ips_of_zks_stack_by_name(self, zk_stack_name: str) -> List:
        if not self.is_stack_complete(zk_stack_name):
            return []
        return [self.get_specify_resource_from_output(
            zk_stack_name, Params.ZOOKEEPER_IP.value.format(num=i))
            for i in range(1, 4)
        ]

    def create_zk_stack(self) -> Optional[Dict]:
        return self.create_zk_stack_by_cluster()

    def create_zk_stack_by_cluster(self, cluster_num: int = None) -> Optional[Dict]:
        if cluster_num:
            zk_stack_name = self.generate_stack_of_scaled_cluster_for_zk(cluster_num)
        else:
            zk_stack_name = self.zk_stack_name

        if self.is_stack_complete(zk_stack_name):
            return
        params: Dict = self.config[Config.EC2_ZOOKEEPERS_PARAMS.value]
        # update needed params
        params = self.update_basic_params(params)

        resp = self.create_stack(
            stack_name=zk_stack_name,
            file_path=self.path_of_zk_stack,
            params=params
        )
        assert self.is_stack_complete(zk_stack_name), f'{zk_stack_name} create failed, please check.'
        return resp

    def terminate_zk_stack(self) -> Optional[Dict]:
        resp = self.terminate_stack_by_name(self.zk_stack_name)
        return resp

    def after_create_zk_cluster(self) -> None:
        self.after_create_zk_of_target_cluster()

    def after_create_zk_of_target_cluster(self, cluster_num: int = None) -> None:
        if cluster_num:
            zk_stack = self.generate_stack_of_scaled_cluster_for_zk(cluster_num)
        else:
            zk_stack = self.zk_stack_name
        zk_ips = self.get_instance_ips_of_zks_stack_by_name(zk_stack)
        zk_ids = self.get_instance_ids_of_zks_stack_by_name(zk_stack)
        # Check related instances status before refresh zks and start them
        for zk_id in zk_ids:
            assert self.is_ec2_instance_running(zk_id), f'Instance {zk_id} is not running, please start it first.'

        # refresh zk cluster cfg, because the zk cfg was not included
        # FIXME: it's hard code to make sure that zks were already initialized.
        time.sleep(10)

        self.refresh_zks_cfg(zk_ips=zk_ips, zk_ids=zk_ids)
        self.start_zks(zk_ids=zk_ids, zk_ips=zk_ips)

    def refresh_zks_cfg(self, zk_ips: List, zk_ids: List) -> None:
        assert len(zk_ips) == 3, f'Initialized zookeeper ips num is 3, not {len(zk_ips)}.'
        assert len(zk_ids) == 3, f'Initialized zookeeper ids num is 3, not {len(zk_ids)}.'
        configured_zks = self.configured_zks_cfg(zk_ids=zk_ids, zk_ips=zk_ips)
        if self.is_configured_zks(configured_zks):
            logger.warning('Zookeepers already configured, skip configure.')
            return
        need_to_configure_zks = [zk_id for zk_id in zk_ids if zk_id not in configured_zks]
        refresh_command = Commands.ZKS_CFG_COMMAND.value.format(host1=zk_ips[0], host2=zk_ips[1], host3=zk_ips[2])
        for zk_id in need_to_configure_zks:
            self.exec_script_instance_and_return(name_or_id=zk_id, script=refresh_command)

    @staticmethod
    def is_configured_zks(configured_zks: List) -> bool:
        return len(configured_zks) == 3

    def configured_zks_cfg(self, zk_ids: List, zk_ips: List) -> List:
        check_command = Commands.ZKS_CHECK_CONFIGURED_COMMAND.value
        configured_instances = []
        for zk_id, zk_ip in zip(zk_ids, zk_ips):
            resp = self.exec_script_instance_and_return(zk_id, check_command.format(host=zk_ip))
            if resp['StandardOutputContent'] == '0\n':
                logger.warning(f'Instance: {zk_id} which ip is {zk_ip} already configured zoo.cfg.')
                configured_instances.append(zk_id)
        return configured_instances

    def start_zks(self, zk_ids: List, zk_ips: List) -> None:
        assert len(zk_ids) == 3, f'Expected to start 3 zookeepers, not {len(zk_ids)}.'
        started_zks = self.started_zks(zk_ids=zk_ids, zk_ips=zk_ips)
        if self.is_started_zks(started_zks=started_zks):
            logger.warning('Zookeepers already started.')
            return

        need_to_start_zks = [zk_id for zk_id in zk_ids if zk_id not in started_zks]
        start_zk_command = Commands.ZKS_START_COMMAND.value
        for zk_id in need_to_start_zks:
            self.exec_script_instance_and_return(name_or_id=zk_id, script=start_zk_command)
        logger.info('Start zookeepers successfully.')

    @staticmethod
    def is_started_zks(started_zks: List) -> bool:
        return len(started_zks) == 3

    def started_zks(self, zk_ids: List, zk_ips: List) -> List:
        check_command = Commands.ZKS_CHECK_STARTED_COMMAND.value
        started_instances = []
        for zk_id, zk_ip in zip(zk_ids, zk_ips):
            resp = self.exec_script_instance_and_return(zk_id, check_command)
            if resp['StandardOutputContent'] == '0\n':
                logger.warning(f'Instance: {zk_id} which ip is {zk_ip} already started.')
                started_instances.append(zk_id)
        return started_instances

    def get_target_cluster_zk_host(self, cluster_num: int = None) -> str:
        if cluster_num:
            zk_stack_name = self.generate_stack_of_scaled_cluster_for_zk(cluster_num)
        else:
            zk_stack_name = self.zk_stack_name
        return self.get_zookeepers_host_by_zk_stack_name(zk_stack_name)

    def get_zookeepers_host_by_zk_stack_name(self, zk_stack_name) -> str:
        zk_ips = self.get_instance_ips_of_zks_stack_by_name(zk_stack_name)
        res = ','.join([zk_ip + ':2181' for zk_ip in zk_ips])
        return res

    def get_zks_basic_msg(self) -> List:
        return self.get_zks_of_target_cluster_msg()

    def get_zks_of_target_cluster_msg(self, cluster_num: int = None) -> List:
        if cluster_num:
            zk_stack_name = self.generate_stack_of_scaled_cluster_for_zk(cluster_num)
        else:
            zk_stack_name = self.zk_stack_name

        if not self.is_stack_complete(zk_stack_name):
            return []

        res = [
            Params.ZOOKEEPER_NAME_IN_CLUSTER.value.format(
                num=i, cluster=cluster_num if cluster_num else 'default') + '\t'
            + self.get_specify_resource_from_output(
                zk_stack_name, Params.ZOOKEEPER_INSTANCE_ID.value.format(num=i)) + '\t'
            + self.get_specify_resource_from_output(zk_stack_name,
                                                    Params.ZOOKEEPER_IP.value.format(num=i)) + '\t'
            + self.get_specify_resource_from_output(zk_stack_name, Params.ZOOKEEPER_PUB_IP.value.format(num=i))
            for i in range(1, 4)
        ]
        return res

    # ============ Zookeeper Services End ============

    # ============ kylin Services Start ============
    def create_kylin_stack(self) -> Optional[Dict]:
        return self.create_kylin_stack_by_cluster()

    def create_kylin_stack_by_cluster(self, cluster_num: int = None) -> Optional[Dict]:
        if cluster_num:
            kylin_stack_name = self.generate_stack_of_scaled_cluster_for_kylin(cluster_num)
            zk_stack = self.generate_stack_of_scaled_cluster_for_zk(cluster_num)
            spark_master_stack = self.generate_stack_of_scaled_cluster_for_spark_master(cluster_num)
        else:
            kylin_stack_name = self.kylin_stack_name
            zk_stack = self.zk_stack_name
            spark_master_stack = self.spark_master_stack_name

        if self.is_stack_complete(kylin_stack_name):
            return

        params: Dict = self.config[Config.EC2_KYLIN4_PARAMS.value]
        params = self.update_basic_params(params)
        # update extra params
        params[Params.SPARK_MASTER_HOST.value] = self.get_spark_master_host_by_name(spark_master_stack)
        params[Params.ZOOKEEPER_HOSTS.value] = self.get_zookeepers_host_by_zk_stack_name(zk_stack)
        params[Params.DB_HOST.value] = self.get_db_host()
        params[Params.CLUSTER_NUM.value] = str(cluster_num) if cluster_num else 'default'

        resp = self.create_stack(
            stack_name=kylin_stack_name,
            file_path=self.path_of_kylin_stack,
            params=params
        )
        assert self.is_stack_complete(kylin_stack_name), f"Create scaled kylin stack not complete, please check."
        return resp

    def terminate_kylin_stack(self) -> Optional[Dict]:
        resp = self.terminate_stack_by_name(self.kylin_stack_name)
        return resp

    def get_kylin_private_ip(self) -> str:
        return self.get_kylin_private_ip_of_target_cluster()

    def get_kylin_private_ip_of_target_cluster(self, cluster_num: int = None) -> str:
        kylin_stack = self.get_kylin_stack_name(cluster_num)
        return self._get_kylin_private_ip(kylin_stack)

    def get_kylin_private_ip_by_name(self, stack_name: str) -> str:
        return self._get_kylin_private_ip(stack_name)

    def get_scaled_kylin_private_ip(self, stack_name: str) -> str:
        return self._get_kylin_private_ip(stack_name)

    def _get_kylin_private_ip(self, stack_name: str) -> str:
        if not self.is_stack_complete(stack_name):
            return ""
        return self.get_specify_resource_from_output(stack_name, Params.KYLIN4_PRIVATE_IP.value)

    def get_kylin_public_ip(self) -> str:
        return self.get_kylin_public_ip_of_target_cluster()

    def get_kylin_public_ip_of_target_cluster(self, cluster_num: int = None) -> str:
        kylin_stack = self.get_kylin_stack_name(cluster_num)
        if not self.is_stack_complete(kylin_stack):
            return ''
        if not self.is_associated_public_ip:
            logger.warning('Current kylin was associated to a public ip.')
            return ''
        return self.get_specify_resource_from_output(kylin_stack, Params.KYLIN4_PUBLIC_IP.value)

    def get_kylin_instance_id(self) -> str:
        return self.get_kylin_instance_id_of_target_cluster()

    def get_kylin_instance_id_of_target_cluster(self, cluster_num: int = None) -> str:
        kylin_stack = self.get_kylin_stack_name(cluster_num)
        if not self.is_stack_complete(kylin_stack):
            return ''
        return self.get_specify_resource_from_output(kylin_stack, Params.INSTANCE_ID.value)

    def get_kylin_basic_msg(self) -> str:
        return self.get_kylin_basic_msg_of_target_cluster()

    def get_kylin_basic_msg_of_target_cluster(self, cluster_num: int = None) -> str:
        if not self.is_stack_complete(self.static_service_stack_name):
            return ''
        if not self.is_stack_complete(self.get_kylin_stack_name(cluster_num=cluster_num)):
            return ''
        res = Params.KYLIN_NAME_IN_CLUSTER.value.format(cluster=cluster_num if cluster_num else 'default') + '\t' \
              + self.get_kylin_instance_id_of_target_cluster(cluster_num=cluster_num) + '\t' \
              + self.get_kylin_private_ip_of_target_cluster(cluster_num=cluster_num) + '\t' \
              + self.get_kylin_public_ip_of_target_cluster(cluster_num=cluster_num)
        return res

    def get_kylin_stack_name(self, cluster_num: int = None) -> str:
        if cluster_num:
            kylin_stack = self.generate_stack_of_scaled_cluster_for_kylin(cluster_num)
        else:
            kylin_stack = self.kylin_stack_name

        return kylin_stack

    def get_scaled_kylin_private_ip(self, stack_name: str) -> str:
        if not self.is_stack_complete(stack_name):
            return ''
        return self.get_specify_resource_from_output(stack_name, Params.KYLIN4_PRIVATE_IP.value)

    def get_scaled_kylin_public_ip(self, stack_name: str) -> str:
        if not self.is_stack_complete(stack_name):
            return ""
        if not self.is_associated_public_ip:
            logger.warning('Current scaled kylin node was associated to a public ip.')
            return ''
        return self.get_specify_resource_from_output(stack_name, Params.KYLIN4_PUBLIC_IP.value)

    def get_scaled_kylin_basic_msg(self) -> List:
        return self.get_scaled_kylin_basic_msg_of_target_cluster()

    def get_scaled_kylin_basic_msg_of_target_cluster(self, cluster_num: int = None) -> List:
        msgs = []
        for stack in self.scaled_target_kylin_stacks(cluster_num):
            if not self.is_stack_complete(stack):
                continue

            instance_id = self.get_instance_id(stack)
            private_ip = self.get_scaled_kylin_private_ip(stack)
            public_ip = self.get_scaled_kylin_public_ip(stack)
            msg = stack + '\t' + instance_id + '\t' + private_ip + '\t' + public_ip + '\t'
            msgs.append(msg)
        return msgs

    def scale_up_kylin(self, kylin_num: int, cluster_num: int = None) -> Optional[Dict]:
        """
        add kylin node
        """

        if cluster_num:
            spark_master_stack = self.generate_stack_of_scaled_cluster_for_spark_master(cluster_num=cluster_num)
            zk_stack = self.generate_stack_of_scaled_cluster_for_zk(cluster_num=cluster_num)
        else:
            spark_master_stack = self.spark_master_stack_name
            zk_stack = self.zk_stack_name

        if not cluster_num:
            cluster_num = Cluster.DEFAULT.value

        stack_name = Params.KYLIN_SCALE_TARGET_CLUSTER_STACK_NAME.value.format(
            num=kylin_num, cluster=cluster_num)

        self._validate_kylin_of_target_cluster_scale(stack_name=stack_name, cluster_num=cluster_num)

        if self.is_stack_complete(stack_name):
            return

        params: Dict = self.config[Config.EC2_KYLIN4_SCALE_PARAMS.value]
        # update extra params
        params = self.update_basic_params(params)
        params[Params.SPARK_MASTER_HOST.value] = self.get_spark_master_host_by_name(spark_master_stack)
        params[Params.ZOOKEEPER_HOSTS.value] = self.get_zookeepers_host_by_zk_stack_name(zk_stack)
        params[Params.DB_HOST.value] = self.get_db_host()
        params[Params.CLUSTER_NUM.value] = str(cluster_num)
        params[Params.IS_SCALED.value] = 'true'

        resp = self.create_stack(
            stack_name=stack_name,
            file_path=self.path_of_kylin_scale_stack,
            params=params
        )
        assert self.is_stack_complete(stack_name)
        return resp

    def scale_down_kylin(self, kylin_num: int, cluster_num: int = None) -> Optional[Dict]:
        if not cluster_num:
            cluster_num = Cluster.DEFAULT.value
        stack_name = Params.KYLIN_SCALE_TARGET_CLUSTER_STACK_NAME.value.format(num=kylin_num, cluster=cluster_num)
        self._validate_kylin_of_target_cluster_scale(stack_name=stack_name, cluster_num=cluster_num)

        # before terminate and delete stack, the worker should be decommissioned.
        resp = self.terminate_stack_by_name(stack_name)

        assert self.is_stack_deleted_complete(stack_name)
        return resp

    # ============ kylin Services End ============

    # ============ Spark Master Services Start ============
    def get_target_cluster_spark_master_host(self, cluster_num: int = None) -> str:
        if cluster_num:
            spark_master_stack = self.generate_stack_of_scaled_cluster_for_spark_master(cluster_num)
        else:
            spark_master_stack = self.spark_master_stack_name

        return self.get_spark_master_host_by_name(spark_master_stack)

    def get_spark_master_host(self) -> str:
        return self.get_spark_master_host_of_target_cluster()

    def get_spark_master_host_of_target_cluster(self, cluster_num: int = None) -> str:
        spark_master_stack = self.get_spark_master_stack_name(cluster_num)
        return self.get_spark_master_host_by_name(spark_master_stack)

    def get_spark_master_host_by_name(self, spark_master_stack_name: str) -> str:
        if not self.is_stack_complete(spark_master_stack_name):
            return ''

        return self.get_specify_resource_from_output(spark_master_stack_name, Params.SPARK_MASTER_HOST.value)

    def get_spark_master_public_ip(self) -> str:
        return self.get_spark_master_public_ip_of_target_cluster()

    def get_spark_master_public_ip_of_target_cluster(self, cluster_num: int = None) -> str:
        spark_master_stack = self.get_spark_master_stack_name(cluster_num)
        if not self.is_stack_complete(spark_master_stack):
            return ''

        return self.get_specify_resource_from_output(spark_master_stack, Params.SPARK_PUB_IP.value)

    def get_spark_master_stack_name(self, cluster_num: int = None) -> str:
        if cluster_num:
            spark_master_stack_name = self.generate_stack_of_scaled_cluster_for_spark_master(cluster_num)
        else:
            spark_master_stack_name = self.spark_master_stack_name

        return spark_master_stack_name

    def create_spark_master_stack(self) -> Optional[Dict]:
        return self.create_spark_master_stack_by_cluster()

    def create_spark_master_stack_by_cluster(self, cluster_num: int = None) -> Optional[Dict]:
        if cluster_num:
            spark_master_stack_name = self.generate_stack_of_scaled_cluster_for_spark_master(cluster_num)
        else:
            spark_master_stack_name = self.spark_master_stack_name

        if self.is_stack_complete(spark_master_stack_name):
            return

        params: Dict = self.config[Config.EC2_SPARK_MASTER_PARAMS.value]
        # update needed params
        params = self.update_basic_params(params)
        params[Params.DB_HOST.value] = self.get_db_host()
        resp = self.create_stack(
            stack_name=spark_master_stack_name,
            file_path=self.path_of_spark_master_stack,
            params=params
        )

        assert self.is_stack_complete(spark_master_stack_name), \
            f'Current {spark_master_stack_name} stack not create complete, please check.'
        return resp

    def terminate_spark_master_stack(self) -> Optional[Dict]:
        resp = self.terminate_stack_by_name(self.spark_master_stack_name)
        return resp

    def start_spark_master(self) -> None:
        spark_master_id = self.get_instance_id(self.spark_master_stack_name)
        if not self.is_ec2_instance_running(spark_master_id):
            msg = f'Instance of spark master{spark_master_id} was not running, please start it first.'
            logger.error(msg)
            raise Exception(msg)

        start_command = Commands.START_SPARK_MASTER_COMMAND.value
        self.exec_script_instance_and_return(name_or_id=spark_master_id, script=start_command)

    def get_spark_master_instance_id(self) -> str:
        return self.get_target_cluster_spark_master_host()

    def get_spark_master_instance_id_of_target_cluster(self, cluster_num: int = None) -> str:
        spark_master_stack = self.get_spark_master_stack_name(cluster_num)

        if not self.is_stack_complete(spark_master_stack):
            return ''

        return self.get_specify_resource_from_output(spark_master_stack, Params.INSTANCE_ID.value)

    def get_spark_master_msg(self) -> str:
        return self.get_spark_master_of_target_cluster_msg()

    def get_spark_master_of_target_cluster_msg(self, cluster_num: int = None) -> str:
        if cluster_num:
            spark_master_stack_name = self.generate_stack_of_scaled_cluster_for_spark_master(cluster_num)
        else:
            spark_master_stack_name = self.spark_master_stack_name

        if not self.is_stack_complete(spark_master_stack_name):
            return ''

        res = Params.SPARK_MASTER_NAME_IN_CLUSTER.value.format(
              cluster=cluster_num if cluster_num else 'default') + '\t' \
              + self.get_spark_master_instance_id_of_target_cluster(cluster_num) + '\t' \
              + self.get_spark_master_host_of_target_cluster(cluster_num) + '\t' \
              + self.get_spark_master_public_ip_of_target_cluster(cluster_num)
        return res

    # ============ Spark Master Services End ============

    # ============ Spark Slave Services Start ============
    def create_spark_slave_stack(self) -> Optional:
        return self.create_spark_slave_stack_by_cluster()

    def create_spark_slave_stack_by_cluster(self, cluster_num: int = None) -> Optional[Dict]:
        if cluster_num:
            spark_master_stack = self.generate_stack_of_scaled_cluster_for_spark_master(cluster_num)
            spark_worker_stack = self.generate_stack_of_scaled_cluster_for_spark_worker(cluster_num)
        else:
            spark_master_stack = self.spark_master_stack_name
            spark_worker_stack = self.spark_slave_stack_name

        if not self.is_stack_complete(spark_master_stack):
            msg = f'Spark master {spark_master_stack} must be created before create spark slaves.'
            logger.error(msg)
            raise Exception(msg)

        if self.is_stack_complete(spark_worker_stack):
            return

        params: Dict = self.config[Config.EC2_SPARK_WORKER_PARAMS.value]
        params = self.update_basic_params(params)
        params[Params.SPARK_MASTER_HOST.value] = self.get_spark_master_host_by_name(spark_master_stack)

        resp = self.create_stack(
            stack_name=spark_worker_stack,
            file_path=self.path_of_spark_slave_stack,
            params=params
        )
        return resp

    def terminate_spark_slave_stack(self) -> Optional[Dict]:
        resp = self.terminate_stack_by_name(self.spark_slave_stack_name)
        return resp

    def get_instance_ids_of_spark_slave_stack(self) -> Optional[List]:
        if not self.is_stack_complete(self.spark_slave_stack_name):
            return
        return [
            self.get_specify_resource_from_output(
                self.spark_slave_stack_name, Params.SPARK_WORKER_ID.value.format(num=i))
            for i in range(1, 4)
        ]

    def get_instance_ips_of_spark_slaves_stack(self) -> List:
        return self.get_instance_ips_of_spark_slaves_stack_by_name(self.spark_slave_stack_name)

    def get_instance_ips_of_spark_slaves_stack_by_name(self, stack_name: str) -> List:
        if not self.is_stack_complete(stack_name):
            return []
        return [
            self.get_specify_resource_from_output(
                stack_name, Params.SPARK_WORKER_PRIVATE_IP.value.format(num=i))
            for i in range(1, 4)
        ]

    def get_scaled_spark_worker_private_ip(self, stack_name: str) -> str:
        if not self.is_stack_complete(stack_name):
            return ''
        return self.get_specify_resource_from_output(stack_name, Params.SPARK_SCALED_WORKER_PRIVATE_IP.value)

    def get_scaled_spark_worker_public_ip(self, stack_name: str) -> str:
        if not self.is_stack_complete(stack_name):
            return ''
        if not self.is_associated_public_ip:
            logger.warning('Current spark worker was associated to a public ip.')
            return ''
        return self.get_specify_resource_from_output(stack_name, Params.SPARK_SCALED_WORKER_PUBLIC_IP.value)

    def get_spark_slaves_basic_msg(self) -> List:
        if not self.is_stack_complete(self.spark_slave_stack_name):
            return []

        res = [
            Params.SPARK_WORKER_NAME.value.format(num=i) + '\t'
            + self.get_specify_resource_from_output(
                self.spark_slave_stack_name, Params.SPARK_WORKER_ID.value.format(num=i)) + '\t'
            + self.get_specify_resource_from_output(
                self.spark_slave_stack_name, Params.SPARK_WORKER_PRIVATE_IP.value.format(num=i)) + '\t'
            + (self.get_specify_resource_from_output(
                self.spark_slave_stack_name, Params.SPARK_WORKER_PUBLIC_IP.value.format(num=i))
               if self.is_associated_public_ip else '')
            for i in range(1, 4)
        ]
        return res

    def get_scaled_spark_workers_basic_msg(self) -> List:
        return self.get_scaled_spark_workers_basic_msg_of_target_cluster()

    def get_spark_workers_of_target_cluster_msg(self, cluster_num: int = None) -> str:
        if cluster_num:
            spark_worker_stack_name = self.generate_stack_of_scaled_cluster_for_spark_worker(cluster_num)
        else:
            spark_worker_stack_name = self.spark_slave_stack_name

        if not self.is_stack_complete(spark_worker_stack_name):
            return []

        res = [
            Params.SPARK_WORKER_NAME_IN_CLUSTER.value.format(num=i, cluster=cluster_num) + '\t'
            + self.get_specify_resource_from_output(
                spark_worker_stack_name, Params.SPARK_WORKER_ID.value.format(num=i)) + '\t'
            + self.get_specify_resource_from_output(
                spark_worker_stack_name, Params.SPARK_WORKER_PRIVATE_IP.value.format(num=i)) + '\t'
            + (self.get_specify_resource_from_output(
                spark_worker_stack_name, Params.SPARK_WORKER_PUBLIC_IP.value.format(num=i))
               if self.is_associated_public_ip else '')
            for i in range(1, 4)
        ]
        return res

    def get_scaled_spark_workers_basic_msg_of_target_cluster(self, cluster_num: int = None) -> List:
        msgs = []
        for stack in self.scaled_target_spark_workers_stacks(cluster_num):
            if not self.is_stack_complete(stack):
                continue

            instance_id = self.get_instance_id(stack)
            private_ip = self.get_scaled_spark_worker_private_ip(stack)
            public_ip = self.get_scaled_spark_worker_public_ip(stack)
            msg = stack + '\t' + instance_id + '\t' + private_ip + '\t' + public_ip
            msgs.append(msg)
        return msgs

    def scale_up_basic_services_for_cluster(self, cluster_num: int) -> None:
        # create zookeeper
        self.create_zk_stack_by_cluster(cluster_num)
        self.after_create_zk_of_target_cluster(cluster_num)

        # create spark master
        self.create_spark_master_stack_by_cluster(cluster_num)

        # create spark worker
        self.create_spark_slave_stack_by_cluster(cluster_num)

    def scale_up_kylin_for_cluster(self, cluster_num: int) -> None:
        # create kylin
        self.create_kylin_stack_by_cluster(cluster_num)

    def destroy_cluster(self, cluster_num: int) -> None:
        scaled_kylin_stack_name = self.generate_stack_of_scaled_cluster_for_kylin(cluster_num)
        self.terminate_stack_by_name(scaled_kylin_stack_name)

        scaled_spark_slave_stack_name = self.generate_stack_of_scaled_cluster_for_spark_worker(cluster_num)
        self.terminate_stack_by_name(scaled_spark_slave_stack_name)

        scaled_spark_master_stack_name = self.generate_stack_of_scaled_cluster_for_spark_master(cluster_num)
        self.terminate_stack_by_name(scaled_spark_master_stack_name)

        scaled_zk_stack_name = self.generate_stack_of_scaled_cluster_for_zk(cluster_num)
        self.terminate_stack_by_name(scaled_zk_stack_name)

    def generate_stack_of_scaled_cluster_for_zk(self, cluster_num: int) -> str:
        return f'{self.zk_stack_name}-scaled-cluster-{cluster_num}'

    def generate_stack_of_scaled_cluster_for_kylin(self, cluster_num: int) -> str:
        return f'{self.kylin_stack_name}-scaled-cluster-{cluster_num}'

    def generate_stack_of_scaled_cluster_for_spark_master(self, cluster_num: int) -> str:
        return f'{self.spark_master_stack_name}-scaled-cluster-{cluster_num}'

    def generate_stack_of_scaled_cluster_for_spark_worker(self, cluster_num: int) -> str:
        return f'{self.spark_slave_stack_name}-scaled-cluster-{cluster_num}'

    def scale_up_worker(self, worker_num: int, cluster_num: int = None) -> Optional[Dict]:
        """
        add spark workers for kylin
        """

        if cluster_num:
            spark_master_stack = self.generate_stack_of_scaled_cluster_for_spark_master(num=cluster_num)
        else:
            spark_master_stack = self.spark_master_stack_name

        if not cluster_num:
            cluster_num = Cluster.DEFAULT.value

        stack_name = Params.SPARK_WORKER_SCALE_TARGET_CLUSTER_STACK_NAME.value.format(
            num=worker_num, cluster=cluster_num)

        self._validate_spark_worker_of_target_cluster_scale(stack_name=stack_name, cluster_num=cluster_num)

        if self.is_stack_complete(stack_name):
            return

        params: Dict = self.config[Config.EC2_SPARK_SCALE_SLAVE_PARAMS.value]
        params = self.update_basic_params(params)
        params[Params.SPARK_MASTER_HOST.value] = self.get_spark_master_host_by_name(spark_master_stack)
        params[Params.SPARK_WORKER_NUM.value] = str(worker_num)

        resp = self.create_stack(
            stack_name=stack_name,
            file_path=self.path_of_spark_slave_scaled_stack,
            params=params
        )
        assert self.is_stack_complete(stack_name)
        return resp

    def scale_down_worker(self, worker_num: int, cluster_num: int = None) -> Optional[Dict]:
        if not cluster_num:
            cluster_num = Cluster.DEFAULT.value

        stack_name = Params.SPARK_WORKER_SCALE_TARGET_CLUSTER_STACK_NAME.value.format(
            num=worker_num, cluster=cluster_num)

        self._validate_spark_worker_of_target_cluster_scale(stack_name=stack_name, cluster_num=cluster_num)

        if self.is_stack_deleted_complete(stack_name):
            return

        instance_id = self.get_instance_id(stack_name)
        # spark decommission feature start to be supported in spark 3.1.x.
        # refer: https://issues.apache.org/jira/browse/SPARK-20624.
        try:
            self.exec_script_instance_and_return(
                name_or_id=instance_id, script=Commands.SPARK_DECOMMISION_WORKER_COMMAND.value)
            # FIXME: hard code for sleep spark worker to execute remaining jobs
            # sleep 5 min to ensure all jobs in decommissioned workers are done
            time.sleep(60 * 3)
        except AssertionError as ex:
            logger.error(ex)

        # before terminate and delete stack, the worker should be decommissioned.
        resp = self.delete_stack(stack_name)
        assert self.is_stack_deleted_complete(stack_name)
        return resp
    # ============ Spark Slave Services End ============

    # ============ Prometheus Services Start ============
    def start_prometheus_first_time(self) -> None:
        self.refresh_prometheus_param_map()
        self.start_prometheus_server()

    def start_prometheus_server(self) -> None:
        start_command = Commands.START_PROMETHEUS_COMMAND.value
        instance_id = self.instance_id_of_static_services
        self.exec_script_instance_and_return(name_or_id=instance_id, script=start_command)

    def stop_prometheus_server(self) -> None:
        stop_command = Commands.STOP_PROMETHEUS_COMMAND.value
        instance_id = self.instance_id_of_static_services
        self.exec_script_instance_and_return(name_or_id=instance_id, script=stop_command)

    def restart_prometheus_server(self) -> None:
        if not self.is_stack_complete(self.static_service_stack_name):
            return
        self.stop_prometheus_server()
        self.start_prometheus_server()

    def refresh_prometheus_param_map(self) -> None:
        static_services_id = self.instance_id_of_static_services
        if not static_services_id:
            raise Exception(f'Current static services stack was not create complete, please check.')

        refresh_config_commands = self.refresh_prometheus_commands()
        for command in refresh_config_commands:
            self.exec_script_instance_and_return(name_or_id=static_services_id, script=command)

        # Special support spark metrics into prometheus
        spark_config_commands = self.refresh_spark_metrics_commands()
        for command in spark_config_commands:
            self.exec_script_instance_and_return(name_or_id=static_services_id, script=command)

    def refresh_prometheus_commands(self) -> List:
        params = self.prometheus_param_map()
        # NOTE: the spaces in templates is for prometheus config's indent
        commands = [Commands.PROMETHEUS_CFG_COMMAND.value.format(node=node, host=host) for node, host in params.items()]
        return commands

    def after_scale_prometheus_params_map_of_kylin(self, cluster_num: int = None, is_scale_down: bool = False) -> Dict:
        kylin_ips = self.get_scaled_node_private_ips(cluster_num)[0]

        if is_scale_down:
            nodes = Utils.generate_nodes(self.scaled_down_kylin_nodes)
        else:
            nodes = Utils.generate_nodes(self.scaled_kylin_nodes)

        kylin_node_keys = [Params.KYLIN_SCALE_NODE_NAME_IN_CLUSTER.value.format(
            num=i, cluster=cluster_num if cluster_num else 'default')
            for i in nodes]

        kylin_params_map = dict(zip(kylin_node_keys, kylin_ips))
        return kylin_params_map

    def after_scale_prometheus_params_map_of_spark_worker(
            self,
            cluster_num: int = None,
            is_scale_down: bool = False) -> Dict:
        spark_workers_ips = self.get_scaled_node_private_ips(cluster_num)[1]

        if is_scale_down:
            nodes = Utils.generate_nodes(self.scaled_down_spark_workers)
        else:
            nodes = Utils.generate_nodes(self.scaled_spark_workers)

        spark_workers_keys = [Params.SPARK_SCALE_WORKER_NAME_IN_CLUSTER.value.format(
            num=i, cluster=cluster_num if cluster_num else 'default')
            for i in nodes]

        spark_workers_params_map = dict(zip(spark_workers_keys, spark_workers_ips))

        return spark_workers_params_map

    def after_scale_prometheus_params_map_of_cluster(self, cluster_nums: List[int]) -> Dict:
        nodes_of_cluster_params_map = self.prometheus_param_map_of_scaled_cluster(cluster_nums)
        return nodes_of_cluster_params_map

    def after_launch_or_destroy_prometheus_params_map_of_clusters(self, cluster_nums: List[int]) -> Dict:
        nodes_of_cluster_params_map = self.prometheus_param_map_of_launched_clusters(cluster_nums)
        return nodes_of_cluster_params_map

    def after_scale_prometheus_params_map_of_cluster_for_spark_metric(self, cluster_nums: List[int]) -> (Dict, Dict):
        kylin_params_map = self.prometheus_spark_metric_params_of_kylin_in_cluster(cluster_nums)
        spark_params_map = self.prometheus_spark_metric_params_of_spark_in_cluster(cluster_nums)
        return kylin_params_map, spark_params_map

    def after_launch_or_destroy_prometheus_params_of_clusters_for_spark_metric(self, cluster_nums: List[int]) -> (Dict, Dict):
        kylin_params_map = self.prometheus_spark_metric_params_of_kylin_in_cluster(cluster_nums)
        spark_params_map = self.prometheus_spark_metric_params_of_spark_in_cluster(cluster_nums)
        return kylin_params_map, spark_params_map

    def is_prometheus_configured(self, host: str) -> bool:
        static_services_instance_id = self.instance_id_of_static_services
        check_command = Commands.PROMETHEUS_CFG_CHECK_COMMAND.value.format(node=host)
        output = self.exec_script_instance_and_return(name_or_id=static_services_instance_id, script=check_command)
        return output['StandardOutputContent'] == '0\n'

    def get_prometheus_configured_hosts(self, hosts: List) -> List:
        static_services_instance_id = self.instance_id_of_static_services
        configured_hosts = []
        for host in hosts:
            check_command = Commands.PROMETHEUS_CFG_CHECK_COMMAND.value.format(node=host)
            output = self.exec_script_instance_and_return(name_or_id=static_services_instance_id, script=check_command)
            if output['StandardOutputContent'] == '0\n':
                configured_hosts.append(host)
        return configured_hosts

    def check_prometheus_config_after_scale(self, node_type: str, cluster_num: int = None) -> Dict:
        if node_type == NodeType.KYLIN.value:
            workers_param_map = self.after_scale_prometheus_params_map_of_kylin(cluster_num)
        else:
            workers_param_map = self.after_scale_prometheus_params_map_of_spark_worker(cluster_num)

        return self._check_prometheus_not_exists_nodes(workers_param_map)

    def check_prometheus_config_after_scale_cluster(self, cluster_nums: List[int]) -> Dict:
        params_map = self.after_scale_prometheus_params_map_of_cluster(cluster_nums)
        return self._check_prometheus_not_exists_nodes(params_map)

    def check_prometheus_config_after_launch_clusters(self, cluster_nums: List[int]) -> Dict:
        params_map = self.after_launch_or_destroy_prometheus_params_map_of_clusters(cluster_nums=cluster_nums)
        return self._check_prometheus_not_exists_nodes(params_map)

    def check_spark_metric_config_after_scale_cluster(self, cluster_nums: List[int]) -> (Dict, Dict):
        kylin_params, spark_params = self.after_scale_prometheus_params_map_of_cluster_for_spark_metric(cluster_nums)
        not_exists_kylin_nodes: Dict = self._check_prometheus_not_exists_nodes(kylin_params)
        not_exists_spark_nodes: Dict = self._check_prometheus_not_exists_nodes(spark_params)
        return not_exists_kylin_nodes, not_exists_spark_nodes

    def check_spark_metric_config_after_launch_clusters(self, cluster_nums: List[int]) -> (Dict, Dict):
        kylin_params, spark_params = self.after_launch_or_destroy_prometheus_params_of_clusters_for_spark_metric(
            cluster_nums)
        not_exists_kylin_nodes: Dict = self._check_prometheus_not_exists_nodes(kylin_params)
        not_exists_spark_nodes: Dict = self._check_prometheus_not_exists_nodes(spark_params)
        return not_exists_kylin_nodes, not_exists_spark_nodes

    def check_spark_metric_config_after_scale_down_cluster(self, cluster_nums: List[int]) -> (Dict, Dict):
        kylin_params, spark_params = self.after_scale_prometheus_params_map_of_cluster_for_spark_metric(cluster_nums)
        exists_kylin_nodes: Dict = self._check_prometheus_exists_nodes(kylin_params)
        exists_spark_nodes: Dict = self._check_prometheus_exists_nodes(spark_params)
        return exists_kylin_nodes, exists_spark_nodes

    def check_spark_metric_config_after_destroy_clusters(self, cluster_nums: List[int]) -> (Dict, Dict):
        kylin_params, spark_params = self.after_launch_or_destroy_prometheus_params_of_clusters_for_spark_metric(
            cluster_nums)
        exists_kylin_nodes: Dict = self._check_prometheus_exists_nodes(kylin_params)
        exists_spark_nodes: Dict = self._check_prometheus_exists_nodes(spark_params)
        return exists_kylin_nodes, exists_spark_nodes

    def check_prometheus_config_after_scale_down(self, node_type: str, cluster_num: int = None) -> Dict:
        if node_type == NodeType.KYLIN.value:
            workers_param_map = self.after_scale_prometheus_params_map_of_kylin(cluster_num, is_scale_down=True)
        else:
            workers_param_map = self.after_scale_prometheus_params_map_of_spark_worker(cluster_num, is_scale_down=True)

        return self._check_prometheus_exists_nodes(params=workers_param_map)

    def _check_prometheus_exists_nodes(self, params: Dict) -> Dict:
        exists_nodes: Dict = {}
        for k, v in params.items():
            command = Commands.PROMETHEUS_CFG_CHECK_COMMAND.value.format(node=k)
            output = self.exec_script_instance_and_return(
                name_or_id=self.instance_id_of_static_services, script=command)
            # output['StandardOutputContent'] = '0\n' means the node exists in the prometheus config
            if output['StandardOutputContent'] == '0\n':
                exists_nodes.update({k: v})
        return exists_nodes

    def _check_prometheus_not_exists_nodes(self, params: Dict) -> Dict:
        not_exists_nodes: Dict = {}
        for k, v in params.items():
            command = Commands.PROMETHEUS_CFG_CHECK_COMMAND.value.format(node=k)
            output = self.exec_script_instance_and_return(
                name_or_id=self.instance_id_of_static_services, script=command)
            # output['StandardOutputContent'] = '1\n' means the node not exists in the prometheus config
            if output['StandardOutputContent'] == '1\n':
                not_exists_nodes.update({k: v})
        return not_exists_nodes

    def check_prometheus_config_after_scale_down_cluster(self, cluster_nums: List[int]) -> Dict:
        params_map = self.after_scale_prometheus_params_map_of_cluster(cluster_nums)
        return self._check_prometheus_exists_nodes(params_map)

    def check_prometheus_config_after_destroy_clusters(self, cluster_nums: List[int]) -> Dict:
        params_map = self.after_launch_or_destroy_prometheus_params_map_of_clusters(cluster_nums)
        return self._check_prometheus_exists_nodes(params_map)

    def refresh_prometheus_config_after_launch_clusters(self, expected_nodes: Dict) -> None:
        self.refresh_prometheus_config_after_scale_up(expected_nodes=expected_nodes)

    def refresh_prometheus_config_after_scale_up(self, expected_nodes: Dict) -> None:
        commands = self.refresh_prometheus_commands_after_scale(expected_nodes)
        for command in commands:
            self.exec_script_instance_and_return(name_or_id=self.instance_id_of_static_services, script=command)

    def refresh_prometheus_spark_driver_of_kylin_after_scale_up(self, expected_nodes: Dict) -> None:
        commands = self.refresh_prometheus_spark_driver_of_kylin_after_scale(expected_nodes)
        for command in commands:
            self.exec_script_instance_and_return(name_or_id=self.instance_id_of_static_services, script=command)

    def refresh_prometheus_spark_metrics_of_kylin_in_cluster_after_scale_up(self, expected_nodes: Dict) -> None:
        commands = self.refresh_prometheus_spark_driver_of_kylin_in_cluster_after_scale(expected_nodes)
        for command in commands:
            self.exec_script_instance_and_return(name_or_id=self.instance_id_of_static_services, script=command)

    def refresh_prometheus_config_after_destroy_clusters(self, exists_nodes: Dict) -> None:
        self.refresh_prometheus_config_after_scale_down(exists_nodes)

    def refresh_prometheus_config_after_scale_down(self, exists_nodes: Dict) -> None:
        commands = [Commands.PROMETHEUS_DELETE_CFG_COMMAND.value.format(node=worker) for worker in exists_nodes.keys()]
        for command in commands:
            self.exec_script_instance_and_return(name_or_id=self.instance_id_of_static_services, script=command)

    def refresh_prometheus_spark_driver_of_kylin_after_scale_down(self, exists_nodes: Dict) -> None:
        commands = [Commands.SPARK_DRIVER_METRIC_OF_KYLIN_DELETE_CFG_COMMAND.value.format(node=worker)
                    for worker in exists_nodes.keys()]
        for command in commands:
            self.exec_script_instance_and_return(name_or_id=self.instance_id_of_static_services, script=command)

    # ============ Prometheus Services End ============

    # ============ Utils Services Start ============
    def is_scaled_stacks_in_clusters_terminated(self, cluster_nums: List) -> bool:
        for num in cluster_nums:
            for stack in self.scaled_target_spark_workers_stacks():
                if not self.is_stack_deleted_complete(stack):
                    logger.warning(f'Scaled Spark Workers Stack:{stack} '
                                   f'in Cluster {num} was not terminated, please check.')
                    return False
            for stack in self.scaled_target_kylin_stacks(num):
                if not self.is_stack_deleted_complete(stack):
                    logger.warning(f'Scaled Kylin Stack:{stack} '
                                   f'in Cluster {num} was not terminated, please check.')
                    return False
        return True

    def is_target_clusters_terminated(self, cluster_nums: List) -> bool:
        for num in cluster_nums:
            return self.is_target_cluster_terminated(num)

    def is_target_cluster_terminated(self, cluster_num: int) -> bool:
        zk_stack = self.generate_stack_of_scaled_cluster_for_zk(cluster_num)

        if not self.is_stack_deleted_complete(zk_stack):
            logger.warning(f'Stack {zk_stack} in cluster {cluster_num} was not terminated, please check.')
            return False

        kylin_stack = self.generate_stack_of_scaled_cluster_for_kylin(cluster_num)
        if not self.is_stack_deleted_complete(kylin_stack):
            logger.warning(f'Stack {kylin_stack} in cluster {cluster_num} was not terminated, please check.')
            return False

        spark_master_stack = self.generate_stack_of_scaled_cluster_for_spark_master(cluster_num)
        if not self.is_stack_deleted_complete(spark_master_stack):
            logger.warning(f'Stack {spark_master_stack} in cluster {cluster_num} was not terminated, please check.')
            return False

        spark_workers_stack = self.generate_stack_of_scaled_cluster_for_spark_worker(cluster_num)
        if not self.is_stack_deleted_complete(spark_workers_stack):
            logger.warning(f'Stack {spark_workers_stack} in cluster {cluster_num} was not terminated, please check.')
            return False

        return True


    def update_basic_params(self, params: Dict) -> Dict:
        params[Params.SUBNET_ID.value] = self.get_subnet_id()
        params[Params.SECURITY_GROUP.value] = self.get_security_group_id()
        params[Params.INSTANCE_PROFILE.value] = self.get_instance_profile()
        # update bucket path
        params[Params.BUCKET_FULL_PATH.value] = self.bucket_full_path
        params[Params.BUCKET_PATH.value] = self.bucket_path
        return params

    def terminate_stack_by_name(self, stack_name: str) -> Optional[Dict]:
        if self.is_stack_deleted_complete(stack_name):
            return

        resp = self.delete_stack(stack_name)
        assert self.is_stack_deleted_complete(stack_name), f'Current delete stack {stack_name} failed, please check.'
        return resp

    def create_stack(self, stack_name: str, file_path: str, params: Dict, is_capability: bool = False) -> Dict:
        try:
            if is_capability:
                resp = self.cf_client.create_stack(
                    StackName=stack_name,
                    TemplateBody=Utils.read_template(file_path),
                    Parameters=[{'ParameterKey': k, 'ParameterValue': v} for k, v in params.items()],
                    Capabilities=['CAPABILITY_IAM']
                )
            else:
                resp = self.cf_client.create_stack(
                    StackName=stack_name,
                    TemplateBody=Utils.read_template(file_path),
                    Parameters=[{'ParameterKey': k, 'ParameterValue': v} for k, v in params.items()],
                )
            return resp
        except ParamValidationError as ex:
            logger.error(ex)
        assert self.is_stack_complete(stack_name=stack_name), \
            f"Stack {stack_name} not create complete, please check."

    def delete_stack(self, stack_name: str) -> Dict:
        logger.info(f'Current terminating stack: {stack_name}.')
        resp = self.cf_client.delete_stack(StackName=stack_name)
        return resp

    def refresh_spark_metrics_commands(self) -> List:
        spark_master_host = self.get_spark_master_host()
        # kylin ip will be the host of spark driver and spark worker
        kylin_ip = self.get_kylin_private_ip()
        commands = [
            Commands.SPARK_DRIVER_METRIC_COMMAND.value.format(node='spark_driver_in_kylin', host=kylin_ip),
            Commands.SPARK_WORKER_METRIC_COMMAND.value.format(node='spark_worker_in_kylin', host=kylin_ip),
            Commands.SPARK_APPLICATIONS_METRIC_COMMAND.value.format(node='spark_applications', host=spark_master_host),
            Commands.SPARK_MASTER_METRIC_COMMAND.value.format(node='spark_master', host=spark_master_host),
            Commands.SPARK_EXECUTORS_METRIC_COMMAND.value.format(node='spark_executors', host=spark_master_host),
        ]
        return commands

    @staticmethod
    def refresh_prometheus_commands_after_scale(expected_nodes: Dict) -> List:
        commands = [Commands.PROMETHEUS_CFG_COMMAND.value.format(node=node, host=host)
                    for node, host in expected_nodes.items()]
        return commands

    @staticmethod
    def refresh_prometheus_spark_driver_of_kylin_after_scale(expected_nodes: Dict) -> List:
        commands = [Commands.SPARK_DRIVER_METRIC_OF_KYLIN_COMMAND.value.format(node=node, host=host)
                    for node, host in expected_nodes.items()]
        return commands

    @staticmethod
    def refresh_prometheus_spark_driver_of_kylin_in_cluster_after_scale(expected_nodes: Dict) -> List:
        commands = [Commands.SPARK_DRIVER_METRIC_COMMAND.value.format(node=node, host=host)
                    for node, host in expected_nodes.items()]
        return commands

    @staticmethod
    def refresh_prometheus_spark_executor_in_cluster_after_scale(expected_nodes: Dict) -> List:
        commands = [Commands.SPARK_EXECUTORS_METRIC_COMMAND.value.format(node=node, host=host)
                    for node, host in expected_nodes.items()]
        return commands

    @staticmethod
    def refresh_prometheus_spark_master_in_cluster_after_scale(expected_nodes: Dict) -> List:
        commands = [Commands.SPARK_MASTER_METRIC_COMMAND.value.format(node=node, host=host)
                    for node, host in expected_nodes.items()]
        return commands

    @staticmethod
    def refresh_prometheus_spark_application_in_cluster_after_scale(expected_nodes: Dict) -> List:
        commands = [Commands.SPARK_APPLICATIONS_METRIC_COMMAND.value.format(node=node, host=host)
                    for node, host in expected_nodes.items()]
        return commands

    def prometheus_param_map(self) -> Dict:
        params_map: Dict = {}
        static_ip = self.get_static_services_private_ip()
        static_map = {Params.STATIC_SERVICES_NAME.value: static_ip}

        kylin_ip = self.get_kylin_private_ip()
        kylin_map = {Params.KYLIN_NAME.value: kylin_ip}

        spark_master_ip = self.get_spark_master_host()
        spark_master_map = {Params.SPARK_MASTER_NAME.value: spark_master_ip}

        zk_ips = self.get_instance_ips_of_zks_stack()
        zks_map = {
            Params.ZOOKEEPER_NAME.value.format(num=i): zk_ips[i - 1] for i in range(1, 4)
        }
        spark_slaves_ips = self.get_instance_ips_of_spark_slaves_stack()
        spark_slaves_map = {
            Params.SPARK_WORKER_NAME.value.format(num=i): spark_slaves_ips[i - 1] for i in range(1, 4)
        }
        params_map.update(static_map)
        params_map.update(kylin_map)
        params_map.update(spark_master_map)
        params_map.update(zks_map)
        params_map.update(spark_slaves_map)

        return params_map

    def prometheus_param_map_of_scaled_cluster(self, cluster_nums: List[int]) -> Dict:
        params_map: Dict = {}
        for num in cluster_nums:
            # kylin stack
            kylin_map = self.kylin_param_map_by_cluster_num(num)
            if kylin_map:
                params_map.update(kylin_map)

            # spark master
            spark_master_map = self.spark_master_param_map_by_num(num)
            if spark_master_map:
                params_map.update(spark_master_map)

            # zookeeper
            zks_map = self.zks_param_map_by_num(num)
            if zks_map:
                params_map.update(zks_map)

            # spark worker
            spark_slaves_map = self.spark_worker_param_map(num)
            if not spark_slaves_map:
                params_map.update(spark_slaves_map)

        return params_map

    def prometheus_param_map_of_launched_clusters(self, cluster_nums: List[int]) -> Dict:
        params_map: Dict = {}
        for num in cluster_nums:
            param: Dict = self.prometheus_param_map_of_launched_cluster(cluster_num=num)
            params_map.update(param)

        return params_map

    def prometheus_param_map_of_launched_cluster(self, cluster_num: int) -> Dict:
        params_map: Dict = {}
        # kylin stack
        kylin_map = self.kylin_param_map_by_cluster_num(num=cluster_num)
        if kylin_map:
            params_map.update(kylin_map)

        # spark master
        spark_master_map = self.spark_master_param_map_by_num(num=cluster_num)
        if spark_master_map:
            params_map.update(spark_master_map)

        # zookeeper
        zks_map = self.zks_param_map_by_num(num=cluster_num)
        if zks_map:
            params_map.update(zks_map)

        # spark worker
        spark_slaves_map = self.spark_worker_param_map(num=cluster_num)
        if not spark_slaves_map:
            params_map.update(spark_slaves_map)

        return params_map

    def prometheus_spark_metric_params_of_kylin_in_cluster(self, cluster_nums: List[int]) -> Dict:
        params_map: Dict = {}
        for num in cluster_nums:
            # kylin stack
            kylin_map = self.spark_metric_of_kylin_param_map_by_cluster_num(num)
            if kylin_map:
                params_map.update(kylin_map)
        return params_map

    def prometheus_spark_metric_params_of_spark_in_cluster(self, cluster_nums: List[int]) -> Dict:
        params_map: Dict = {}
        for num in cluster_nums:
            # spark master
            spark_master_map = self.metric_of_spark_master_param_map_by_num(num)
            if spark_master_map:
                params_map.update(spark_master_map)
        return params_map

    def kylin_param_map_by_cluster_num(self, num: int) -> Dict:
        # kylin stack
        kylin_stack = self.generate_stack_of_scaled_cluster_for_kylin(num)
        kylin_ip = self.get_kylin_private_ip_by_name(kylin_stack)
        if not kylin_ip:
            return {}
        kylin_map = {Params.KYLIN_NAME_IN_CLUSTER.value.format(cluster=num): kylin_ip}
        return kylin_map

    def spark_metric_of_kylin_param_map_by_cluster_num(self, num: int) -> Dict:
        # kylin stack
        kylin_stack = self.generate_stack_of_scaled_cluster_for_kylin(num)
        kylin_ip = self.get_kylin_private_ip_by_name(kylin_stack)
        if not kylin_ip:
            return {}
        kylin_map = {Params.KYLIN_SPARK_DIRVER_IN_CLUSTER.value.format(cluster=num): kylin_ip}
        return kylin_map

    def spark_master_param_map_by_num(self, num: int) -> Dict:
        # spark master
        spark_master_stack = self.generate_stack_of_scaled_cluster_for_spark_master(num)
        spark_master_ip = self.get_spark_master_host_by_name(spark_master_stack)
        if not spark_master_ip:
            return {}
        spark_master_map = {Params.SPARK_MASTER_NAME_IN_CLUSTER.value.format(cluster=num): spark_master_ip}
        return spark_master_map

    def metric_of_spark_master_param_map_by_num(self, num: int) -> Dict:
        # spark master
        spark_master_stack = self.generate_stack_of_scaled_cluster_for_spark_master(num)
        spark_master_ip = self.get_spark_master_host_by_name(spark_master_stack)
        if not spark_master_ip:
            return {}
        spark_master_map = {Params.SPARK_MASTER_METRIC_IN_CLUSTER.value.format(cluster=num): spark_master_ip}
        return spark_master_map

    def zks_param_map_by_num(self, num: int) -> Dict:
        # zookeeper
        zk_stack = self.generate_stack_of_scaled_cluster_for_zk(num)
        zk_ips = self.get_instance_ips_of_zks_stack_by_name(zk_stack)
        if not zk_ips:
            return {}
        zks_map = {
            Params.ZOOKEEPER_NAME_IN_CLUSTER.value.format(num=i, cluster=num): zk_ips[i - 1] for i in range(1, 4)
        }
        return zks_map

    def spark_worker_param_map(self, num: int) -> Dict:
        # spark worker
        spark_worker = self.generate_stack_of_scaled_cluster_for_spark_worker(num)
        spark_slaves_ips = self.get_instance_ips_of_spark_slaves_stack_by_name(spark_worker)
        if not spark_slaves_ips:
            return {}
        spark_slaves_map = {
            Params.SPARK_WORKER_NAME_IN_CLUSTER.value.format(num=i, cluster=num): spark_slaves_ips[i - 1]
            for i in range(1, 4)
        }
        return spark_slaves_map

    def zk_param_map(self) -> Dict:
        return dict(zip(self.get_instance_ids_of_zks_stack(), self.get_instance_ips_of_zks_stack()))

    def static_services_param_map(self) -> Dict:
        return dict(zip(self.instance_id_of_static_services, self.get_static_services_private_ip()))

    def spark_master_param_map(self) -> Dict:
        return dict(zip(self.get_spark_master_instance_id(), self.get_spark_master_host()))

    def spark_slaves_param_map(self) -> Dict:
        return dict(zip(self.get_instance_ids_of_spark_slave_stack(), self.get_instance_ips_of_spark_slaves_stack()))

    def kylin_param_map(self) -> Dict:
        return dict(zip(self.get_kylin_instance_id(), self.get_kylin_private_ip()))

    def get_scaled_node_private_ips(self, cluster_num: int = None) -> [List, List]:
        if cluster_num:
            scaled_kylin_stacks = self.scaled_target_kylin_stacks(cluster_num)
            scaled_spark_worker_stacks = self.scaled_target_spark_workers_stacks(cluster_num)
        else:
            # Always get the full scale nodes in the `UP` range
            scaled_kylin_stacks = self.scaled_kylin_stacks
            scaled_spark_worker_stacks = self.scaled_spark_workers_stacks

        scaled_kylin_ips = []
        for stack in scaled_kylin_stacks:
            ip = self.get_scaled_kylin_private_ip(stack)
            if not ip:
                continue
            scaled_kylin_ips.append(ip)

        scaled_workers_ips = []
        for stack in scaled_spark_worker_stacks:
            ip = self.get_scaled_spark_worker_private_ip(stack)
            if not ip:
                continue
            scaled_workers_ips.append(ip)
        return scaled_kylin_ips, scaled_workers_ips

    def get_specify_resource_from_output(self, stack_name: str, resource_type: str) -> str:
        output = self.get_stack_output(stack_name)
        return output[resource_type]

    def get_stack_output(self, stack_name: str) -> Dict:
        is_complete = self._stack_complete(stack_name)
        if not is_complete:
            raise Exception(f"{stack_name} is not complete, please check.")
        output = self.cf_client.describe_stacks(StackName=stack_name)
        """current output format:
        {
            'Stacks' : [{
                            ...,
                            Outputs: [
                                        {
                                            'OutputKey': 'xxx',
                                            'OutputValue': 'xxx',
                                            'Description': ...    
                                        }, 
                                        ...]
                        }],
            'ResponseMEtadata': {...}
        }
        """
        handled_outputs = {entry['OutputKey']: entry['OutputValue']
                           for entry in list(output['Stacks'][0]['Outputs'])}

        return handled_outputs

    def alive_nodes(self, cluster_nums: List = None) -> None:
        # default cluster
        static_msg = self.get_static_services_basic_msg()
        kylin_msg = self.get_kylin_basic_msg()
        spark_master_msg = self.get_spark_master_msg()

        msgs = [m for m in [static_msg, kylin_msg, spark_master_msg] if m]

        spark_slaves_msg = self.get_spark_slaves_basic_msg()
        zks_msg = self.get_zks_basic_msg()

        scaled_kylins_msg = self.get_scaled_kylin_basic_msg()
        scaled_spark_workers_msg = self.get_scaled_spark_workers_basic_msg()

        msgs.extend(zks_msg)
        msgs.extend(spark_slaves_msg)
        msgs.extend(scaled_kylins_msg)
        msgs.extend(scaled_spark_workers_msg)

        # scaled clusters nodes
        if cluster_nums is None:
            cluster_nums = []
        for num in cluster_nums:
            zks_msg_of_target_cluster = self.get_zks_of_target_cluster_msg(num)
            msgs.extend(zks_msg_of_target_cluster)

            spark_master_msg_of_target_cluster = self.get_spark_master_of_target_cluster_msg(num)
            kylin_msg_of_target_cluster = self.get_kylin_basic_msg_of_target_cluster(num)

            for msg in [spark_master_msg_of_target_cluster, kylin_msg_of_target_cluster]:
                if not msg:
                    continue
                msgs.append(msg)

            spark_workers_msgs_of_target_cluster = self.get_spark_workers_of_target_cluster_msg(num)
            msgs.extend(spark_workers_msgs_of_target_cluster)

            scaled_kylins_msg_of_target_cluster = self.get_scaled_kylin_basic_msg_of_target_cluster(num)
            scaled_workers_msg_of_target_cluster = self.get_scaled_spark_workers_basic_msg_of_target_cluster(num)
            msgs.extend(scaled_kylins_msg_of_target_cluster)
            msgs.extend(scaled_workers_msg_of_target_cluster)

        header_msg = '\n=================== List Alive Nodes ===========================\n'
        result = header_msg + f"Stack Name\t\tInstance ID\t\tPrivate Ip\t\tPublic Ip\t\t\n"
        for msg in msgs:
            result += msg + '\n'
        result += header_msg
        logger.info(result)

    def is_ec2_stacks_ready(self) -> bool:
        if not (
                self.is_stack_complete(self.vpc_stack_name)
                and self.is_stack_complete(self.rds_stack_name)
                and self.is_stack_complete(self.static_service_stack_name)
                and self.is_stack_complete(self.zk_stack_name)
                and self.is_stack_complete(self.spark_master_stack_name)
                and self.is_stack_complete(self.spark_slave_stack_name)
                and self.is_stack_complete(self.kylin_stack_name)
        ):
            return False
        return True

    def is_target_ec2_stacks_ready(self, cluster_num: int) -> bool:
        # Note:
        #  target cluster already must contains zookeeper stack,
        #  spark master stack, spark worker stack and kylin_stack
        zk_stack = self.generate_stack_of_scaled_cluster_for_zk(cluster_num)
        kylin_stack = self.generate_stack_of_scaled_cluster_for_kylin(cluster_num)
        spark_master_stack = self.generate_stack_of_scaled_cluster_for_spark_master(cluster_num)
        spark_worker_stack = self.generate_stack_of_scaled_cluster_for_spark_worker(cluster_num)
        if not (
                self.is_stack_complete(zk_stack)
                and self.is_stack_complete(kylin_stack)
                and self.is_stack_complete(spark_master_stack)
                and self.is_stack_complete(spark_worker_stack)
        ):
            return False

        return True

    def is_prepared_for_scale_cluster(self) -> bool:
        if not (
                self.is_stack_complete(self.vpc_stack_name)
                and self.is_stack_complete(self.rds_stack_name)
                and self.is_stack_complete(self.static_service_stack_name)
        ):
            return False
        return True

    def is_ec2_stacks_terminated(self) -> bool:
        deleted_cost_stacks: bool = (
                self.is_stack_deleted_complete(self.static_service_stack_name)
                and self.is_stack_deleted_complete(self.zk_stack_name)
                and self.is_stack_deleted_complete(self.spark_master_stack_name)
                and self.is_stack_deleted_complete(self.spark_slave_stack_name)
                and self.is_stack_deleted_complete(self.kylin_stack_name)
        )
        if not deleted_cost_stacks:
            return False
        if not self.config['ALWAYS_DESTROY_ALL'] \
                or self.is_stack_deleted_complete(self.vpc_stack_name):
            return True
        return False

    def send_command(self, **kwargs) -> Dict:
        instance_ids = kwargs['vm_name']
        script = kwargs['script']
        document_name = "AWS-RunShellScript"
        parameters = {'commands': [script]}
        response = self.ssm_client.send_command(
            InstanceIds=instance_ids,
            DocumentName=document_name,
            Parameters=parameters
        )
        return response

    def get_command_invocation(self, command_id, instance_id) -> Dict:
        response = self.ssm_client.get_command_invocation(CommandId=command_id, InstanceId=instance_id)
        return response

    def exec_script_instance_and_return(self, name_or_id: str, script: str, timeout: int = 20) -> Dict:
        vm_name = None
        if isinstance(name_or_id, str):
            vm_name = [name_or_id]
        response = self.send_command(vm_name=vm_name, script=script)
        command_id = response['Command']['CommandId']
        time.sleep(5)
        start = time.time()
        output = None
        while time.time() - start < timeout * 60:
            output = self.get_command_invocation(
                command_id=command_id,
                instance_id=name_or_id,
            )
            if output['Status'] in ['Delayed', 'Success', 'Cancelled', 'TimedOut', 'Failed']:
                break
            time.sleep(10)
        assert output and output['Status'] == 'Success', \
            f"execute script failed, failed info: {output['StandardErrorContent']}"
        return output

    def stop_ec2_instance(self, instance_id: str):
        self.ec2_client.stop_instances(
            InstanceIds=[
                instance_id,
            ],
            Force=True
        )

    def stop_ec2_instances(self, instance_ids: List):
        self.ec2_client.stop_instances(
            InstanceIds=instance_ids,
            Force=True
        )

    def start_ec2_instance(self, instance_id: str) -> Dict:
        resp = self.ec2_client.start_instances(
            InstanceIds=[instance_id]
        )
        return resp

    def start_ec2_instances(self, instance_ids: List) -> Dict:
        resp = self.ec2_client.start_instances(
            InstanceIds=instance_ids,
        )
        return resp

    def ec2_instance_statuses(self, instance_ids: List) -> Dict:
        resp = self.ec2_client.describe_instance_status(
            Filters=[{
                'Name': 'instance-state-name',
                'Values': ['pending', 'running', 'shutting-down', 'terminated', 'stopping', 'stopped'],
            }],
            InstanceIds=instance_ids,
            # Note: IncludeAllInstances (boolean), Default is false.
            # When `true` , includes the health status for all instances.
            # When `false` , includes the health status for running instances only.
            IncludeAllInstances=True,
        )
        return resp['InstanceStatuses']

    def ec2_instance_status(self, instance_id: str) -> str:
        resp = self.ec2_instance_statuses(instance_ids=[instance_id])
        assert resp, 'Instance statuses must be not empty.'
        return resp[0]['InstanceState']['Name']

    def is_ec2_instance_running(self, instance_id: str) -> bool:
        return self.ec2_instance_status(instance_id) == 'running'

    def is_ec2_instance_stopped(self, instance_id: str) -> bool:
        return self.ec2_instance_status(instance_id) == 'stopped'

    def is_rds_ready(self) -> bool:
        describe_rds: Dict = self.get_rds_describe()
        if not describe_rds:
            return False
        rds_endpoints: Dict = describe_rds['Endpoint']
        # TODO: check rds with password and user is accessible.

        is_rds_available: bool = describe_rds['DBInstanceStatus'] == 'available'
        is_rds_matched_port = str(rds_endpoints['Port']) == self.db_port
        return is_rds_available and is_rds_matched_port

    def valid_s3_bucket(self) -> None:
        if not self.is_s3_directory_exists(self.bucket, self.bucket_dir):
            msg = f'Invalid S3 bucket path: {self.bucket_full_path}, please check.'
            raise Exception(msg)

    def valid_iam_role(self) -> None:
        if not self.is_iam_role_exists():
            msg = f'Invalid IAM role: {self.iam_role}, please check.'
            raise Exception(msg)

    def is_iam_role_exists(self) -> bool:
        try:
            self.iam_client.get_role(RoleName=self.iam_role)
            return True
        except self.iam_client.exceptions.NoSuchEntityException:
            return False

    def valid_key_pair(self) -> None:
        if not self.is_key_pair_exists():
            msg = f'Invalid KeyPair: {self.key_pair}. please check.'
            raise Exception(msg)

    def valid_cidr_ip(self) -> None:
        if not self.is_valid_cidr_ip():
            msg = f'Invalid CidrIp: {self.cidr_ip}. please check.'
            raise Exception(msg)

    def is_key_pair_exists(self) -> bool:
        try:
            self.ec2_client.describe_key_pairs(KeyNames=[self.key_pair])
            return True
        except ClientError:
            return False

    def is_valid_cidr_ip(self) -> bool:
        if not self.cidr_ip:
            return False

        pattern = r'^([0-9]{1,3}\.){3}[0-9]{1,3}($|/(16|24|32))$'
        res = re.match(pattern=pattern, string=self.cidr_ip)
        if not res:
            logger.error(f'Current {self.cidr_ip} not match correct pattern of CidrIp, please check.')
            return False
        return True

    def is_object_exists_on_s3(self, filename: str, bucket: str, bucket_dir: str) -> bool:
        try:
            self.s3_client.head_object(Bucket=bucket, Key=bucket_dir + filename)
        except botocore.exceptions.ClientError as ex:
            assert ex.response['Error']['Code'] == '404'
            return False
        return True

    def is_s3_directory_exists(self, bucket: str, bucket_dir: str) -> bool:
        if bucket_dir.endswith('/'):
            bucket_dir = bucket_dir.rstrip('/')

        resp = self.s3_client.list_objects(Bucket=bucket, Prefix=bucket_dir, Delimiter='/', MaxKeys=1)
        return 'CommonPrefixes' in resp

    def upload_tars_to_s3(self, tars: List) -> None:
        for tar in tars:
            if self.is_object_exists_on_s3(tar, self.bucket, self.bucket_tars_dir):
                logger.info(f'{tar} already exists, skip upload it.')
                continue
            self.upload_file_to_s3(TARS_PATH, tar, self.bucket, self.bucket_tars_dir)

    def check_tars_on_s3(self, tars: List) -> None:
        for tar in tars:
            assert self.is_object_exists_on_s3(tar, self.bucket, self.bucket_tars_dir), \
                f'{tar} not exists, please check.'

    def upload_jars_to_s3(self, jars: List) -> None:
        for jar in jars:
            if self.is_object_exists_on_s3(jar, self.bucket, self.bucket_jars_dir):
                logger.info(f'{jar} already exists, skip upload it.')
                continue
            self.upload_file_to_s3(JARS_PATH, jar, self.bucket, self.bucket_jars_dir)

    def check_jars_on_s3(self, jars: List) -> None:
        for jar in jars:
            assert self.is_object_exists_on_s3(jar, self.bucket, self.bucket_jars_dir), \
                f'{jar} not exists, please check.'

    def upload_scripts_to_s3(self, scripts: List) -> None:
        for script in scripts:
            if self.is_object_exists_on_s3(script, self.bucket, self.bucket_scripts_dir):
                logger.info(f'{script} already exists, skip upload it.')
                continue
            self.upload_file_to_s3(SCRIPTS_PATH, script, self.bucket, self.bucket_scripts_dir)

    def upload_kylin_properties_to_s3(self, cluster_num: int = None, properties_file: str = 'kylin.properties') -> None:
        # Always to upload local kylin.properties
        local_dir = KYLIN_PROPERTIES_TEMPLATE_DIR.format(cluster_num=cluster_num if cluster_num else 'default')
        bucket_dir = self.bucket_kylin_properties_of_other_cluster_dir.format(
            cluster_num=cluster_num if cluster_num else 'default')
        self.upload_file_to_s3(
            local_file_dir=local_dir,
            filename=properties_file,
            bucket=self.bucket,
            bucket_dir=bucket_dir)

    def check_scripts_on_s3(self, scripts: List) -> None:
        for script in scripts:
            assert self.is_object_exists_on_s3(script, self.bucket, self.bucket_scripts_dir), \
                f'{script} not exists, please check.'

    def upload_file_to_s3(self, local_file_dir: str, filename: str, bucket: str, bucket_dir: str) -> None:
        logger.info(f'Uploading {filename} from {local_file_dir} to S3 bucket: {bucket}/{bucket_dir}.')
        self.s3_client.upload_file(os.path.join(local_file_dir, filename), bucket, bucket_dir + filename)
        logger.info(f'Uploaded {filename} successfully.')

    def is_stack_deleted_complete(self, stack_name: str) -> bool:
        if self.is_stack_create_complete(stack_name):
            # return directly if current stack status is 'CREATE_COMPLETE'
            return False
        if self._stack_delete_complete(stack_name):
            return True
        return False

    def is_stack_create_complete(self, stack_name: str) -> bool:
        return self._stack_status_check(name_or_id=stack_name, status='CREATE_COMPLETE')

    def is_stack_delete_complete(self, stack_name: str) -> bool:
        return self._stack_status_check(name_or_id=stack_name, status='DELETE_COMPLETE')

    def is_stack_complete(self, stack_name: str) -> bool:
        if self._stack_complete(stack_name):
            return True
        return False

    def _validate_spark_worker_scale(self, stack_name: str) -> None:
        if stack_name not in self.scaled_spark_workers_stacks:
            msg = f'{stack_name} not in scaled list, please check.'
            logger.error(msg)
            raise Exception(msg)

    def _validate_spark_worker_of_target_cluster_scale(self, stack_name: str, cluster_num: int) -> None:
        if stack_name not in self.scaled_target_spark_workers_stacks(cluster_num):
            msg = f'{stack_name} not in scaled list of target cluster {cluster_num}, please check.'
            logger.error(msg)
            raise Exception(msg)

    def _validate_kylin_scale(self, stack_name: str) -> None:
        if stack_name not in self.scaled_kylin_stacks:
            msg = f'{stack_name} not in scaled list, please check.'
            logger.error(msg)
            raise Exception(msg)

    def _validate_kylin_of_target_cluster_scale(self, stack_name: str, cluster_num: int) -> None:
        if stack_name not in self.scaled_target_kylin_stacks(cluster_num):
            msg = f'{stack_name} not in scaled list of target cluster {cluster_num}, please check.'
            logger.error(msg)
            raise Exception(msg)

    def _stack_status_check(self, name_or_id: str, status: str) -> bool:
        try:
            resp: Dict = self.cf_client.describe_stacks(StackName=name_or_id)
        except ClientError:
            return False
        return resp['Stacks'][0]['StackStatus'] == status

    def _stack_complete(self, stack_name: str) -> bool:
        try:
            self.create_complete_waiter.wait(
                StackName=stack_name,
                WaiterConfig={
                    'Delay': 30,
                    'MaxAttempts': 120
                }
            )
        except WaiterError as wx:
            # logger.error(wx)
            return False
        return True

    def _stack_exists(self, stack_name: str) -> bool:
        try:
            self.exists_waiter.wait(
                StackName=stack_name,
                WaiterConfig={
                    'Delay': 5,
                    'MaxAttempts': 2
                }
            )
        except WaiterError:
            return False
        return True

    def _stack_delete_complete(self, stack_name: str) -> bool:
        try:
            self.delete_complete_waiter.wait(
                StackName=stack_name,
                WaiterConfig={
                    'Delay': 60,
                    'MaxAttempts': 120
                }
            )
        except WaiterError as wx:
            # logger.error(wx)
            return False
        return True
    # ============ Utils Services End ============
