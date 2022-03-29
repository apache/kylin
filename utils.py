#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import os
import shutil
from typing import List, Tuple, Generator, Dict

import requests
from jinja2 import Environment, FileSystemLoader, TemplateNotFound

from constant.path import (
    TARS_PATH,
    JARS_PATH,
    TEMPLATES_PATH,
    KYLIN_PROPERTIES_TEMPLATE_DIR,
    KYLIN_PROPERTIES_DIR,
    RENDERED_FILE,
    PROPERTIES_TEMPLATE_DIR,
    TEMPLATE_OF_KYLIN_PROPERTIES,
    KYLIN_PROPERTIES_TEMPLATES_DIR,
    DEMOS_PATH,
)
from constant.server_mode import ServerMode

logger = logging.getLogger(__name__)


class Utils:
    DOWNLOAD_BASE_URL = 'https://s3.cn-north-1.amazonaws.com.cn/public.kyligence.io/kylin'

    FILES_SIZE_IN_BYTES = {
        'jdk-8u301-linux-x64.tar.gz': 145520298,
        'apache-kylin-4.0.0-bin-spark3.tar.gz': 198037626,
        'apache-kylin-4.0.2-bin-spark3.tar.gz': 198061387,
        'apache-hive-2.3.9-bin.tar.gz': 286170958,
        'hadoop-3.2.0.tar.gz': 345625475,
        'node_exporter-1.3.1.linux-amd64.tar.gz': 9033415,
        'prometheus-2.31.1.linux-amd64.tar.gz': 73079452,
        'spark-3.1.1-bin-hadoop3.2.tgz': 228721937,
        'zookeeper-3.4.13.tar.gz': 37191810,
        'commons-configuration-1.3.jar': 232915,
        'mysql-connector-java-5.1.40.jar': 990924,
        'mysql-connector-java-8.0.24.jar': 2428323,
        'mdx-kylin-4.0.2-beta.tar.gz': 81935515,
        'spark-3.1.1-bin-hadoop3.2-aws.tgz': 229859671,
    }

    @staticmethod
    def generate_nodes(scale_nodes: Tuple) -> List:
        if not scale_nodes:
            return []
        _from, _to = scale_nodes
        if _from == _to:
            return [_from]
        return list(range(_from, _to + 1))

    @staticmethod
    def read_template(file_path: str):
        with open(file=file_path, mode='r') as template:
            res_template = template.read()
        return res_template

    @staticmethod
    def full_path_of_yaml(yaml_name: str) -> str:
        return os.path.join(TEMPLATES_PATH, yaml_name)

    @staticmethod
    def download_tar(filename: str) -> None:
        base_url = Utils.DOWNLOAD_BASE_URL + '/tar/'
        url = base_url + filename
        Utils.download(url=url, dest_folder=TARS_PATH, filename=filename)
        Utils.is_downloaded_success(filename=filename, dest_folder=TARS_PATH)

    @staticmethod
    def download_jar(filename: str) -> None:
        base_url = Utils.DOWNLOAD_BASE_URL + '/jars/'
        url = base_url + filename
        Utils.download(url=url, dest_folder=JARS_PATH, filename=filename)
        Utils.is_downloaded_success(filename=filename, dest_folder=JARS_PATH)

    @staticmethod
    def download_demo(filename: str) -> str:
        base_url = Utils.DOWNLOAD_BASE_URL + '/kylin_demo/'
        url = base_url + filename
        Utils.download(url=url, dest_folder=DEMOS_PATH, filename=filename)
        Utils.is_downloaded_success(filename=filename, dest_folder=DEMOS_PATH)

    @staticmethod
    def download(url: str, dest_folder: str, filename: str) -> None:
        if not Utils.is_file_exists(dest_folder):
            # create folder if it does not exist
            os.makedirs(dest_folder)

        file_path = os.path.join(dest_folder, filename)
        if Utils.is_file_exists(file_path):
            logger.info(f'{filename} already exists, skip download it.')
            return
        r = requests.get(url, stream=True)
        if not r.ok:
            # HTTP status code 4XX/5XX
            logger.error("Download failed: status code {}\n{}".format(r.status_code, r.text))
            return
        logger.info(f"Downloading {os.path.abspath(file_path)}.")
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 8):
                if not chunk:
                    break
                f.write(chunk)
                f.flush()
                os.fsync(f.fileno())

    @staticmethod
    def files_in_tar() -> int:
        if not Utils.is_file_exists(TARS_PATH):
            logger.error(f'{TARS_PATH} does exists, please check.')
            return 0
        return sum(1 for _ in Utils.listdir_nohidden(TARS_PATH))

    @staticmethod
    def files_in_jars() -> int:
        if not Utils.is_file_exists(JARS_PATH):
            logger.error(f'{JARS_PATH} does exists, please check.')
            return 0
        return sum(1 for _ in Utils.listdir_nohidden(JARS_PATH))

    @staticmethod
    def list_dir(dest_folder: str) -> List:
        return os.listdir(dest_folder)

    @staticmethod
    def listdir_nohidden(dest_folder: str) -> Generator:
        for f in os.listdir(dest_folder):
            if not f.startswith('.'):
                yield f

    @staticmethod
    def render_properties(params: Dict,
                          cluster_num: int = None,
                          properties_file: str = 'kylin.properties',
                          kylin_mode: str = 'all') -> None:
        target_path = KYLIN_PROPERTIES_TEMPLATE_DIR.format(cluster_num=cluster_num if cluster_num else 'default')

        # replace a matched kylin.properties for a kylin in cluster
        Utils.replace_property_for_kylin(target_path, kylin_mode)

        dest_path = os.path.join(target_path, 'kylin.properties')
        rendered_file = os.path.join(target_path, RENDERED_FILE)
        if Utils.is_file_exists(rendered_file):
            logger.info(f'{dest_path} already rendered. Skip render it again.')
            return

        env = Environment(loader=FileSystemLoader(searchpath=target_path))
        try:
            template = env.get_template(properties_file)
        except TemplateNotFound:
            raise Exception(f'Properties template: {properties_file} not in the path: {target_path}.\n '
                            f'Please copy the needed kylin.properties template in `backup/properties/templates` '
                            f'to `backup/properties/{cluster_num}`\n. If `backup/properties/{cluster_num}` not exists, '
                            f'please make it and rename the template file to `kylin.properties` in this dir.')
        output_from_parsed_template = template.render(params)

        with open(dest_path, 'w') as f:
            f.write(output_from_parsed_template)
        # touch a file with current time
        with open(rendered_file, 'a'):
            os.utime(rendered_file, None)
        logger.info(f'Current {dest_path} rendered.')

    @staticmethod
    def replace_property_for_kylin(target_path: str, kylin_mode: str = 'all', properties_file: str = 'kylin.properties'):
        # default kylin.properties is for 'all' mode to a kylin node
        if kylin_mode == ServerMode.ALL.value:
            return

        source_file = os.path.join(KYLIN_PROPERTIES_TEMPLATES_DIR, kylin_mode, properties_file)
        destination_file = os.path.join(target_path, properties_file)
        shutil.copy(source_file, destination_file)

    @staticmethod
    def refresh_kylin_properties(properties_file: str = 'kylin.properties') -> None:
        Utils.refresh_kylin_properties_in_clusters()
        Utils.refresh_kylin_properties_in_default(properties_template=properties_file)

    @staticmethod
    def refresh_kylin_properties_in_clusters(cluster_nums: List[int] = None) -> None:
        # delete useless kylin.properties
        kylin_properties_paths = os.listdir(KYLIN_PROPERTIES_DIR)
        for path in kylin_properties_paths:
            if path in ['default', 'templates']:
                continue

            if not cluster_nums and (cluster_nums and path not in cluster_nums):
                continue

            absolute_path = os.path.join(KYLIN_PROPERTIES_DIR, path)
            if not path.isdigit():
                logger.warning(f'Illegal path of {absolute_path}, please check.')
                continue
            logger.info(f'Start to delete useless path: {absolute_path}.')
            shutil.rmtree(absolute_path, ignore_errors=True)
            logger.info(f'Delete useless path: {absolute_path} done.')

    @staticmethod
    def refresh_kylin_properties_in_default(properties_template: str = 'kylin.properties') -> None:
        # refresh default kylin.properties
        default_path = KYLIN_PROPERTIES_TEMPLATE_DIR.format(cluster_num='default')
        mark_file_path = os.path.join(default_path, RENDERED_FILE)
        if Utils.is_file_exists(mark_file_path):
            logger.info(f'Removing the render file.')
            os.remove(mark_file_path)
            logger.info(f'Removed the render file.')

        kylin_properties = os.path.join(default_path, properties_template)
        if Utils.is_file_exists(kylin_properties):
            logger.info(f'Removing the render file.')
            os.remove(kylin_properties)
            logger.info(f'Removed the render file.')

        # copy template & rename it to kylin.properties
        template = os.path.join(PROPERTIES_TEMPLATE_DIR, TEMPLATE_OF_KYLIN_PROPERTIES)
        logger.info(f'Copy template from {template} to {kylin_properties}.')
        shutil.copy(template, kylin_properties)
        logger.info(f'Copy done.')

    @staticmethod
    def is_file_exists(file_with_full_path: str) -> bool:
        return os.path.exists(file_with_full_path)

    @staticmethod
    def is_downloaded_success(filename: str, dest_folder: str) -> None:
        if filename not in Utils.FILES_SIZE_IN_BYTES.keys():
            logger.warning(f'Current file {filename} is not the matched version, skip check file size.')
            return
        expected_size = Utils.FILES_SIZE_IN_BYTES[filename]
        real_size = Utils.size_in_bytes(dest_folder=dest_folder, filename=filename)

        assert expected_size == real_size, \
            f'{filename} size should be {expected_size} bytes not {real_size} bytes, please check.'
        logger.info(f'Downloaded file {filename} successfully.')

    @staticmethod
    def size_in_bytes(filename: str, dest_folder: str) -> int:
        # return file size in bytes
        return os.path.getsize(os.path.join(dest_folder,filename))

    @staticmethod
    def is_uploaded_success(filename: str, size_in_bytes: int) -> None:
        if filename.endswith('.sh'):
            # .sh scripts don't need to check file size, because it is in local
            return
        if filename not in Utils.FILES_SIZE_IN_BYTES.keys():
            logger.warning(f'Current uploading file: {filename} is not match version, skip check file size.')
            return
        assert Utils.FILES_SIZE_IN_BYTES[filename] == size_in_bytes, \
            f'Uploaded file {filename} size should be {Utils.FILES_SIZE_IN_BYTES[filename]} bytes, ' \
            f'not {size_in_bytes} bytes, please check.'
