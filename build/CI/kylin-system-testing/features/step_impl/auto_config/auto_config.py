#!/usr/bin/python
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

from getgauge.python import step
import os
import json
import pytest
from kylin_utils import util
from kylin_utils import shell
import re
import time
from getgauge.python import Messages

global client
client = util.setup_instance('kylin_instance.yml')


@step("use default configuration on cube <cube_name>")
def default_config(cube_name):
    sh_config(set_auto="True", executor_instance="5", instance_strategy="100,2,500,3,1000,4")

    time.sleep(60)

    resp = client.build_segment(start_time="1325376000000",
                                end_time="1388534400000",
                                cube_name=cube_name)
    job_id = resp['uuid']
    step_id = job_id + "-01"
    client.await_job(job_id)
    resp = client.get_step_output(job_id=job_id, step_id=step_id)
    output = resp.get('cmd_output')

    check_log(output=output, memorySize="4GB", coresSize="5", memoryOverheadSize="1GB", instancesSize="5", partitionsSize="2")

    client.disable_cube(cube_name)

    time.sleep(30)
    client.purge_cube(cube_name)

    time.sleep(30)


@step("auto configuration with user-defined parameters on cube <cube_name>")
def user_def_config(cube_name):
    sh_config(set_auto="True", executor_instance="1", instance_strategy="100,1,500,2,1000,3")

    time.sleep(60)

    resp = client.build_segment(start_time="1325376000000",
                                end_time="1388534400000",
                                cube_name=cube_name)

    job_id = resp['uuid']
    step_id = job_id + "-01"
    client.await_job(job_id)
    resp = client.get_step_output(job_id=job_id, step_id=step_id)
    output = resp.get('cmd_output')

    check_log(output=output, memorySize="4GB", coresSize="5", memoryOverheadSize="1GB", instancesSize="1", partitionsSize="2")

    client.disable_cube(cube_name)

    time.sleep(30)
    client.purge_cube(cube_name)

    time.sleep(30)


@step("auto configuration with user-defined parameters on <cube_name> and <cube_no_distinct>")
def user_defined_no_dist(cube_name, cube_no_distinct):
    sh_config(set_auto="True", executor_instance="2", instance_strategy="100,4,500,6,1000,10")

    time.sleep(60)

    resp = client.build_segment(start_time="1325376000000",
                                end_time="1388534400000",
                                cube_name=cube_name)

    job_id = resp['uuid']
    step_id = job_id + "-01"
    client.await_job(job_id)
    resp = client.get_step_output(job_id=job_id, step_id=step_id)
    output = resp.get('cmd_output')

    check_log(output=output, memorySize="4GB", coresSize="5", memoryOverheadSize="1GB", instancesSize="2", partitionsSize="2")

    client.disable_cube(cube_name)

    time.sleep(30)
    client.purge_cube(cube_name)

    time.sleep(30)

    with open(os.path.join('meta_data/auto_config', 'auto_config_no_distinct.json'), 'r') as f:
        cube_desc_data = json.load(f)['cube_desc_data']

    client.create_cube('learn_kylin', cube_no_distinct, cube_desc_data=cube_desc_data)

    resp = client.build_segment(start_time="1325376000000",
                                end_time="1388534400000",
                                cube_name=cube_no_distinct)

    job_id = resp['uuid']
    step_id = job_id + "-01"
    client.await_job(job_id)
    resp = client.get_step_output(job_id=job_id, step_id=step_id)
    output = resp.get('cmd_output')

    check_log(output=output, memorySize="1GB", coresSize="1", memoryOverheadSize="512MB", instancesSize="2", partitionsSize="2")

    client.disable_cube(cube_no_distinct)

    time.sleep(30)
    client.delete_cube(cube_no_distinct)

    time.sleep(30)


@step("auto_config and override on cube level on cube <cube_override_conf>")
def override_on_cube(cube_override_conf):
    sh_config(set_auto="True", executor_instance="5", instance_strategy="100,2,500,3,1000,4")

    time.sleep(60)

    with open(os.path.join('meta_data/auto_config', 'auto_config_override_conf.json'), 'r') as f:
        cube_desc_data = json.load(f)['cube_desc_data']

    client.create_cube('learn_kylin', cube_override_conf, cube_desc_data=cube_desc_data)

    resp = client.build_segment(start_time="1325376000000",
                                end_time="1388534400000",
                                cube_name=cube_override_conf)

    job_id = resp['uuid']
    step_id = job_id + "-01"
    client.await_job(job_id)
    resp = client.get_step_output(job_id=job_id, step_id=step_id)
    output = resp.get('cmd_output')

    memory = re.findall("Override user-defined spark conf, set spark.executor.memory.*", output)
    cores = re.findall("Override user-defined spark conf, set spark.executor.cores.*", output)
    memoryOverhead = re.findall("Override user-defined spark conf, set spark.executor.memoryOverhead.*", output)
    instances = re.findall("Override user-defined spark conf, set spark.executor.instances.*", output)
    partitions = re.findall("Override user-defined spark conf, set spark.sql.shuffle.partitions.*", output)
    assert memory[0] == "Override user-defined spark conf, set spark.executor.memory=2G.", \
        Messages.write_message("expected Override user-defined spark conf, set spark.executor.memory=2G; actually " + memory[0])
    assert cores[0] == "Override user-defined spark conf, set spark.executor.cores=2.", \
        Messages.write_message("expected Override user-defined spark conf, set spark.executor.cores=2; actually " + cores[0])
    assert memoryOverhead[0] == "Override user-defined spark conf, set spark.executor.memoryOverhead=256M.", \
        Messages.write_message("expected Override user-defined spark conf, set spark.executor.memoryOverhead=256M; actually " + memoryOverhead[0])
    assert instances[0] == "Override user-defined spark conf, set spark.executor.instances=3.",\
        Messages.write_message("expected Override user-defined spark conf, set spark.executor.instances=3; actually" + instances[0])
    assert partitions[0] == "Override user-defined spark conf, set spark.sql.shuffle.partitions=3.", \
        Messages.write_message("expected Override user-defined spark conf, set spark.sql.shuffle.partitions=3; actually " + partitions[0])

    client.disable_cube(cube_override_conf)

    time.sleep(30)
    client.delete_cube(cube_override_conf)

    time.sleep(30)


def sh_config(set_auto, executor_instance, instance_strategy):
    sh = util.ssh_shell()

    sh_command = "cd $KYLIN_HOME/conf " \
                 "&& sed -i -r '/kylin.spark-conf.auto.prior/d' kylin.properties" \
                 "&& sed -i -r '/kylin.engine.base-executor-instance/d' kylin.properties" \
                 "&& sed -i -r '/kylin.engine.executor-instance-strategy/d' kylin.properties" \
                 "&& sed -i '$akylin.spark-conf.auto.prior={set_auto}' kylin.properties" \
                 "&& sed -i '$akylin.engine.base-executor-instance={executor_instance}' kylin.properties" \
                 "&& sed -i '$akylin.engine.executor-instance-strategy={instance_strategy}' kylin.properties".format(
        set_auto=set_auto, executor_instance=executor_instance, instance_strategy=instance_strategy)

    resp = sh.command(sh_command)

    resp = sh.command("cd $KYLIN_HOME/bin"
                      "&& sh kylin.sh stop"
                      "&& sh kylin.sh start")


def check_log(output, memorySize, coresSize, memoryOverheadSize, instancesSize, partitionsSize):
    memory = re.findall("Auto set spark conf: spark.executor.memory = .*", output)
    cores = re.findall("Auto set spark conf: spark.executor.cores = .*", output)
    memoryOverhead = re.findall("Auto set spark conf: spark.executor.memoryOverhead = .*", output)
    instances = re.findall("Auto set spark conf: spark.executor.instances = .*", output)
    partitions = re.findall("Auto set spark conf: spark.sql.shuffle.partitions = .*", output)
    assert memory[0] == "Auto set spark conf: spark.executor.memory = {memory}.".format(memory = memorySize), \
        Messages.write_message("expected "+"Auto set spark conf: spark.executor.memory = {memory}.".format(memory = memorySize)+ ", actually " +memory[0])
    assert cores[0] == "Auto set spark conf: spark.executor.cores = {cores}.".format(cores = coresSize), \
        Messages.write_message("expected "+"Auto set spark conf: spark.executor.cores = {cores}.".format(cores = coresSize)+", actually " +cores[0])
    assert memoryOverhead[0] == "Auto set spark conf: spark.executor.memoryOverhead = {memoryOverhead}.".format(memoryOverhead = memoryOverheadSize), \
        Messages.write_message("expected "+"Auto set spark conf: spark.executor.memoryOverhead = {memoryOverhead}.".format(memoryOverhead = memoryOverheadSize)+", actually " +memoryOverhead[0])
    assert instances[0] == "Auto set spark conf: spark.executor.instances = {instances}.".format(instances = instancesSize), \
        Messages.write_message("expected "+"Auto set spark conf: spark.executor.instances = {instances}.".format(instances = instancesSize)+", actually " +instances[0])
    assert partitions[0] == "Auto set spark conf: spark.sql.shuffle.partitions = {partitions}.".format(partitions = partitionsSize), \
        Messages.write_message("expected "+"Auto set spark conf: spark.sql.shuffle.partitions = {partitions}.".format(partitions = partitionsSize)+", actually " +partitions[0])

