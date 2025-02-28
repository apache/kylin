/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.engine.spark.job;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.engine.spark.application.SparkApplication;
import org.apache.kylin.engine.spark.builder.InternalTableLoader;
import org.apache.kylin.engine.spark.job.exec.InternalTableLoadExec;
import org.apache.kylin.engine.spark.job.stage.internal.InternalTableLoad;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.table.InternalTableDesc;
import org.apache.kylin.metadata.table.InternalTableManager;
import org.apache.kylin.metadata.table.InternalTablePartitionDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.val;

//
public class InternalTableLoadJob extends SparkApplication {
    protected static final Logger logger = LoggerFactory.getLogger(InternalTableLoadJob.class);

    public static void main(String[] args) {
        InternalTableLoadJob job = new InternalTableLoadJob();
        job.execute(args);
    }

    @Override
    protected void doExecute() throws Exception {
        val jobStepId = StringUtils.replace(infos.getJobStepId(), JOB_NAME_PREFIX, "");
        val exec = new InternalTableLoadExec(jobStepId);
        exec.addStage(new InternalTableLoad(this));
        exec.executeStep();
    }

    public void innerExecute() throws IOException {
        boolean dropPartition = Boolean.parseBoolean(getParam(NBatchConstants.P_DELETE_PARTITION));
        if (dropPartition) {
            logger.info("Start to drop partitions");
            dropPartitons();
        } else {
            logger.info("Start to load data into table");
            loadIntoInternalTable();
        }
    }

    public void loadIntoInternalTable() throws IOException {
        String tableIdentity = getParam(NBatchConstants.P_TABLE_NAME);
        InternalTableDesc internalTable = InternalTableManager.getInstance(config, project)
                .getInternalTableDesc(tableIdentity);
        boolean incrementalBuild = "true".equals(getParam(NBatchConstants.P_INCREMENTAL_BUILD));
        String startDate = getParam(NBatchConstants.P_START_DATE);
        String endDate = getParam(NBatchConstants.P_END_DATE);
        String storagePolicy = config.getGlutenStoragePolicy();
        InternalTableLoader loader = new InternalTableLoader();
        loader.loadInternalTable(ss, internalTable, new String[] { startDate, endDate }, null, storagePolicy,
                incrementalBuild);
    }

    public void dropPartitons() throws IOException {
        InternalTableLoader loader = new InternalTableLoader();
        String tableName = getParam(NBatchConstants.P_TABLE_NAME);
        String toBeDelete = getParam(NBatchConstants.P_DELETE_PARTITION_VALUES);
        InternalTableDesc internalTable = InternalTableManager.getInstance(config, project)
                .getInternalTableDesc(tableName);
        loader.dropPartitions(ss, internalTable, toBeDelete);
    }

    @Getter
    @AllArgsConstructor
    public static class InternalTableMetaUpdateInfo {
        long finalCount;
        List<String> partitionValues;
        List<InternalTablePartitionDetail> partitionDetails;
    }
}
