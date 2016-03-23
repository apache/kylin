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

package org.apache.kylin.storage.hbase.steps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Drop HBase tables that is no longer needed
 */
public class MergeGCStep extends AbstractExecutable {

    private static final String OLD_HTABLES = "oldHTables";

    private static final Logger logger = LoggerFactory.getLogger(MergeGCStep.class);

    public MergeGCStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {

        try {
            logger.info("Sleep one minute before deleting the Htables");
            Thread.sleep(60000);
        } catch (InterruptedException e) {
            logger.warn("Thread interrupted");
        }

        logger.info("Start doing merge gc work");

        StringBuffer output = new StringBuffer();
        List<String> oldTables = getOldHTables();
        if (oldTables != null && oldTables.size() > 0) {
            String metadataUrlPrefix = KylinConfig.getInstanceFromEnv().getMetadataUrlPrefix();
            Admin admin = null;
            try {
                Connection conn = HBaseConnection.get(KylinConfig.getInstanceFromEnv().getStorageUrl());
                admin = conn.getAdmin();

                for (String table : oldTables) {
                    if (admin.tableExists(TableName.valueOf(table))) {
                        HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf((table)));
                        String host = tableDescriptor.getValue(IRealizationConstants.HTableTag);
                        if (metadataUrlPrefix.equalsIgnoreCase(host)) {
                            if (admin.isTableEnabled(TableName.valueOf(table))) {
                                admin.disableTable(TableName.valueOf(table));
                            }
                            admin.deleteTable(TableName.valueOf(table));
                            logger.debug("Dropped htable: " + table);
                            output.append("HBase table " + table + " is dropped. \n");
                        } else {
                            logger.debug("Skip htable: " + table);
                            output.append("Skip htable: " + table + ". \n");
                        }
                    }
                }

            } catch (IOException e) {
                output.append("Got error when drop HBase table, exiting... \n");
                // This should not block the merge job; Orphans should be cleaned up in StorageCleanupJob
                return new ExecuteResult(ExecuteResult.State.ERROR, output.append(e.getLocalizedMessage()).toString());
            } finally {
                if (admin != null)
                    try {
                        admin.close();
                    } catch (IOException e) {
                        logger.error(e.getLocalizedMessage());
                    }
            }
        }

        return new ExecuteResult(ExecuteResult.State.SUCCEED, output.toString());
    }

    public void setOldHTables(List<String> ids) {
        setParam(OLD_HTABLES, StringUtils.join(ids, ","));
    }

    private List<String> getOldHTables() {
        final String ids = getParam(OLD_HTABLES);
        if (ids != null) {
            final String[] splitted = StringUtils.split(ids, ",");
            ArrayList<String> result = Lists.newArrayListWithExpectedSize(splitted.length);
            for (String id : splitted) {
                result.add(id);
            }
            return result;
        } else {
            return Collections.emptyList();
        }
    }

}
