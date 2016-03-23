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

package org.apache.kylin.storage.hbase.util;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * clean hbase tables by tag
 */
public class HBaseClean extends AbstractApplication {

    @SuppressWarnings("static-access")
    private static final Option OPTION_DELETE = OptionBuilder.withArgName("delete").hasArg().isRequired(true).withDescription("actually delete or not").create("delete");

    @SuppressWarnings("static-access")
    private static final Option OPTION_TAG = OptionBuilder.withArgName("tag").hasArg().isRequired(true).withDescription("the tag of HTable").create("tag");

    protected static final Logger logger = LoggerFactory.getLogger(HBaseClean.class);

    boolean delete = false;
    String tag = null;

    private void cleanUp() {
        try {
            // get all kylin hbase tables
            Connection conn = HBaseConnection.get(KylinConfig.getInstanceFromEnv().getStorageUrl());
            Admin hbaseAdmin = conn.getAdmin();
            String tableNamePrefix = IRealizationConstants.SharedHbaseStorageLocationPrefix;
            HTableDescriptor[] tableDescriptors = hbaseAdmin.listTables(tableNamePrefix + ".*");
            List<String> allTablesNeedToBeDropped = Lists.newArrayList();
            for (HTableDescriptor desc : tableDescriptors) {
                String host = desc.getValue(IRealizationConstants.HTableTag);
                if (tag.equalsIgnoreCase(host)) {
                    allTablesNeedToBeDropped.add(desc.getTableName().getNameAsString());
                }
            }

            if (delete) {
                // drop tables
                for (String htableName : allTablesNeedToBeDropped) {
                    logger.info("Deleting HBase table " + htableName);
                    if (hbaseAdmin.tableExists(TableName.valueOf(htableName))) {
                        if (hbaseAdmin.isTableEnabled(TableName.valueOf(htableName))) {
                            hbaseAdmin.disableTable(TableName.valueOf(htableName));
                        }

                        hbaseAdmin.deleteTable(TableName.valueOf(htableName));
                        logger.info("Deleted HBase table " + htableName);
                    } else {
                        logger.info("HBase table" + htableName + " does not exist");
                    }
                }
            } else {
                System.out.println("--------------- Tables To Be Dropped ---------------");
                for (String htableName : allTablesNeedToBeDropped) {
                    System.out.println(htableName);
                }
                System.out.println("----------------------------------------------------");
            }

            hbaseAdmin.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        HBaseClean cli = new HBaseClean();
        cli.execute(args);
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_DELETE);
        options.addOption(OPTION_TAG);
        return options;

    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        tag = optionsHelper.getOptionValue(OPTION_TAG);
        delete = Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_DELETE));

        cleanUp();
    }
}
