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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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

/**
 */
public class OrphanHBaseCleanJob extends AbstractApplication {

    @SuppressWarnings("static-access")
    private static final Option OPTION_DELETE = OptionBuilder.withArgName("delete").hasArg().isRequired(false).withDescription("Delete the unused storage").create("delete");
    @SuppressWarnings("static-access")
    private static final Option OPTION_WHITELIST = OptionBuilder.withArgName("whitelist").hasArg().isRequired(true).withDescription("metadata store whitelist, separated with comma").create("whitelist");

    protected static final Logger logger = LoggerFactory.getLogger(OrphanHBaseCleanJob.class);

    boolean delete = false;
    Set<String> metastoreWhitelistSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    private void cleanUnusedHBaseTables(Configuration conf) throws IOException {
        Connection conn = HBaseConnection.get(KylinConfig.getInstanceFromEnv().getStorageUrl());
        // get all kylin hbase tables
        Admin hbaseAdmin = conn.getAdmin();
        String tableNamePrefix = IRealizationConstants.SharedHbaseStorageLocationPrefix;
        HTableDescriptor[] tableDescriptors = hbaseAdmin.listTables(tableNamePrefix + ".*");
        List<String> allTablesNeedToBeDropped = new ArrayList<String>();
        for (HTableDescriptor desc : tableDescriptors) {
            String host = desc.getValue(IRealizationConstants.HTableTag);
            if (!metastoreWhitelistSet.contains(host)) {
                logger.info("HTable {} is recognized as orphan because its tag is {}", desc.getTableName(), host);
                //collect orphans
                allTablesNeedToBeDropped.add(desc.getTableName().getNameAsString());
            } else {
                logger.info("HTable {} belongs to {}", desc.getTableName(), host);
            }
        }

        if (delete == true) {
            // drop tables
            for (String htableName : allTablesNeedToBeDropped) {
                logger.info("Deleting HBase table " + htableName);
                TableName tableName = TableName.valueOf(htableName);
                if (hbaseAdmin.tableExists(tableName)) {
                    if (hbaseAdmin.isTableEnabled(tableName)) {
                        hbaseAdmin.disableTable(tableName);
                    }

                    hbaseAdmin.deleteTable(tableName);
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
    }

    public static void main(String[] args) throws Exception {
        OrphanHBaseCleanJob job = new OrphanHBaseCleanJob();
        job.execute(args);
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_DELETE);
        options.addOption(OPTION_WHITELIST);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        logger.info("options: '" + optionsHelper.getOptionsAsString() + "'");
        logger.info("delete option value: '" + optionsHelper.getOptionValue(OPTION_DELETE) + "'");
        delete = Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_DELETE));
        String[] metastoreWhitelist = optionsHelper.getOptionValue(OPTION_WHITELIST).split(",");

        for (String ms : metastoreWhitelist) {
            logger.info("metadata store in white list: " + ms);
            metastoreWhitelistSet.add(ms);
        }

        Configuration conf = HBaseConfiguration.create();
        cleanUnusedHBaseTables(conf);
    }
}
