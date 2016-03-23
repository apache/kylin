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
import java.util.Arrays;
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
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class OrphanHBaseCleanJob extends AbstractHadoopJob {

    @SuppressWarnings("static-access")
    private static final Option OPTION_DELETE = OptionBuilder.withArgName("delete").hasArg().isRequired(false).withDescription("Delete the unused storage").create("delete");
    @SuppressWarnings("static-access")
    private static final Option OPTION_WHITELIST = OptionBuilder.withArgName("whitelist").hasArg().isRequired(true).withDescription("metadata store whitelist, separated with comma").create("whitelist");

    protected static final Logger logger = LoggerFactory.getLogger(OrphanHBaseCleanJob.class);

    boolean delete = false;
    Set<String> metastoreWhitelistSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        logger.info("jobs args: " + Arrays.toString(args));
        try {
            options.addOption(OPTION_DELETE);
            options.addOption(OPTION_WHITELIST);
            parseOptions(options, args);

            logger.info("options: '" + getOptionsAsString() + "'");
            logger.info("delete option value: '" + getOptionValue(OPTION_DELETE) + "'");
            delete = Boolean.parseBoolean(getOptionValue(OPTION_DELETE));
            String[] metastoreWhitelist = getOptionValue(OPTION_WHITELIST).split(",");

            for (String ms : metastoreWhitelist) {
                logger.info("metadata store in white list: " + ms);
                metastoreWhitelistSet.add(ms);
            }

            Configuration conf = HBaseConfiguration.create(getConf());

            cleanUnusedHBaseTables(conf);

            return 0;
        } catch (Exception e) {
            e.printStackTrace(System.err);
            throw e;
        }
    }

    private void cleanUnusedHBaseTables(Configuration conf) throws IOException {

        // get all kylin hbase tables
        Connection conn = HBaseConnection.get(KylinConfig.getInstanceFromEnv().getStorageUrl());
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
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new OrphanHBaseCleanJob(), args);
        System.exit(exitCode);
    }
}
