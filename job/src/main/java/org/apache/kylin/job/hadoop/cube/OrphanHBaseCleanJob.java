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

package org.apache.kylin.job.hadoop.cube;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 */
public class OrphanHBaseCleanJob extends AbstractHadoopJob {

    @SuppressWarnings("static-access")
    private static final Option OPTION_DELETE = OptionBuilder.withArgName("delete").hasArg().isRequired(false).withDescription("Delete the unused storage").create("delete");
    @SuppressWarnings("static-access")
    private static final Option OPTION_WHITELIST = OptionBuilder.withArgName("whitelist").hasArg().isRequired(true).withDescription("metadata store whitelist, separated with comma").create("whitelist");

    protected static final Logger log = LoggerFactory.getLogger(OrphanHBaseCleanJob.class);

    boolean delete = false;
    Set<String> metastoreWhitelistSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        log.info("----- jobs args: " + Arrays.toString(args));
        try {
            options.addOption(OPTION_DELETE);
            options.addOption(OPTION_WHITELIST);
            parseOptions(options, args);

            log.info("options: '" + getOptionsAsString() + "'");
            log.info("delete option value: '" + getOptionValue(OPTION_DELETE) + "'");
            delete = Boolean.parseBoolean(getOptionValue(OPTION_DELETE));
            String[] metastoreWhitelist = getOptionValue(OPTION_WHITELIST).split(",");

            for (String ms : metastoreWhitelist) {
                log.info("metadata store in white list: " + ms);
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
        HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
        String tableNamePrefix = IRealizationConstants.SharedHbaseStorageLocationPrefix;
        HTableDescriptor[] tableDescriptors = hbaseAdmin.listTables(tableNamePrefix + ".*");
        List<String> allTablesNeedToBeDropped = new ArrayList<String>();
        for (HTableDescriptor desc : tableDescriptors) {
            String host = desc.getValue(IRealizationConstants.HTableTag);
            if (!metastoreWhitelistSet.contains(host)) {
                log.info("HTable {} is recognized as orphan because its tag is {}", desc.getTableName(), host);
                //collect orphans
                allTablesNeedToBeDropped.add(desc.getTableName().getNameAsString());
            } else {
                log.info("HTable {} belongs to {}", desc.getTableName(), host);
            }
        }

        if (delete == true) {
            // drop tables
            for (String htableName : allTablesNeedToBeDropped) {
                log.info("Deleting HBase table " + htableName);
                if (hbaseAdmin.tableExists(htableName)) {
                    if (hbaseAdmin.isTableEnabled(htableName)) {
                        hbaseAdmin.disableTable(htableName);
                    }

                    hbaseAdmin.deleteTable(htableName);
                    log.info("Deleted HBase table " + htableName);
                } else {
                    log.info("HBase table" + htableName + " does not exist");
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
