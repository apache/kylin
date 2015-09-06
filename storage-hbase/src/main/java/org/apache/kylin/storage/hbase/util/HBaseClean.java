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

import com.google.common.collect.Lists;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * clean hbase tables by tag
 */
public class HBaseClean extends AbstractHadoopJob {

    @SuppressWarnings("static-access")
    private static final Option OPTION_DELETE = OptionBuilder.withArgName("delete").hasArg().isRequired(true).withDescription("actually delete or not").create("delete");

    @SuppressWarnings("static-access")
    private static final Option OPTION_TAG = OptionBuilder.withArgName("tag").hasArg().isRequired(true).withDescription("the tag of HTable").create("tag");

    protected static final Logger log = LoggerFactory.getLogger(HBaseClean.class);
    boolean delete = false;
    String tag = null;

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        log.info("----- jobs args: " + Arrays.toString(args));
        try {
            options.addOption(OPTION_DELETE);
            options.addOption(OPTION_TAG);
            parseOptions(options, args);

            log.info("options: '" + getOptionsAsString() + "'");
            
            tag = getOptionValue(OPTION_TAG);
            delete = Boolean.parseBoolean(getOptionValue(OPTION_DELETE));

            cleanUp();

            return 0;
        } catch (Exception e) {
            e.printStackTrace(System.err);
            throw e;
        }
    }

    private void cleanUp() {
        try {
            // get all kylin hbase tables
            Configuration conf = HBaseConfiguration.create();
            HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
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
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HBaseClean(), args);
        System.exit(exitCode);
    }
}
