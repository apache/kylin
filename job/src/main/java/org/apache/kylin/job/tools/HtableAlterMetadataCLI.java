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

package org.apache.kylin.job.tools;

import java.io.IOException;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.persistence.HBaseConnection;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by honma on 11/11/14.
 */
@SuppressWarnings("static-access")
public class HtableAlterMetadataCLI extends AbstractHadoopJob {

    private static final Option OPTION_METADATA_KEY = OptionBuilder.withArgName("key").hasArg().isRequired(true).withDescription("The metadata key").create("key");
    private static final Option OPTION_METADATA_VALUE = OptionBuilder.withArgName("value").hasArg().isRequired(true).withDescription("The metadata value").create("value");

    protected static final Logger log = LoggerFactory.getLogger(HtableAlterMetadataCLI.class);

    String tableName;
    String metadataKey;
    String metadataValue;

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        try {
            options.addOption(OPTION_HTABLE_NAME);
            options.addOption(OPTION_METADATA_KEY);
            options.addOption(OPTION_METADATA_VALUE);

            parseOptions(options, args);
            tableName = getOptionValue(OPTION_HTABLE_NAME);
            metadataKey = getOptionValue(OPTION_METADATA_KEY);
            metadataValue = getOptionValue(OPTION_METADATA_VALUE);

            alter();

            return 0;
        } catch (Exception e) {
            printUsage(options);
            throw e;
        }
    }

    private void alter() throws IOException {
        Admin hbaseAdmin = HBaseConnection.get().getAdmin();
        HTableDescriptor table = hbaseAdmin.getTableDescriptor(TableName.valueOf(tableName));

        hbaseAdmin.disableTable(table.getTableName());
        table.setValue(metadataKey, metadataValue);
        hbaseAdmin.modifyTable(table.getTableName(), table);
        hbaseAdmin.enableTable(table.getTableName());
        hbaseAdmin.close();
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HtableAlterMetadataCLI(), args);
        System.exit(exitCode);
    }
}
