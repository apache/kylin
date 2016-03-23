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
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
@SuppressWarnings("static-access")
public class HtableAlterMetadataCLI extends AbstractApplication {

    private static final Option OPTION_METADATA_KEY = OptionBuilder.withArgName("key").hasArg().isRequired(true).withDescription("The metadata key").create("key");
    private static final Option OPTION_METADATA_VALUE = OptionBuilder.withArgName("value").hasArg().isRequired(true).withDescription("The metadata value").create("value");
    protected static final Option OPTION_HTABLE_NAME = OptionBuilder.withArgName(BatchConstants.ARG_HTABLE_NAME).hasArg().isRequired(true).withDescription("HTable name").create(BatchConstants.ARG_HTABLE_NAME);

    protected static final Logger logger = LoggerFactory.getLogger(HtableAlterMetadataCLI.class);

    String tableName;
    String metadataKey;
    String metadataValue;

    private void alter() throws IOException {
        Connection conn = HBaseConnection.get(KylinConfig.getInstanceFromEnv().getStorageUrl());
        Admin hbaseAdmin = conn.getAdmin();
        HTableDescriptor table = hbaseAdmin.getTableDescriptor(TableName.valueOf(tableName));

        hbaseAdmin.disableTable(table.getTableName());
        table.setValue(metadataKey, metadataValue);
        hbaseAdmin.modifyTable(table.getTableName(), table);
        hbaseAdmin.enableTable(table.getTableName());
        hbaseAdmin.close();
    }

    public static void main(String[] args) throws Exception {
        HtableAlterMetadataCLI cli = new HtableAlterMetadataCLI();
        cli.execute(args);
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_HTABLE_NAME);
        options.addOption(OPTION_METADATA_KEY);
        options.addOption(OPTION_METADATA_VALUE);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        tableName = optionsHelper.getOptionValue(OPTION_HTABLE_NAME);
        metadataKey = optionsHelper.getOptionValue(OPTION_METADATA_KEY);
        metadataValue = optionsHelper.getOptionValue(OPTION_METADATA_VALUE);

        alter();

    }
}
