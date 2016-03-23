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

/**
 */
public class CleanHtableCLI extends AbstractApplication {

    protected static final Logger logger = LoggerFactory.getLogger(CleanHtableCLI.class);

    private void clean() throws IOException {
        Connection conn = HBaseConnection.get(KylinConfig.getInstanceFromEnv().getStorageUrl());
        Admin hbaseAdmin = conn.getAdmin();

        for (HTableDescriptor descriptor : hbaseAdmin.listTables()) {
            String name = descriptor.getNameAsString().toLowerCase();
            if (name.startsWith("kylin") || name.startsWith("_kylin")) {
                String x = descriptor.getValue(IRealizationConstants.HTableTag);
                System.out.println("table name " + descriptor.getNameAsString() + " host: " + x);
                System.out.println(descriptor);
                System.out.println();

                descriptor.setValue(IRealizationConstants.HTableOwner, "DL-eBay-Kylin@ebay.com");
                hbaseAdmin.modifyTable(TableName.valueOf(descriptor.getNameAsString()), descriptor);
            }
        }
        hbaseAdmin.close();
    }

    public static void main(String[] args) throws Exception {
        CleanHtableCLI cli = new CleanHtableCLI();
        cli.execute(args);
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        clean();
    }
}
