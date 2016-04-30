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
 *
 */

package org.apache.kylin.tool;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.storage.hbase.HBaseConnection;

public class ToolUtil {
    public static String getConfFolder() {
        final String CONF = "conf";
        String path = System.getProperty(KylinConfig.KYLIN_CONF);
        if (StringUtils.isNotEmpty(path)) {
            return path;
        }
        path = KylinConfig.getKylinHome();
        if (StringUtils.isNotEmpty(path)) {
            return path + File.separator + CONF;
        }
        return null;
    }

    public static String getHBaseMetaStoreId() throws IOException {
        try (final HBaseAdmin hbaseAdmin = new HBaseAdmin(HBaseConfiguration.create(HadoopUtil.getCurrentConfiguration()))) {
            final String metaStoreName = KylinConfig.getInstanceFromEnv().getMetadataUrlPrefix();
            final HTableDescriptor desc = hbaseAdmin.getTableDescriptor(TableName.valueOf(metaStoreName));
            return "MetaStore UUID: " + desc.getValue(HBaseConnection.HTABLE_UUID_TAG);
        }
    }
}
