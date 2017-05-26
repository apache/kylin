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

import java.util.Arrays;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.kylin.storage.hbase.HBaseConnection;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

public class ZookeeperUtil {

    /**
     * Get zookeeper connection string from HBase Configuration
     *
     * @return Zookeeper Connection string
     */
    public static String getZKConnectString() {
        Configuration conf = HBaseConnection.getCurrentHBaseConfiguration();
        final String serverList = conf.get(HConstants.ZOOKEEPER_QUORUM);
        final String port = conf.get(HConstants.ZOOKEEPER_CLIENT_PORT);
        return StringUtils.join(Iterables.transform(Arrays.asList(serverList.split(",")), new Function<String, String>() {
            @Nullable
            @Override
            public String apply(String input) {
                return input + ":" + port;
            }
        }), ",");
    }
}
