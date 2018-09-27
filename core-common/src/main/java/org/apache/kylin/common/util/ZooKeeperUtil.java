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

package org.apache.kylin.common.util;

import java.util.Arrays;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

/**
 * Use reflection to get zookeeper connect string from HBase configuration.
 */
public class ZooKeeperUtil {
    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperUtil.class);

    private ZooKeeperUtil() {
        throw new IllegalStateException("Class ZooKeeperUtil is an utility class !");
    }

    public static String getZKConnectStringFromHBase() {
        Configuration hconf = null;
        try {
            Class<? extends Object> hbaseConnClz = ClassUtil.forName("org.apache.kylin.storage.hbase.HBaseConnection", Object.class);
            hconf = (Configuration) hbaseConnClz.getMethod("getCurrentHBaseConfiguration").invoke(null);
        } catch (Throwable ex) {
            logger.warn("Failed to get zookeeper connect string from HBase configuration", ex);
            return null;
        }
        
        final String serverList = hconf.get("hbase.zookeeper.quorum");
        final String port = hconf.get("hbase.zookeeper.property.clientPort");
        return StringUtils.join(Iterables.transform(Arrays.asList(serverList.split(",")), new Function<String, String>() {
            @Nullable
            @Override
            public String apply(String input) {
                return input + ":" + port;
            }
        }), ",");
    }
}
