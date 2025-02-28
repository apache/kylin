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
package org.apache.kylin.query.util;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

public class PartitionsFilter implements PathFilter, Configurable, Serializable {
    public static final String PARTITION_COL = "partition_col";
    public static final String PARTITIONS = "partitions";

    private Set<String> partitions = Sets.newHashSet();

    @Override
    public boolean accept(Path path) {
        if (partitions.contains(path.getParent().getName())) {
            return true;
        }
        return false;
    }

    private Set<String> toPartitions(String colName, String conf) {
        Set<String> partitionNames = new LinkedHashSet<>();
        for (String id : conf.split(",")) {
            partitionNames.add(colName + "=" + id);
        }
        return partitionNames;
    }

    @Override
    public Configuration getConf() {
        return null;
    }

    @Override
    public void setConf(Configuration conf) {
        String colName = conf.get(PARTITION_COL);
        partitions = toPartitions(colName, conf.get(PARTITIONS));
    }

}
