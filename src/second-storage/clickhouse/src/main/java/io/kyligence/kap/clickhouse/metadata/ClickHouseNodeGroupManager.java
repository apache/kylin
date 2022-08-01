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
package io.kyligence.kap.clickhouse.metadata;

import io.kyligence.kap.secondstorage.metadata.Manager;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

import static io.kyligence.kap.clickhouse.ClickHouseConstants.NODE_GROUP;
import static io.kyligence.kap.clickhouse.ClickHouseConstants.RES_PATH_FMT;
import static io.kyligence.kap.clickhouse.ClickHouseConstants.STORAGE_NAME;

public class ClickHouseNodeGroupManager extends Manager<NodeGroup> {

    private static final Logger logger = LoggerFactory.getLogger(ClickHouseNodeGroupManager.class);

    private ClickHouseNodeGroupManager(KylinConfig cfg, final String project) {
        super(cfg, project);
    }

    // called by reflection
    static ClickHouseNodeGroupManager newInstance(KylinConfig config, String project) {
        return new ClickHouseNodeGroupManager(config, project);
    }

    @Override
    protected NodeGroup newRootEntity(String cubeName) {
        return NodeGroup.builder().build();
    }

    @Override
    public Logger logger() {
        return logger;
    }

    @Override
    public String name() {
        return "ClickHouseNodeGroupManager";
    }

    @Override
    public String rootPath() {
        return String.format(Locale.ROOT, RES_PATH_FMT, project, STORAGE_NAME, NODE_GROUP);
    }

    @Override
    public Class<NodeGroup> entityType() {
        return NodeGroup.class;
    }

}
