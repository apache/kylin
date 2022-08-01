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
import io.kyligence.kap.secondstorage.metadata.TablePlan;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

import static io.kyligence.kap.clickhouse.ClickHouseConstants.RES_PATH_FMT;
import static io.kyligence.kap.clickhouse.ClickHouseConstants.STORAGE_NAME;
import static io.kyligence.kap.clickhouse.ClickHouseConstants.PLAN;

public class ClickHouseManager extends Manager<TablePlan> {

    private static final Logger logger = LoggerFactory.getLogger(ClickHouseManager.class);

    // called by reflection
    static ClickHouseManager newInstance(KylinConfig config, String project) {
        return new ClickHouseManager(config, project);
    }

    private ClickHouseManager(KylinConfig cfg, final String project) {
        super(cfg, project);
    }

    @Override
    public Logger logger() {
        return logger;
    }

    @Override
    public String name() {
        return "ClickHouseManager";
    }

    @Override
    public String rootPath() {
        return String.format(Locale.ROOT, RES_PATH_FMT, project, STORAGE_NAME, PLAN);
    }

    @Override
    public Class<TablePlan> entityType() {
        return TablePlan.class;
    }

    @Override
    protected TablePlan newRootEntity(String model) {
        final String description = "create by newRootEntity";
        return TablePlan.builder()
                .setModel(model)
                .setDescription(description)
                .build();
    }
}
