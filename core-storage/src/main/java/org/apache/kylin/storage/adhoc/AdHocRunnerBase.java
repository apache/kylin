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

package org.apache.kylin.storage.adhoc;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class AdHocRunnerBase {

    private static final Logger logger = LoggerFactory.getLogger(AdHocRunnerBase.class);

    protected KylinConfig config = null;

    public AdHocRunnerBase() {
    }

    public AdHocRunnerBase(KylinConfig config) {
        this.config = config;
    }

    public void setConfig(KylinConfig config) {
        this.config = config;
    }

    public abstract void init();

    public abstract void executeQuery(String query, List<List<String>> results, List<SelectedColumnMeta> columnMetas) throws Exception;
}