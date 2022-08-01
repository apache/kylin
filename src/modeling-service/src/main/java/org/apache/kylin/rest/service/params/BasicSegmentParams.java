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
package org.apache.kylin.rest.service.params;

import java.util.List;
import java.util.Set;

import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.rest.aspect.TransactionProjectUnit;

import com.clearspring.analytics.util.Lists;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
public class BasicSegmentParams implements TransactionProjectUnit {
    protected String project;
    protected String modelId;
    protected Set<String> ignoredSnapshotTables;
    protected int priority = ExecutablePO.DEFAULT_PRIORITY;

    protected boolean partialBuild = false;
    protected List<Long> batchIndexIds = Lists.newArrayList();

    protected String yarnQueue;
    protected Object tag;

    BasicSegmentParams(String project, String modelId) {
        this.project = project;
        this.modelId = modelId;
    }

    @Override
    public String transactionProjectUnit() {
        return getProject();
    }
}
