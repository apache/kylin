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

package io.kyligence.kap.secondstorage;

import java.util.Map;
import java.util.Set;

public interface SecondStorageUpdater {
    /**
     * Clean second storage model
     * if needed, return clean job id
     * @param project project
     * @param modelId model id
     * @return clean job id
     */
    String cleanModel(final String project, final String modelId);

    /**
     * disable model
     * @param project project
     * @param modelId model id
     * @return clean job id
     */
    String disableModel(final String project, final String modelId);

    /**
     * Update model index
     * if needed, return clean index id
     * @param project project
     * @param modelId model id
     * @return clean index job id
     */
    String updateIndex(String project, String modelId);

    /**
     * Get query metric , like scan rows / scan bytes
     * @param project project
     * @param queryId query id
     * @return metrics
     */
    Map<String, Object> getQueryMetric(String project, String queryId);

    String removeIndexByLayoutId(String project, String modelId, Set<Long> layoutIds);
}
