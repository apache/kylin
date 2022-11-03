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
package io.kyligence.kap.metadata.favorite;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryHistoryTimeOffsetManager {
    private static final Logger logger = LoggerFactory.getLogger(QueryHistoryTimeOffsetManager.class);

    public static final Serializer<QueryHistoryTimeOffset> QUERY_HISTORY_TIME_OFFSET_SERIALIZER = new JsonSerializer<>(
            QueryHistoryTimeOffset.class);

    private final KylinConfig kylinConfig;
    private ResourceStore resourceStore;
    private String resourceRoot;

    public static QueryHistoryTimeOffsetManager getInstance(KylinConfig kylinConfig, String project) {
        return kylinConfig.getManager(project, QueryHistoryTimeOffsetManager.class);
    }

    // called by reflection
    static QueryHistoryTimeOffsetManager newInstance(KylinConfig config, String project) {
        return new QueryHistoryTimeOffsetManager(config, project);
    }

    private QueryHistoryTimeOffsetManager(KylinConfig kylinConfig, String project) {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing QueryHistoryTimeOffsetManager with KylinConfig Id: {} for project {}",
                    System.identityHashCode(kylinConfig), project);
        this.kylinConfig = kylinConfig;
        resourceStore = ResourceStore.getKylinMetaStore(this.kylinConfig);
        this.resourceRoot = "/" + project + ResourceStore.QUERY_HISTORY_TIME_OFFSET;
    }

    private String path(String uuid) {
        return this.resourceRoot + "/" + uuid + MetadataConstants.FILE_SURFIX;
    }

    public void save(QueryHistoryTimeOffset time) {
        resourceStore.checkAndPutResource(path(time.getUuid()), time, QUERY_HISTORY_TIME_OFFSET_SERIALIZER);
    }

    public QueryHistoryTimeOffset get() {
        List<QueryHistoryTimeOffset> queryHistoryTimeOffsetList = resourceStore.getAllResources(resourceRoot,
                QUERY_HISTORY_TIME_OFFSET_SERIALIZER);
        if (queryHistoryTimeOffsetList.isEmpty()) {
            return new QueryHistoryTimeOffset(System.currentTimeMillis(), System.currentTimeMillis());
        }

        return queryHistoryTimeOffsetList.get(0);
    }
}
