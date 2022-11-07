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

package org.apache.kylin.metadata.favorite;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryHistoryIdOffsetManager {

    private static final Logger logger = LoggerFactory.getLogger(QueryHistoryIdOffsetManager.class);

    public static final Serializer<QueryHistoryIdOffset> QUERY_HISTORY_ID_OFFSET_SERIALIZER = new JsonSerializer<>(
            QueryHistoryIdOffset.class);

    private final KylinConfig kylinConfig;
    private ResourceStore resourceStore;
    private String resourceRoot;

    private QueryHistoryIdOffsetManager(KylinConfig kylinConfig, String project) {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing QueryHistoryIdOffsetManager with KylinConfig Id: {} for project {}",
                    System.identityHashCode(kylinConfig), project);
        this.kylinConfig = kylinConfig;
        resourceStore = ResourceStore.getKylinMetaStore(this.kylinConfig);
        this.resourceRoot = "/" + project + ResourceStore.QUERY_HISTORY_ID_OFFSET;
    }

    // called by reflection
    static QueryHistoryIdOffsetManager newInstance(KylinConfig config, String project) {
        return new QueryHistoryIdOffsetManager(config, project);
    }

    public static QueryHistoryIdOffsetManager getInstance(KylinConfig kylinConfig, String project) {
        return kylinConfig.getManager(project, QueryHistoryIdOffsetManager.class);
    }

    private String path(String uuid) {
        return this.resourceRoot + "/" + uuid + MetadataConstants.FILE_SURFIX;
    }

    public void save(QueryHistoryIdOffset idOffset) {
        resourceStore.checkAndPutResource(path(idOffset.getUuid()), idOffset, QUERY_HISTORY_ID_OFFSET_SERIALIZER);
    }

    public QueryHistoryIdOffset get() {
        List<QueryHistoryIdOffset> queryHistoryIdOffsetList = resourceStore.getAllResources(resourceRoot,
                QUERY_HISTORY_ID_OFFSET_SERIALIZER);
        if (queryHistoryIdOffsetList.isEmpty()) {
            return new QueryHistoryIdOffset(0);
        }

        return queryHistoryIdOffsetList.get(0);
    }
}
