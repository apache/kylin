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

package org.apache.kylin.stream.server.storage;

import org.apache.kylin.common.QueryContextFacade;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.gtrecord.GTCubeStorageQueryBase;
import org.apache.kylin.storage.gtrecord.GTCubeStorageQueryRequest;
import org.apache.kylin.stream.core.query.StreamingQueryProfile;
import org.apache.kylin.stream.core.storage.StreamingSegmentManager;
import org.apache.kylin.stream.server.StreamingServer;

/**
 * local storage query for streaming
 */
public class LocalStreamStorageQuery extends GTCubeStorageQueryBase {

    public LocalStreamStorageQuery(CubeInstance cube) {
        super(cube);
    }

    @Override
    public ITupleIterator search(StorageContext context, SQLDigest sqlDigest, TupleInfo returnTupleInfo) {
        StreamingSegmentManager cubeDataStore = StreamingServer.getInstance().getStreamingSegmentManager(
                cubeInstance.getName());
        boolean enableStreamProfile = BackdoorToggles.isStreamingProfileEnable();
        StreamingQueryProfile queryProfile = new StreamingQueryProfile(QueryContextFacade.current().getQueryId(),
                System.currentTimeMillis());
        if (enableStreamProfile) {
            queryProfile.enableDetailProfile();
        }
        StreamingQueryProfile.set(queryProfile);
        GTCubeStorageQueryRequest request = getStorageQueryRequest(context, sqlDigest, returnTupleInfo);
        return cubeDataStore.getSearcher().search(returnTupleInfo, request.getFilter(), request.getHavingFilter(),
                request.getDimensions(), request.getGroups(), request.getMetrics(), context.isNeedStorageAggregation());
    }

    @Override
    protected String getGTStorage() {
        return null;
    }

}
