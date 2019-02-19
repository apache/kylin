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

package org.apache.kylin.storage.stream;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.storage.IStorage;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.kylin.storage.hbase.steps.HBaseMROutput2Transition;
import org.apache.kylin.storage.stream.rpc.HttpStreamDataSearchClient;
import org.apache.kylin.storage.stream.rpc.IStreamDataSearchClient;
import org.apache.kylin.storage.stream.rpc.MockedStreamDataSearchClient;

//used by reflection
public class StreamStorage implements IStorage {
    private volatile IStreamDataSearchClient realTimeSearchClient;

    @Override
    public IStorageQuery createQuery(IRealization realization) {
        if (realization.getType() == RealizationType.CUBE) {
            CubeInstance cubeInstance = (CubeInstance) realization;
            return new StreamStorageQuery(cubeInstance, getStreamingDataSearchClient());
        } else {
            throw new IllegalArgumentException("Unknown realization type " + realization.getType());
        }
    }

    private IStreamDataSearchClient getStreamingDataSearchClient() {
        if (realTimeSearchClient == null) {
            synchronized (this) {
                if (realTimeSearchClient == null) {
                    KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
                    if (kylinConfig.isStreamingStandAloneMode()) {
                        realTimeSearchClient = new MockedStreamDataSearchClient();
                    } else {
                        realTimeSearchClient = new HttpStreamDataSearchClient();
                    }
                }
            }
        }
        return realTimeSearchClient;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <I> I adaptToBuildEngine(Class<I> engineInterface) {
        if (engineInterface == IMROutput2.class) {
            return (I) new HBaseMROutput2Transition();
        } else {
            throw new RuntimeException("Cannot adapt to " + engineInterface);
        }
    }
}
