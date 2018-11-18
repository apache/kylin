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

package org.apache.kylin.storage.druid;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.storage.IStorage;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.kylin.storage.druid.common.DruidSerdeHelper;
import org.apache.kylin.storage.druid.http.HttpClient;
import org.apache.kylin.storage.druid.http.HttpClientConfig;
import org.apache.kylin.storage.druid.http.HttpClientFactory;
import org.apache.kylin.storage.druid.read.DruidStorageQuery;
import org.apache.kylin.storage.druid.read.cursor.RowCursorFactory;
import org.apache.kylin.storage.druid.write.DruidMROutput;
import org.joda.time.Duration;

public class DruidStorage implements IStorage {
    private final HttpClient httpClient;
    private final RowCursorFactory cursorFactory;

    public DruidStorage() {
        final HttpClientConfig.Builder builder = HttpClientConfig
                .builder()
                .withNumConnections(100)
                .withReadTimeout(Duration.standardSeconds(60))
                .withWorkerCount(40)
                .withCompressionCodec(HttpClientConfig.CompressionCodec.IDENTITY);

        this.httpClient = HttpClientFactory.createClient(builder.build());
        this.cursorFactory = new RowCursorFactory(httpClient, DruidSerdeHelper.JSON_MAPPER);
    }

    @Override
    public IStorageQuery createQuery(IRealization realization) {
        if (realization.getType() == RealizationType.CUBE) {
            CubeInstance cube = (CubeInstance) realization;
            return new DruidStorageQuery(cube, cursorFactory);
        }
        throw new RuntimeException("Druid storage doesn't support realization type: " + realization.getType());
    }

    @Override
    public <I> I adaptToBuildEngine(Class<I> engineInterface) {
        if (engineInterface == IMROutput2.class) {
            return engineInterface.cast(new DruidMROutput());
        }
        throw new RuntimeException("Druid storage can't adapt to " + engineInterface);
    }
}
