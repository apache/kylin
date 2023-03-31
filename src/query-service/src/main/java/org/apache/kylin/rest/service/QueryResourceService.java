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
package org.apache.kylin.rest.service;

import java.util.ArrayList;

import org.apache.spark.ExecutorAllocationClient;
import org.apache.spark.sql.SparderEnv;
import org.springframework.stereotype.Component;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import scala.collection.JavaConverters;

@Slf4j
@Component
public class QueryResourceService {

    public QueryResource adjustQueryResource(QueryResource resource) {
        int adjustNum;
        if (resource.instance > 0) {
            adjustNum = requestExecutor(resource.instance);
        } else {
            adjustNum = releaseExecutor(resource.instance * -1, resource.force);
        }
        return new QueryResource(adjustNum, resource.force);
    }

    public int getExecutorSize() {
        return getExecutorAllocationClient().getExecutorIds().size();
    }

    private int requestExecutor(int instance) {
        val client = getExecutorAllocationClient();
        return client.requestExecutors(instance) ? instance : 0;
    }

    private int releaseExecutor(int instance, boolean force) {
        val client = getExecutorAllocationClient();
        val ids = client.getExecutorIds().iterator();
        val idsToRemoved = new ArrayList<String>();
        while (ids.hasNext()) {
            if (idsToRemoved.size() == instance)
                break;
            val id = ids.next();
            idsToRemoved.add(id);
        }

        if (idsToRemoved.isEmpty()) {
            return 0;
        }
        return client.killExecutors(JavaConverters.asScalaBuffer(idsToRemoved).toSeq(), true, false, force).size();
    }

    private ExecutorAllocationClient getExecutorAllocationClient() {
        return SparderEnv.executorAllocationClient().get();
    }

    public boolean isAvailable() {
        boolean available = SparderEnv.executorAllocationClient().isDefined() && SparderEnv.isSparkAvailable();
        log.info("node is available={}", available);
        return available;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class QueryResource {
        private int instance;
        private boolean force;
    }
}