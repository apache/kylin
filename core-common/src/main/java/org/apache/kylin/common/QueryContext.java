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

package org.apache.kylin.common;

import java.util.concurrent.atomic.AtomicLong;

/**
 * checkout {@link org.apache.kylin.common.debug.BackdoorToggles} for comparison
 */
public class QueryContext {

    private static final ThreadLocal<QueryContext> contexts = new ThreadLocal<QueryContext>() {
        @Override
        protected QueryContext initialValue() {
            return new QueryContext();
        }
    };

    private String queryId;
    private AtomicLong scanBytes = new AtomicLong();

    private QueryContext() {
        // use QueryContext.current() instead
    }

    public static QueryContext current() {
        return contexts.get();
    }

    public static void reset() {
        contexts.remove();
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public long getScanBytes() {
        return scanBytes.get();
    }

    public long addAndGetScanBytes(long delta) {
        return scanBytes.addAndGet(delta);
    }
}
