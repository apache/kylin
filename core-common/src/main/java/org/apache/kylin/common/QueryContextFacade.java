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

import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;

import org.apache.kylin.common.threadlocal.InternalThreadLocal;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryContextFacade {

    private static final Logger logger = LoggerFactory.getLogger(QueryContextFacade.class);

    private static final ConcurrentMap<String, QueryContext> RUNNING_CTX_MAP = Maps.newConcurrentMap();
    private static final InternalThreadLocal<QueryContext> CURRENT_CTX = new InternalThreadLocal<>();

    public static QueryContext current() {
        QueryContext ret = CURRENT_CTX.get();
        if (ret == null) {
            throw new RuntimeException("Query context hasn't been initialized!!!");
        }
        return ret;
    }

    /**
     * invoked by program
     */
    public static void resetCurrent() {
        QueryContext queryContext = CURRENT_CTX.get();
        if (queryContext != null) {
            RUNNING_CTX_MAP.remove(queryContext.getQueryId());
            CURRENT_CTX.remove();
        }
    }

    public static QueryContext startQuery(String project, String sql, String user) {
        return startQuery(project, sql, user, KylinConfig.getInstanceFromEnv().getHBaseMaxConnectionThreadsPerQuery());
    }

    public static QueryContext startQuery(String project, String sql, String user, int maxHBaseConnectionThreads) {
        QueryContext query = new QueryContext(project, sql, user, maxHBaseConnectionThreads);
        CURRENT_CTX.set(query);
        RUNNING_CTX_MAP.put(query.getQueryId(), query);
        return query;
    }

    /**
     * invoked by user to let query stop early
     * @link resetCurrent() should be finally invoked
     */
    public static void stopQuery(String queryId, String info) {
        QueryContext queryContext = RUNNING_CTX_MAP.get(queryId);
        if (queryContext != null) {
            queryContext.stopEarly(info);
        } else {
            logger.info("the query:{} is not existed", queryId);
        }
    }

    public static TreeSet<QueryContext> getAllRunningQueries() {
        TreeSet<QueryContext> runningQueries = Sets.newTreeSet(new Comparator<QueryContext>() {
            @Override
            public int compare(QueryContext o1, QueryContext o2) {
                if (o2.getAccumulatedMillis() > o1.getAccumulatedMillis()) {
                    return 1;
                } else if (o2.getAccumulatedMillis() < o1.getAccumulatedMillis()) {
                    return -1;
                } else {
                    return o1.getQueryId().compareTo(o2.getQueryId());
                }
            }
        });

        runningQueries.addAll(RUNNING_CTX_MAP.values());
        return runningQueries;
    }

    /**
     * @param runningTime in milliseconds
     * @return running queries that have run more than specified time
     */
    public static TreeSet<QueryContext> getLongRunningQueries(long runningTime) {
        SortedSet<QueryContext> allRunningQueries = getAllRunningQueries();
        QueryContext tmpCtx = new QueryContext(null, null, null,
                KylinConfig.getInstanceFromEnv().getHBaseMaxConnectionThreadsPerQuery(), runningTime + 1L); // plus 1 to include those contexts in same accumulatedMills but different uuid
        return (TreeSet<QueryContext>) allRunningQueries.headSet(tmpCtx);
    }
}
