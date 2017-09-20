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
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class QueryContextManager {

    private static final Logger logger = LoggerFactory.getLogger(QueryContextManager.class);

    private static final ConcurrentMap<String, QueryContext> idContextMap = Maps.newConcurrentMap();
    private static final ThreadLocal<QueryContext> contexts = new ThreadLocal<QueryContext>() {
        @Override
        protected QueryContext initialValue() {
            QueryContext queryContext = new QueryContext();
            idContextMap.put(queryContext.getQueryId(), queryContext);
            return queryContext;
        }
    };

    public static QueryContext current() {
        return contexts.get();
    }

    /**
     * invoked by program
     */
    public static void resetCurrent() {
        QueryContext queryContext = contexts.get();
        if (queryContext != null) {
            idContextMap.remove(queryContext.getQueryId());
            contexts.remove();
        }
    }

    /**
     * invoked by user to let query stop early
     * @link resetCurrent() should be finally invoked
     */
    public static void stopQuery(String queryId, String info) {
        QueryContext queryContext = idContextMap.get(queryId);
        if (queryContext != null) {
            queryContext.stopEarly(info);
        } else {
            logger.info("the query:{} is not existed", queryId);
        }
    }

    public static List<QueryContext> getAllRunningQueries() {
        // Sort by descending order
        TreeSet<QueryContext> queriesSet = new TreeSet<>(new Comparator<QueryContext>() {
            @Override
            public int compare(QueryContext o1, QueryContext o2) {
                if (o2.getAccumulatedMillis() > o1.getAccumulatedMillis()) {
                    return 1;
                } else if (o2.getAccumulatedMillis() < o1.getAccumulatedMillis()) {
                    return -1;
                } else {
                    return 0;
                }
            }
        });

        for (QueryContext runningQuery : idContextMap.values()) {
            queriesSet.add(runningQuery);
        }
        return Lists.newArrayList(queriesSet);
    }

    /**
     * @param runningTime in milliseconds
     * @return running queries that have run more than specified time
     */
    public static List<QueryContext> getLongRunningQueries(int runningTime) {
        List<QueryContext> allRunningQueries = getAllRunningQueries();
        int i = 0;
        for (; i < allRunningQueries.size(); i++) {
            if (allRunningQueries.get(i).getAccumulatedMillis() < runningTime) {
                break;
            }
        }
        return allRunningQueries.subList(0, i);
    }
}
