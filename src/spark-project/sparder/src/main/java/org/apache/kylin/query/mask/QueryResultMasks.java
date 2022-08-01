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

package org.apache.kylin.query.mask;

import org.apache.calcite.rel.RelNode;
import org.apache.kylin.common.KylinConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public final class QueryResultMasks {

    public static final ThreadLocal<QueryResultMask> THREAD_LOCAL = new ThreadLocal<>();

    private QueryResultMasks() {
    }

    public static void init(String project, KylinConfig kylinConfig) {
        THREAD_LOCAL.set(new CompositeQueryResultMasks(new QueryDependentColumnMask(project, kylinConfig),
                new QuerySensitiveDataMask(project, kylinConfig)));
    }

    public static void remove() {
        THREAD_LOCAL.remove();
    }

    public static Dataset<Row> maskResult(Dataset<Row> df) {
        if (THREAD_LOCAL.get() == null) {
            return df;
        }
        return THREAD_LOCAL.get().doMaskResult(df);
    }

    public static void setRootRelNode(RelNode relNode) {
        if (THREAD_LOCAL.get() != null) {
            THREAD_LOCAL.get().doSetRootRelNode(relNode);
        }
    }
}
