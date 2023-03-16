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

import java.util.List;

import org.apache.calcite.rel.RelNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

public class CompositeQueryResultMasks implements QueryResultMask {

    private final List<QueryResultMask> queryResultMasks;

    public CompositeQueryResultMasks(QueryResultMask... queryResultMasks) {
        this.queryResultMasks = Lists.newArrayList(queryResultMasks);
    }

    @Override
    public void init() {
        queryResultMasks.forEach(QueryResultMask::init);
    }

    @Override
    public void doSetRootRelNode(RelNode relNode) {
        queryResultMasks.forEach(mask -> mask.doSetRootRelNode(relNode));
    }

    @Override
    public Dataset<Row> doMaskResult(Dataset<Row> df) {
        Dataset<Row> masked = df;
        for (QueryResultMask queryResultMask : queryResultMasks) {
            masked = queryResultMask.doMaskResult(masked);
        }
        return masked;
    }
}
