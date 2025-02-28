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

package org.apache.kylin.query.calcite;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import java.util.Map;
import java.util.function.Supplier;

import org.apache.calcite.adapter.enumerable.AggImplementor;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.kylin.cache.utils.ReflectionUtil;

public class KylinSumSplitter extends SqlSplittableAggFunction.SumSplitter {

    public static final KylinSumSplitter INSTANCE = new KylinSumSplitter();
    public static final SqlAggFunction KYLIN_SUM = new KylinSqlSumAggFunction(castNonNull(null));

    @SuppressWarnings("unchecked")
    public static void registerRexImpTable() {
        RexImpTable rexImpTable = RexImpTable.INSTANCE;
        Map<SqlAggFunction, Supplier<? extends AggImplementor>> aggMap = //
                (Map<SqlAggFunction, Supplier<? extends AggImplementor>>) ReflectionUtil.getFieldValue(rexImpTable,
                        "aggMap");
        Supplier<? extends AggImplementor> calciteSumImplementorSupplier = aggMap.get(SqlStdOperatorTable.SUM);
        aggMap.put(KYLIN_SUM, calciteSumImplementorSupplier);
    }

    @Override
    public SqlAggFunction getMergeAggFunctionOfTopSplit() {
        return KYLIN_SUM;
    }
}
