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

package org.apache.kylin.query.engine.exec;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.runtime.Bindable;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.QueryTrace;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.query.engine.meta.MutableDataContext;

/**
 * implement and execute a physical plan with Calcite
 * this exec is only used for constants queries
 */
public class CalcitePlanExec implements QueryPlanExec {

    @Override
    public List<List<String>> execute(RelNode rel, MutableDataContext dataContext) {
        QueryContext.currentTrace().startSpan(QueryTrace.EXECUTION);
        initContextVars(dataContext);

        List<List<String>> result = doExecute(rel, dataContext);

        //constant query should fill empty list for scan data
        QueryContext.fillEmptyResultSetMetrics();
        QueryContext.currentTrace().endLastSpan();
        return result;
    }

    public List<List<String>> doExecute(RelNode rel, DataContext dataContext) {
        Bindable bindable = EnumerableInterpretable.toBindable(new HashMap<>(), new TrivialSparkHandler(),
                (EnumerableRel) rel, EnumerableRel.Prefer.ARRAY);

        Enumerable<Object> rawResult = bindable.bind(dataContext);
        List<List<String>> result = new LinkedList<>();

        QueryContext.currentTrace().startSpan(QueryTrace.FETCH_RESULT);
        for (Object rawRow : rawResult.toList()) {
            List<String> row = new LinkedList<>();
            if (rel.getRowType().getFieldCount() > 1) {
                Object[] rowData = (Object[]) rawRow;
                for (int i = 0; i < rowData.length; i++) {
                    row.add(rawQueryResultToString(rowData[i], rel.getRowType().getFieldList().get(i).getType()));
                }
            } else {
                row.add(rawQueryResultToString(rawRow, rel.getRowType().getFieldList().get(0).getType()));
            }
            result.add(row);
        }

        return result;
    }

    private void initContextVars(MutableDataContext dataContext) {
        TimeZone timezone = DataContext.Variable.TIME_ZONE.get(dataContext);
        final long time = System.currentTimeMillis();
        final long localOffset = timezone.getOffset(System.currentTimeMillis());
        dataContext.putContextVar(DataContext.Variable.UTC_TIMESTAMP.camelName, time);
        // to align with calcite implementation, current_timestamp = local_timestamp
        // see org.apache.calcite.jdbc.CalciteConnectionImpl.DataContextImpl.DataContextImpl
        dataContext.putContextVar(DataContext.Variable.CURRENT_TIMESTAMP.camelName, time + localOffset);
        dataContext.putContextVar(DataContext.Variable.LOCAL_TIMESTAMP.camelName, time + localOffset);
    }

    // may induce some puzzle result
    private String rawQueryResultToString(Object object, RelDataType dataType) {
        String value = String.valueOf(object);
        if (object == null) {
            return value;
        }
        switch (dataType.getSqlTypeName()) {
        case DATE:
            return DateFormat.formatDayToEpochToDateStr(Long.parseLong(value), TimeZone.getTimeZone("GMT"));
        case TIMESTAMP:
            return DateFormat.castTimestampToString(Long.parseLong(value), TimeZone.getTimeZone("GMT"));
        case TIME:
            return DateFormat.formatToTimeStr(Long.parseLong(value), "HH:mm:ss", TimeZone.getTimeZone("GMT"));
        // TODO Once KE-42058 has a better fix, this code can be removed
        case DECIMAL:
            return ((BigDecimal) object).stripTrailingZeros().toPlainString();
        default:
            return value;
        }
    }

    private static class TrivialSparkHandler implements CalcitePrepare.SparkHandler {
        public RelNode flattenTypes(RelOptPlanner planner, RelNode rootRel, boolean restructure) {
            return rootRel;
        }

        public void registerRules(RuleSetBuilder builder) {
            // This is a trivial implementation. This method might be called but it is not supposed to do anything
        }

        public boolean enabled() {
            return false;
        }

        public ArrayBindable compile(ClassDeclaration expr, String s) {
            throw new UnsupportedOperationException();
        }

        public Object sparkContext() {
            throw new UnsupportedOperationException();
        }
    }
}
