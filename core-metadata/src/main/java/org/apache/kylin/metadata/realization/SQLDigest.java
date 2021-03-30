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

package org.apache.kylin.metadata.realization;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.metadata.expression.TupleExpression;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.DynamicFunctionDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;

import org.apache.kylin.shaded.com.google.common.collect.ImmutableList;

/**
 */
public class SQLDigest {

    public enum OrderEnum {
        ASCENDING, DESCENDING
    }

    public static class SQLCall {
        public final String function;
        public final List<Object> args;

        public SQLCall(String function, Iterable<Object> args) {
            this.function = function;
            this.args = ImmutableList.copyOf(args);
        }
    }

    // model
    public String factTable;
    public Set<TblColRef> allColumns;
    public List<JoinDesc> joinDescs;

    // group by
    public List<TblColRef> groupbyColumns;
    public Set<TblColRef> subqueryJoinParticipants;

    public Map<TblColRef, TupleExpression> dynGroupbyColumns;
    public boolean groupByExpression;

    // aggregation
    public Set<TblColRef> metricColumns;
    public List<FunctionDesc> aggregations; // storage level measure type, on top of which various sql aggr function may apply
    public List<SQLCall> aggrSqlCalls; // sql level aggregation function call

    public List<DynamicFunctionDesc> dynAggregations;
    public Set<TblColRef> rtDimensionColumns; // dynamic col related dimension columns
    public Set<TblColRef> rtMetricColumns; // dynamic col related metric columns

    // filter
    public Set<TblColRef> filterColumns;
    public TupleFilter filter;
    public TupleFilter havingFilter;

    // sort & limit
    public List<TblColRef> sortColumns;
    public List<OrderEnum> sortOrders;
    public boolean isRawQuery;
    public boolean isBorrowedContext;
    public boolean limitPrecedesAggr;
    public boolean hasLimit;

    public Set<MeasureDesc> involvedMeasure;

    public SQLDigest(String factTable, Set<TblColRef> allColumns, List<JoinDesc> joinDescs, // model
            List<TblColRef> groupbyColumns, Set<TblColRef> subqueryJoinParticipants,
            Map<TblColRef, TupleExpression> dynGroupByColumns, boolean groupByExpression, // group by
            Set<TblColRef> metricColumns, List<FunctionDesc> aggregations, List<SQLCall> aggrSqlCalls, // aggregation
            List<DynamicFunctionDesc> dynAggregations, //
            Set<TblColRef> rtDimensionColumns, Set<TblColRef> rtMetricColumns, // dynamic col related columns
            Set<TblColRef> filterColumns, TupleFilter filter, TupleFilter havingFilter, // filter
            List<TblColRef> sortColumns, List<OrderEnum> sortOrders, boolean limitPrecedesAggr, boolean hasLimit, boolean isBorrowedContext, // sort & limit
            Set<MeasureDesc> involvedMeasure
    ) {
        this.factTable = factTable;
        this.allColumns = allColumns;
        this.joinDescs = joinDescs;

        this.groupbyColumns = groupbyColumns;
        this.subqueryJoinParticipants = subqueryJoinParticipants;

        this.dynGroupbyColumns = dynGroupByColumns;
        this.groupByExpression = groupByExpression;

        this.metricColumns = metricColumns;
        this.aggregations = aggregations;
        this.aggrSqlCalls = aggrSqlCalls;

        this.dynAggregations = dynAggregations;

        this.rtDimensionColumns = rtDimensionColumns;
        this.rtMetricColumns = rtMetricColumns;

        this.filterColumns = filterColumns;
        this.filter = filter;
        this.havingFilter = havingFilter;

        this.sortColumns = sortColumns;
        this.sortOrders = sortOrders;
        this.isRawQuery = isRawQuery();
        this.isBorrowedContext = isBorrowedContext;
        this.limitPrecedesAggr = limitPrecedesAggr;
        this.hasLimit = hasLimit;

        this.involvedMeasure = involvedMeasure;

        this.includeSubqueryJoinParticipants();
    }

    private boolean isRawQuery() {
        return this.groupbyColumns.isEmpty() && // select a group by a -> not raw
                this.aggregations.isEmpty(); // has aggr -> not raw
        //the reason to choose aggregations rather than metricColumns is because the former is set earlier at implOLAP
    }

    public void includeSubqueryJoinParticipants() {
        if (this.isRawQuery) {
            this.allColumns.addAll(this.subqueryJoinParticipants);
        } else {
            this.groupbyColumns.addAll(this.subqueryJoinParticipants);
            this.allColumns.addAll(this.subqueryJoinParticipants);
        }
    }

    @Override
    public String toString() {
        return "fact table " + this.factTable + "," + //
                "group by " + this.groupbyColumns + "," + //
                "filter on " + this.filterColumns + "," + //
                "with aggregates" + this.aggregations + ".";
    }

}
