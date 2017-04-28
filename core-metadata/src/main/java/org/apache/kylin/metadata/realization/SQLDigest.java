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
import java.util.Set;

import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.ImmutableList;

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

    public String factTable;
    public TupleFilter filter;
    public List<JoinDesc> joinDescs;
    public Set<TblColRef> allColumns;
    public List<TblColRef> groupbyColumns;
    public Set<TblColRef> subqueryJoinParticipants;
    public Set<TblColRef> filterColumns;
    public Set<TblColRef> metricColumns;
    public List<FunctionDesc> aggregations; // storage level measure type, on top of which various sql aggr function may apply
    public List<SQLCall> aggrSqlCalls; // sql level aggregation function call
    public List<TblColRef> sortColumns;
    public List<OrderEnum> sortOrders;
    public boolean isRawQuery;
    public boolean limitPrecedesAggr;

    public SQLDigest(String factTable, TupleFilter filter, List<JoinDesc> joinDescs, Set<TblColRef> allColumns, //
            List<TblColRef> groupbyColumns, Set<TblColRef> subqueryJoinParticipants, Set<TblColRef> filterColumns, Set<TblColRef> metricColumns, //
            List<FunctionDesc> aggregations, List<SQLCall> aggrSqlCalls, List<TblColRef> sortColumns, List<OrderEnum> sortOrders, boolean limitPrecedesAggr) {
        this.factTable = factTable;
        this.filter = filter;
        this.joinDescs = joinDescs;
        this.allColumns = allColumns;
        this.groupbyColumns = groupbyColumns;
        this.subqueryJoinParticipants = subqueryJoinParticipants;
        this.filterColumns = filterColumns;
        this.metricColumns = metricColumns;
        this.aggregations = aggregations;
        this.aggrSqlCalls = aggrSqlCalls;
        this.sortColumns = sortColumns;
        this.sortOrders = sortOrders;
        this.isRawQuery = isRawQuery();
        this.limitPrecedesAggr = limitPrecedesAggr;
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
