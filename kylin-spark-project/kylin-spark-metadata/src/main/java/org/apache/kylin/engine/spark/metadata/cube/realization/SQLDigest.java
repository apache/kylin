/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

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

package org.apache.kylin.engine.spark.metadata.cube.realization;

import org.apache.kylin.engine.spark.metadata.cube.model.FunctionDesc;
import org.apache.kylin.engine.spark.metadata.cube.model.JoinDesc;
import org.apache.kylin.engine.spark.metadata.cube.model.MeasureDesc;
import org.apache.kylin.engine.spark.metadata.cube.model.TblColRef;
import org.apache.kylin.metadata.filter.TupleFilter;

import java.util.List;
import java.util.Set;

/**
 */
public class SQLDigest {

    public enum OrderEnum {
        ASCENDING, DESCENDING
    }

    // model
    public String factTable;
    public Set<TblColRef> allColumns;
    public List<JoinDesc> joinDescs;

    // group by
    public List<TblColRef> groupbyColumns;
    public Set<TblColRef> subqueryJoinParticipants; // FIXME: can we add subqueryJoinParticipants to allColumns/groupbyCols at OLAPContext? from dong@newten

    // aggregation
    public Set<TblColRef> metricColumns;
    public List<FunctionDesc> aggregations; // storage level measure type, on top of which various sql aggr function may apply

    // filter
    public Set<TblColRef> filterColumns;
    public TupleFilter filter;
    public TupleFilter havingFilter;

    // sort & limit
    public List<TblColRef> sortColumns;
    public List<OrderEnum> sortOrders;
    public boolean isRawQuery;
    public int limit = Integer.MAX_VALUE;
    public boolean limitPrecedesAggr;

    public Set<MeasureDesc> involvedMeasure;

    public SQLDigest(String factTable, Set<TblColRef> allColumns, List<JoinDesc> joinDescs, // model
                     List<TblColRef> groupbyColumns, Set<TblColRef> subqueryJoinParticipants, // group by
                     Set<TblColRef> metricColumns, List<FunctionDesc> aggregations, // aggregation
                     Set<TblColRef> filterColumns, TupleFilter filter, TupleFilter havingFilter, // filter
                     List<TblColRef> sortColumns, List<OrderEnum> sortOrders, int limit, boolean limitPrecedesAggr, // sort & limit
                     Set<MeasureDesc> involvedMeasure) {
        this.factTable = factTable;
        this.allColumns = allColumns;
        this.joinDescs = joinDescs;

        this.groupbyColumns = groupbyColumns;
        this.subqueryJoinParticipants = subqueryJoinParticipants;

        this.metricColumns = metricColumns;
        this.aggregations = aggregations;

        this.filterColumns = filterColumns;
        this.filter = filter;
        this.havingFilter = havingFilter;

        this.sortColumns = sortColumns;
        this.sortOrders = sortOrders;
        this.isRawQuery = isRawQuery();
        this.limit = limit;
        this.limitPrecedesAggr = limitPrecedesAggr;

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

    public SQLDigest updateDigestFilter(TupleFilter filter) {
        this.filter = filter;
        return this;
    }

    @Override
    public String toString() {
        return "fact table " + this.factTable + "," + //
                "group by " + this.groupbyColumns + "," + //
                "filter on " + this.filterColumns + "," + //
                "with aggregates" + this.aggregations + ".";
    }

}
