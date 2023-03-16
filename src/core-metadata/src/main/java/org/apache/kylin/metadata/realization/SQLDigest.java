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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

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
            Set<TblColRef> filterColumns, // filter
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

        this.sortColumns = sortColumns;
        this.sortOrders = sortOrders;
        this.isRawQuery = isRawQuery();
        this.limit = limit;
        this.limitPrecedesAggr = limitPrecedesAggr;

        this.involvedMeasure = involvedMeasure;

        this.includeSubqueryJoinParticipants();
        this.allColumns = Collections.unmodifiableSet(allColumns);
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

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof SQLDigest))
            return false;
        SQLDigest sqlDigest = (SQLDigest) o;
        return isRawQuery() == sqlDigest.isRawQuery() && limit == sqlDigest.limit
                && limitPrecedesAggr == sqlDigest.limitPrecedesAggr && factTable.equals(sqlDigest.factTable)
                && equalsIgnoreOrder(allColumns, sqlDigest.allColumns)
                && equalsIgnoreOrder(joinDescs, sqlDigest.joinDescs)
                && equalsIgnoreOrder(groupbyColumns, sqlDigest.groupbyColumns)
                && subqueryJoinParticipants.equals(sqlDigest.subqueryJoinParticipants)
                && equalsIgnoreOrder(metricColumns, sqlDigest.metricColumns)
                && equalsIgnoreOrder(aggregations, sqlDigest.aggregations)
                && equalsIgnoreOrder(filterColumns, sqlDigest.filterColumns)
                && equalsIgnoreOrder(sortColumns, sqlDigest.sortColumns) && sortOrders.equals(sqlDigest.sortOrders)
                && involvedMeasure.equals(sqlDigest.involvedMeasure);
    }

    private static boolean equalsIgnoreOrder(Collection c1, Collection c2) {
        if (c1 == c2) {
            return true;
        }
        if (c1 == null || c2 == null) {
            return false;
        }
        if (c1.size() != c2.size()) {
            return false;
        }
        for (Object o1 : c1) {
            boolean findO1OnC2 = false;
            for (Object o2 : c2) {
                if (objectEquals(o1, o2)) {
                    findO1OnC2 = true;
                    break;
                }
            }
            if (!findO1OnC2) {
                return false;
            }
        }
        for (Object o2 : c2) {
            boolean findO2OnC1 = false;
            for (Object o1 : c1) {
                if (objectEquals(o2, o1)) {
                    findO2OnC1 = true;
                    break;
                }
            }
            if (!findO2OnC1) {
                return false;
            }
        }
        return true;
    }

    private static boolean equalsConsiderOrder(Collection l1, Collection l2) {
        if (l1 == l2) {
            return true;
        }
        if (l1 == null || l2 == null) {
            return false;
        }
        if (l1.size() != l2.size()) {
            return false;
        }
        Iterator e1 = l1.iterator();
        Iterator e2 = l1.iterator();
        while (e1.hasNext() && e2.hasNext()) {
            Object o1 = e1.next();
            Object o2 = e2.next();
            if (!(o1 == null ? o2 == null : objectEquals(o1, o2))) {
                return false;
            }
        }
        return !(e1.hasNext() || e2.hasNext());
    }

    private static boolean objectEquals(Object o1, Object o2) {
        if (o1 instanceof TblColRef && o2 instanceof TblColRef) {
            return equals((TblColRef) o1, (TblColRef) o2);
        }
        if (o1 instanceof ParameterDesc && o2 instanceof ParameterDesc) {
            return equals((ParameterDesc) o1, (ParameterDesc) o2);
        }
        if (o1 instanceof FunctionDesc && o2 instanceof FunctionDesc) {
            return equals((FunctionDesc) o1, (FunctionDesc) o2);
        }
        if (o1 instanceof JoinDesc && o2 instanceof JoinDesc) {
            return equals((JoinDesc) o1, (JoinDesc) o2);
        }
        return false;
    }

    private static boolean equals(JoinDesc j1, JoinDesc j2) {
        if (j1 == j2)
            return true;
        if (j1 == null || j2 == null)
            return false;
        // note pk/fk are sorted, sortByFK()
        if (!Arrays.equals(j1.getForeignKey(), j2.getForeignKey()))
            return false;
        if (!Arrays.equals(j1.getPrimaryKey(), j2.getPrimaryKey()))
            return false;
        if (!equalsConsiderOrder(Lists.newArrayList(j1.getForeignKeyColumns()),
                Lists.newArrayList(j2.getForeignKeyColumns()))) {
            return false;
        }
        if (!equalsConsiderOrder(Lists.newArrayList(j1.getPrimaryKeyColumns()),
                Lists.newArrayList(j2.getPrimaryKeyColumns()))) {
            return false;
        }
        if (!j1.getType().equalsIgnoreCase(j2.getType()))
            return false;
        return true;
    }

    private static boolean equals(FunctionDesc f1, FunctionDesc f2) {
        if (f1 == f2)
            return true;
        if (f1 == null || f2 == null)
            return false;
        if (!Objects.equals(f1.getExpression(), f2.getExpression()))
            return false;
        if (f1.isCountDistinct()) {
            // for count distinct func, param's order doesn't matter
            if (CollectionUtils.isEmpty(f1.getParameters())) {
                if (CollectionUtils.isNotEmpty(f2.getParameters()))
                    return false;
            } else {
                return equalsIgnoreOrder(f1.getParameters(), f2.getParameters());
            }
        } else if (f1.isCountConstant() && f2.isCountConstant()) { //count(*) and count(1) are equals
            return true;
        } else {
            if (CollectionUtils.isEmpty(f1.getParameters())) {
                if (CollectionUtils.isNotEmpty(f2.getParameters()))
                    return false;
            } else {
                if (!equalsConsiderOrder(f1.getParameters(), f2.getParameters()))
                    return false;
            }
        }
        return true;
    }

    private static boolean equals(ParameterDesc p1, ParameterDesc p2) {
        if (p1 == p2)
            return true;
        if (p1 == null || p2 == null)
            return false;
        if (p1.getType() != null ? !p1.getType().equals(p2.getType()) : p2.getType() != null)
            return false;
        if (p1.isColumnType() != p2.isColumnType()) {
            return false;
        }
        if (p1.isColumnType() && !equals(p2.getColRef(), p1.getColRef())) {
            return false;
        }
        if (!p1.isColumnType() && !p1.getValue().equals(p2.getValue())) {
            return false;
        }
        return true;
    }

    private static boolean equals(TblColRef t1, TblColRef t2) {
        if (t1 == t2)
            return true;
        if (t1 == null || t2 == null)
            return false;
        if (!StringUtils.equals(t1.getColumnDesc().getTable().getIdentity(),
                t2.getColumnDesc().getTable().getIdentity()))
            return false;
        if (!StringUtils.equals(t1.getColumnDesc().getName(), t2.getColumnDesc().getName()))
            return false;
        if (!(t1.getTableRef() == null ? t2.getTableRef() == null : t1.getTableRef().equals(t2.getTableRef())))
            return false;
        if (t1.isInnerColumn() != t2.isInnerColumn())
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(factTable, allColumns, joinDescs, groupbyColumns, subqueryJoinParticipants, metricColumns,
                aggregations, filterColumns, sortColumns, sortOrders, isRawQuery(), limit, limitPrecedesAggr,
                involvedMeasure);
    }

}
