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

package org.apache.kylin.query.relnode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.query.schema.OLAPSchema;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.hybrid.HybridInstance;

import com.google.common.collect.Lists;

/**
 */
public class OLAPContext {

    public static final String PRM_ACCEPT_PARTIAL_RESULT = "AcceptPartialResult";
    public static final String PRM_USER_AUTHEN_INFO = "UserAuthenInfo";

    private static final ThreadLocal<Map<String, String>> _localPrarameters = new ThreadLocal<Map<String, String>>();

    private static final ThreadLocal<Map<Integer, OLAPContext>> _localContexts = new ThreadLocal<Map<Integer, OLAPContext>>();

    public static void setParameters(Map<String, String> parameters) {
        _localPrarameters.set(parameters);
    }

    public static void clearParameter() {
        _localPrarameters.remove();
    }

    public static void registerContext(OLAPContext ctx) {
        if (_localContexts.get() == null) {
            Map<Integer, OLAPContext> contextMap = new HashMap<Integer, OLAPContext>();
            _localContexts.set(contextMap);
        }
        _localContexts.get().put(ctx.id, ctx);
    }

    public static Collection<OLAPContext> getThreadLocalContexts() {
        Map<Integer, OLAPContext> map = _localContexts.get();
        return map == null ? null : map.values();
    }

    public static OLAPContext getThreadLocalContextById(int id) {
        Map<Integer, OLAPContext> map = _localContexts.get();
        return map.get(id);
    }

    public static void clearThreadLocalContexts() {
        _localContexts.remove();
    }

    public OLAPContext(int seq) {
        this.id = seq;
        this.storageContext = new StorageContext();
        this.sortMeasures = Lists.newArrayList();
        this.sortOrders = Lists.newArrayList();
        Map<String, String> parameters = _localPrarameters.get();
        if (parameters != null) {
            String acceptPartialResult = parameters.get(PRM_ACCEPT_PARTIAL_RESULT);
            if (acceptPartialResult != null) {
                this.storageContext.setAcceptPartialResult(Boolean.parseBoolean(acceptPartialResult));
            }
            String acceptUserInfo = parameters.get(PRM_USER_AUTHEN_INFO);
            if (null != acceptUserInfo)
                this.olapAuthen.parseUserInfo(acceptUserInfo);
        }
    }

    public final int id;
    public final StorageContext storageContext;

    // query info
    public OLAPSchema olapSchema = null;
    public OLAPTableScan firstTableScan = null; // to be fact table scan except "select * from lookupTable"
    public TupleInfo returnTupleInfo = null;
    public boolean afterAggregate = false;
    public boolean afterSkippedFilter = false;
    public boolean afterJoin = false;
    public boolean hasJoin = false;

    // cube metadata
    public IRealization realization;

    public Collection<TblColRef> allColumns = new HashSet<TblColRef>();
    public Collection<TblColRef> groupByColumns = new ArrayList<TblColRef>();
    public Collection<TblColRef> metricsColumns = new HashSet<TblColRef>();
    public List<FunctionDesc> aggregations = new ArrayList<FunctionDesc>();
    public Collection<TblColRef> filterColumns = new HashSet<TblColRef>();
    public TupleFilter filter;
    public List<JoinDesc> joins = new LinkedList<JoinDesc>();
    private List<MeasureDesc> sortMeasures;
    private List<SQLDigest.OrderEnum> sortOrders;

    // rewrite info
    public Map<String, RelDataType> rewriteFields = new HashMap<String, RelDataType>();

    public int limit;

    // hive query
    public String sql = "";

    public OLAPAuthentication olapAuthen = new OLAPAuthentication();

    public boolean isSimpleQuery() {
        return (joins.size() == 0) && (groupByColumns.size() == 0) && (aggregations.size() == 0);
    }

    private SQLDigest sqlDigest;

    public SQLDigest getSQLDigest() {
        if (sqlDigest == null)
            sqlDigest = new SQLDigest(firstTableScan.getTableName(), filter, joins, allColumns, groupByColumns, filterColumns, metricsColumns, aggregations, sortMeasures, sortOrders);
        return sqlDigest;
    }

    public boolean hasPrecalculatedFields() {
        return realization instanceof CubeInstance || realization instanceof HybridInstance;
    }

    public void resetSQLDigest() {
        this.sqlDigest = null;
    }

    public void setReturnTupleInfo(RelDataType rowType, ColumnRowType columnRowType) {
        TupleInfo info = new TupleInfo();
        List<RelDataTypeField> fieldList = rowType.getFieldList();
        for (int i = 0; i < fieldList.size(); i++) {
            RelDataTypeField field = fieldList.get(i);
            TblColRef col = columnRowType == null ? null : columnRowType.getColumnByIndex(i);
            info.setField(field.getName(), col, i);
        }
        this.returnTupleInfo = info;
    }

    public void addSort(MeasureDesc measure, SQLDigest.OrderEnum order) {
        if (measure != null) {
            sortMeasures.add(measure);
            sortOrders.add(order);
        }
    }

    public interface IAccessController {
        /*
        * @return {TupleFilter} if the filter condition exists
        * @OLAPAuthentication the authentication info
        * @columns required columns from logic query plan
        * @realization the cube used in this query
        * @OLAPInsufficientException no rights exception
        */
        public TupleFilter check(OLAPAuthentication olapAuthentication, Collection<TblColRef> columns, IRealization realization) throws IllegalArgumentException;
    }

}
