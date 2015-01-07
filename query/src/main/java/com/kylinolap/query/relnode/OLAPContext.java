/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.query.relnode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.eigenbase.reltype.RelDataType;

import com.kylinolap.cube.CubeInstance;
import com.kylinolap.metadata.model.cube.CubeDesc;
import com.kylinolap.metadata.model.cube.FunctionDesc;
import com.kylinolap.metadata.model.cube.JoinDesc;
import com.kylinolap.metadata.model.cube.TblColRef;
import com.kylinolap.query.schema.OLAPSchema;
import com.kylinolap.storage.StorageContext;
import com.kylinolap.storage.filter.TupleFilter;

/**
 * @author xjiang
 * 
 */
public class OLAPContext {

    public static final String PRM_ACCEPT_PARTIAL_RESULT = "AcceptPartialResult";

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
        return _localContexts.get().get(id);
    }

    public static void clearThreadLocalContexts() {
        _localContexts.remove();
    }

    public OLAPContext(int seq) {
        this.id = seq;
        this.storageContext = new StorageContext();
        Map<String, String> parameters = _localPrarameters.get();
        if (parameters != null) {
            String acceptPartialResult = parameters.get(PRM_ACCEPT_PARTIAL_RESULT);
            if (acceptPartialResult != null) {
                this.storageContext.setAcceptPartialResult(Boolean.parseBoolean(acceptPartialResult));
            }
        }
    }

    public final int id;
    public final StorageContext storageContext;

    // query info
    public OLAPSchema olapSchema = null;
    public OLAPTableScan firstTableScan = null; // to be fact table scan except
                                                // "select * from lookupTable"
    public RelDataType olapRowType = null;
    public boolean afterAggregate = false;
    public boolean afterJoin = false;
    public boolean hasJoin = false;

    // cube metadata
    public CubeInstance cubeInstance;
    public CubeDesc cubeDesc;
    public Collection<TblColRef> allColumns = new HashSet<TblColRef>();
    public Collection<TblColRef> metricsColumns = new HashSet<TblColRef>();
    public Collection<TblColRef> groupByColumns = new ArrayList<TblColRef>();
    public List<FunctionDesc> aggregations = new ArrayList<FunctionDesc>();
    public List<JoinDesc> joins = new LinkedList<JoinDesc>();
    public TupleFilter filter;

    // rewrite info
    public Map<String, RelDataType> rewriteFields = new HashMap<String, RelDataType>();

    // hive query
    public String sql = "";

    public boolean isSimpleQuery() {
        return (joins.size() == 0) && (groupByColumns.size() == 0) && (aggregations.size() == 0);
    }
}
