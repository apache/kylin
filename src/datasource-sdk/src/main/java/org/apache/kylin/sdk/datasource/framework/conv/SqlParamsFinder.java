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
package org.apache.kylin.sdk.datasource.framework.conv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import org.apache.kylin.guava30.shaded.common.cache.Cache;
import org.apache.kylin.guava30.shaded.common.cache.CacheBuilder;
import org.apache.kylin.guava30.shaded.common.collect.Lists;

public class SqlParamsFinder {

    private final static Cache<SqlCall, Map<Integer, List<Integer>>> PATH_CACHE = CacheBuilder.newBuilder()
            .expireAfterWrite(1, TimeUnit.DAYS).maximumSize(100).build();

    private Map<Integer, List<Integer>> paramPath;

    private SqlCall sourceTmpl;

    private SqlCall sqlCall;

    public SqlParamsFinder(SqlCall sourceTmpl, SqlCall sqlCall) {
        this.sourceTmpl = sourceTmpl;
        this.sqlCall = sqlCall;
    }

    public Map<Integer, SqlNode> getParamNodes() {
        paramPath = SqlParamsFinder.PATH_CACHE.getIfPresent(this.sourceTmpl);
        if (paramPath == null) {
            this.paramPath = new TreeMap<>();
            genParamPath(this.sourceTmpl, new ArrayList<Integer>());
            SqlParamsFinder.PATH_CACHE.put(this.sourceTmpl, this.paramPath);
        }
        Map<Integer, SqlNode> sqlNodes = new HashMap<>();
        for (Map.Entry<Integer, List<Integer>> entry : paramPath.entrySet()) {
            List<Integer> path = entry.getValue();
            sqlNodes.put(entry.getKey(), getParamNode(path, sqlCall, 0));
        }
        return sqlNodes;
    }

    private SqlNode getParamNode(List<Integer> path, SqlNode sqlNode, int level) {
        if (level == path.size() - 1) {
            return ((SqlCall) sqlNode).getOperandList().get(path.get(level));
        } else {
            return getParamNode(path, ((SqlCall) sqlNode).getOperandList().get(path.get(level)), ++level);
        }
    }

    private void genParamPath(SqlNode sqlNode, List<Integer> path) {
        if (sqlNode instanceof SqlIdentifier) {
            int paramIdx = ParamNodeParser.parseParamIdx(sqlNode.toString());
            if (paramIdx >= 0 && path.size() > 0) {
                paramPath.put(paramIdx, path);
            }
        } else if (sqlNode instanceof SqlCall) {
            List<SqlNode> operands = ((SqlCall) sqlNode).getOperandList();
            for (int i = 0; i < operands.size(); i++) {
                List<Integer> copiedPath = Lists.newArrayList(path);
                copiedPath.add(i);
                genParamPath(operands.get(i), copiedPath);
            }
        }
    }

    public static SqlParamsFinder newInstance(SqlCall sourceTmpl, final SqlCall sqlCall, boolean isWindowCall) {
        if (!isWindowCall) {
            return new SqlParamsFinder(sourceTmpl, sqlCall);
        } else {
            return new SqlParamsFinder(sourceTmpl, sqlCall) {

                @Override
                public Map<Integer, SqlNode> getParamNodes() {
                    Map<Integer, SqlNode> sqlNodes = new HashMap<>();
                    List<SqlNode> sqlNodeList = sqlCall.getOperandList();
                    SqlNode firstParam = ((SqlCall) sqlNodeList.get(0)).getOperandList().get(0);
                    SqlNode secondParam = (SqlCall) sqlNodeList.get(1);
                    sqlNodes.put(0, firstParam);
                    sqlNodes.put(1, secondParam);
                    return sqlNodes;
                }
            };
        }
    }
}
