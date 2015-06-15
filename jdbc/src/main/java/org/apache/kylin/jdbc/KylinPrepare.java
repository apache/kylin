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

package org.apache.kylin.jdbc;

import java.util.List;
import java.util.Map;

import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.AvaticaPrepareResult;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Cursor;

/**
 * Interface of kylin prepare statement implementation
 * 
 * @author xduo
 * 
 */
public interface KylinPrepare {

    PrepareResult prepare(String sql);

    /**
     * The result of preparing a query. It gives the Avatica driver framework
     * the information it needs to create a prepared statement, or to execute a
     * statement directly, without an explicit prepare step.
     */
    public static class PrepareResult implements AvaticaPrepareResult {
        public final String sql; // for debug
        public final ColumnMetaData.StructType structType;
        public final Enumerator<Object[]> enumerator;
        public final List<AvaticaParameter> parameterList;

        public PrepareResult(String sql, List<AvaticaParameter> parameterList, Enumerator<Object[]> enumerator, ColumnMetaData.StructType structType) {
            super();
            this.sql = sql;
            this.parameterList = parameterList;
            this.enumerator = enumerator;
            this.structType = structType;
        }

        public Cursor createCursor() {
            return new EnumeratorCursor<Object[]>(enumerator) {
                @Override
                protected Getter createGetter(int ordinal) {
                    return new ArrayEnumeratorGetter(ordinal);
                }

                /**
                 * Row field accessor via index
                 */
                class ArrayEnumeratorGetter extends AbstractGetter {
                    protected final int field;

                    public ArrayEnumeratorGetter(int field) {
                        this.field = field;
                    }

                    public Object getObject() {
                        Object o = current()[field];
                        wasNull[0] = o == null;
                        return o;
                    }
                }
            };
        }

        public List<ColumnMetaData> getColumnList() {
            return structType.columns;
        }

        public List<AvaticaParameter> getParameterList() {
            return parameterList;
        }

        public Map<String, Object> getInternalParameters() {
            return null;
        }

        public String getSql() {
            return sql;
        }
    }

}
