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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.kylin.jdbc.KylinMeta.KMetaProject;

public interface IRemoteClient extends Closeable {

    public static class QueryResult {
        public final List<ColumnMetaData> columnMeta;
        public final Iterable<Object> iterable;

        public QueryResult(List<ColumnMetaData> columnMeta, Iterable<Object> iterable) {
            this.columnMeta = columnMeta;
            this.iterable = iterable;
        }
    }

    /**
     * Connect to Kylin restful service. IOException will be thrown if authentication failed.
     */
    public void connect() throws IOException;

    /**
     * Retrieve meta data of given project.
     */
    public KMetaProject retrieveMetaData(String project) throws IOException;

    /**
     * Execute query remotely and get back result.
     */
    public QueryResult executeQuery(String sql, List<AvaticaParameter> params, List<Object> paramValues, Map<String, String> queryToggles) throws IOException;

}
