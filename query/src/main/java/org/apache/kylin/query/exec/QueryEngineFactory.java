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

package org.apache.kylin.query.exec;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryEngineFactory {
    private static final Logger log = LoggerFactory.getLogger(QueryEngineFactory.class);

    public static Enumerable<Object> computeSCALA(DataContext dataContext, RelNode relNode, RelDataType resultType)
            throws IllegalAccessException, ClassNotFoundException, InstantiationException {
        try {
            String property = System.getProperty("kylin-query-engine", "org.apache.kylin.query.runtime.SparkEngine");
            QueryEngine o = (QueryEngine) Class.forName(property).newInstance();
            return o.computeSCALA(dataContext, relNode, resultType);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw e;
        }
    }

    public static Enumerable<Object[]> compute(DataContext dataContext, RelNode relNode, RelDataType resultType)
            throws IllegalAccessException, ClassNotFoundException, InstantiationException {
        try {
            String property = System.getProperty("kylin-query-engine", "org.apache.kylin.query.runtime.SparkEngine");
            QueryEngine o = (QueryEngine) Class.forName(property).newInstance();
            return o.compute(dataContext, relNode, resultType);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw e;
        }
    }

    public static Enumerable<Object[]> computeAsync(DataContext dataContext, RelNode relNode, RelDataType resultType) {
        throw new UnsupportedOperationException("");
    }
}
