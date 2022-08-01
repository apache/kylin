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

package org.apache.kylin.source.adhocquery;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;

import com.google.common.collect.Lists;

public interface IPushDownRunner {

    void init(KylinConfig config);

    /**
     * Run an pushdown query in the source database in case Kylin cannot serve using cube.
     *
     * @param query                 the query statement
     * @param returnRows            an empty list to collect returning rows
     * @param returnColumnMeta      an empty list to collect metadata of returning columns
     * @param project               the project name
     * @throws Exception if running pushdown query fails
     */
    void executeQuery(String query, List<List<String>> returnRows, List<SelectedColumnMeta> returnColumnMeta,
            String project) throws Exception;

    default PushdownResult executeQueryToIterator(String query, String project) throws Exception {
        List<List<String>> returnRows = Lists.newArrayList();
        List<SelectedColumnMeta> returnColumnMeta = Lists.newArrayList();
        executeQuery(query, returnRows, returnColumnMeta, project);
        return new PushdownResult(returnRows, returnRows.size(), returnColumnMeta);
    }

    /**
     * Run an pushdown non-query sql
     *
     * @param sql                 the sql statement
     *
     * @return whether the SQL is executed successfully
     *
     * @throws Exception if running pushdown fails
     */
    void executeUpdate(String sql, String project) throws Exception;

    String getName();

}
