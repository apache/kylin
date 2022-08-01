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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;

public class SingleSqlNodeReplacer extends SqlShuttle {

    private final ConvMaster convMaster;
    private SqlNode sqlNodeTryToFind;
    private SqlNode sqlNodeToReplace;

    SingleSqlNodeReplacer(ConvMaster convMaster) {
        this.convMaster = convMaster;
    }

    public void setSqlNodeTryToFind(SqlNode sqlNodeTryToFind) {
        this.sqlNodeTryToFind = sqlNodeTryToFind;
    }

    public void setSqlNodeToReplace(SqlNode sqlNodeToReplace) {
        this.sqlNodeToReplace = sqlNodeToReplace;
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
        return convMaster.checkNodeEqual(sqlNodeTryToFind, id) ? sqlNodeToReplace : super.visit(id);
    }

}
