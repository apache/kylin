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

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KylinStatement extends AvaticaStatement {

    private static final Logger logger = LoggerFactory.getLogger(KylinStatement.class);

    private String queryId;

    protected KylinStatement(AvaticaConnection connection, StatementHandle h, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability) {
        super(connection, h, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        logger.debug("Query id is set manually to {}", queryId);
        this.queryId = queryId;
    }

    public void resetQueryId() {
        logger.trace("Reset query id");
        this.queryId = null;
    }
}
