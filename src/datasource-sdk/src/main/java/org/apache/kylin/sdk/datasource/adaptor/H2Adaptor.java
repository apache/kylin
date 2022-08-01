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
package org.apache.kylin.sdk.datasource.adaptor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * Mainly used for unit test.
 */
public class H2Adaptor extends DefaultAdaptor {
    public H2Adaptor(AdaptorConfig config) throws Exception {
        super(config);
    }

    @Override
    public List<String> listDatabases() throws SQLException {
        List<String> allSchemas = super.listDatabases();
        List<String> filtered = new LinkedList<>();
        for (String schema : allSchemas) {
            if (!schema.equals("INFORMATION_SCHEMA"))
                filtered.add(schema);
        }
        return filtered;
    }

    @Override
    public void setDefaultDb(Connection connection, String db) throws SQLException {
        connection.setSchema(db);
    }
}
