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

package org.apache.kylin.query.security;

import org.apache.kylin.query.QueryConnection;
import org.apache.kylin.query.relnode.OLAPContext;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class QueryACLTestUtil {
    public static void setUser(String username) {
        Map<String, String> auth = new HashMap<>();
        auth.put(OLAPContext.PRM_USER_AUTHEN_INFO, username);
        OLAPContext.setParameters(auth);
    }

    public static void mockQuery(String project, String sql) throws SQLException {
        Connection conn = null;
        Statement statement = null;
        try {
            conn = QueryConnection.getConnection(project);
            statement = conn.createStatement();
            statement.executeQuery(sql);
        } catch (SQLException ex) {
            throw ex;
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }
}
