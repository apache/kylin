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

package org.apache.kylin.query.enumerator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.query.relnode.OLAPContext;

/**
 * Hive Query Result Enumerator
 */
public class HiveEnumerator implements Enumerator<Object[]> {

    private final OLAPContext olapContext;
    private final Object[] current;
    private ResultSet rs;
    private Connection conn;

    public HiveEnumerator(OLAPContext olapContext) {
        this.olapContext = olapContext;
        this.current = new Object[olapContext.returnTupleInfo.size()];
    }

    @Override
    public Object[] current() {
        return current;
    }

    @Override
    public boolean moveNext() {
        if (rs == null) {
            rs = executeQuery();
        }
        return populateResult();
    }

    private ResultSet executeQuery() {
        String url = olapContext.olapSchema.getStarSchemaUrl();
        String user = olapContext.olapSchema.getStarSchemaUser();
        String pwd = olapContext.olapSchema.getStarSchemaPassword();
        String sql = olapContext.sql;
        Statement stmt = null;
        try {
            conn = DriverManager.getConnection(url, user, pwd);
            stmt = conn.createStatement();
            return stmt.executeQuery(sql);
        } catch (SQLException e) {
            throw new IllegalStateException(url + " can't execute query " + sql, e);
        } finally {
            DBUtils.closeQuietly(stmt);
            DBUtils.closeQuietly(conn);
        }
    }

    private boolean populateResult() {
        try {
            boolean hasNext = rs.next();
            if (hasNext) {
                List<String> allFields = olapContext.returnTupleInfo.getAllFields();
                for (int i = 0; i < allFields.size(); i++) {
                    Object value = rs.getObject(allFields.get(i).toLowerCase(Locale.ROOT));
                    current[i] = value;
                }
            }
            return hasNext;
        } catch (SQLException e) {
            throw new IllegalStateException("Can't populate result!", e);
        }
    }

    @Override
    public void reset() {
        close();
        rs = executeQuery();
    }

    @Override
    public void close() {
        try {
            if (rs != null) {
                rs.close();
                rs = null;
            }
            if (conn != null) {
                conn.close();
                conn = null;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Can't close ResultSet!", e);
        }
    }

}
