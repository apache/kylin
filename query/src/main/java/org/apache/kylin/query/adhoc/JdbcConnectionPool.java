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

package org.apache.kylin.query.adhoc;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;

import org.apache.commons.pool.impl.GenericObjectPool;

public class JdbcConnectionPool implements Closeable {

    private GenericObjectPool internalPool = null;

    public void createPool(JdbcConnectionFactory factory, GenericObjectPool.Config poolConfig) throws IOException {
        if (this.internalPool != null)
            this.close();
        this.internalPool = new GenericObjectPool(factory, poolConfig);
    }

    public Connection getConnection() {

        try {
            return (Connection) internalPool.borrowObject();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void returnConnection(Connection conn) {
        if (conn != null) {
            try {
                internalPool.returnObject(conn);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }

    public void invalidateConnection(Connection conn) {
        if (conn != null)
            try {
                internalPool.invalidateObject(conn);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
    }

    @Override
    public void close() throws IOException {
        try {
            this.internalPool.close();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}