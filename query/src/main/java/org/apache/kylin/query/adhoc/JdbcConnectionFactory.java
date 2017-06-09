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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.pool.PoolableObjectFactory;

@SuppressWarnings("unused")
class JdbcConnectionFactory implements PoolableObjectFactory {

    private final String jdbcUrl;

    private final String driverClass;

    private final String username;

    private final String password;

    public JdbcConnectionFactory(String jdbcUrl, String driverClass, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.driverClass = driverClass;
        this.username = username;
        this.password = password;

        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public Connection makeObject() throws Exception {
        Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
        return connection;
    }

    @Override
    public void activateObject(Object o) throws Exception {

    }

    @Override
    public void passivateObject(Object o) throws Exception {

    }

    @Override
    public void destroyObject(Object pooledObject) throws Exception {

        if (pooledObject instanceof Connection) {
            Connection connection = (Connection) pooledObject;

            if (connection != null)
                connection.close();
        }

    }

    @Override
    public boolean validateObject(Object pooledObject) {
        if (pooledObject instanceof Connection) {
            Connection connection = (Connection) pooledObject;

            if (connection != null) {
                try {
                    return ((!connection.isClosed()) && (connection.isValid(1)));
                } catch (SQLException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            }
        }

        return false;
    }
}
