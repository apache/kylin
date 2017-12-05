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

package org.apache.kylin.storage.druid.common;

import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.mysql.jdbc.exceptions.MySQLTransientException;
import io.druid.java.util.common.RetryUtils;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.kylin.common.KylinConfig;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.exceptions.DBIException;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.concurrent.Callable;

public class MySQLConnector {
    protected static final Logger logger = LoggerFactory.getLogger(MySQLConnector.class);

    private final DBI dbi;
    private final Predicate<Throwable> shouldRetry;

    public MySQLConnector() {
        final BasicDataSource datasource = getDatasource();
        // MySQL driver is classloader isolated as part of the extension
        // so we need to help JDBC find the driver
        datasource.setDriverClassLoader(getClass().getClassLoader());
        datasource.setDriverClassName("com.mysql.jdbc.Driver");

        // use double-quotes for quoting columns, so we can write SQL that works with most databases
        datasource.setConnectionInitSqls(ImmutableList.of("SET sql_mode='ANSI_QUOTES'"));

        this.dbi = new DBI(datasource);

        this.shouldRetry = new Predicate<Throwable>() {
            @Override
            public boolean apply(Throwable e) {
                return isTransientException(e);
            }
        };

        logger.info("Configured MySQL as metadata storage");
    }

    public <T> T retryTransaction(final TransactionCallback<T> callback, final int quietTries, final int maxTries) {
        final Callable<T> call = new Callable<T>() {
            @Override
            public T call() throws Exception {
                return getDBI().inTransaction(callback);
            }
        };
        try {
            return RetryUtils.retry(call, shouldRetry, quietTries, maxTries);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public final boolean isTransientException(Throwable e) {
        if (e == null) {
            return false;
        }

        if (e instanceof RetryTransactionException) {
            return true;
        }

        if (e instanceof SQLTransientException) {
            return true;
        }

        if (e instanceof SQLRecoverableException) {
            return true;
        }
        if (e instanceof UnableToObtainConnectionException) {
            return true;
        }

        if (e instanceof UnableToExecuteStatementException) {
            return true;
        }

        if (connectorIsTransientException(e)) {
            return true;
        }

        if (e instanceof SQLException && isTransientException(e.getCause())) {
            return true;
        }

        if (e instanceof DBIException && isTransientException(e.getCause())) {
            return true;
        }

        return false;
    }

    protected boolean connectorIsTransientException(Throwable e) {
        return e instanceof MySQLTransientException || (e instanceof SQLException && ((SQLException) e).getErrorCode() == 1317 /* ER_QUERY_INTERRUPTED */);
    }

    public DBI getDBI() {
        return dbi;
    }

    private BasicDataSource getDatasource() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUsername(config.getDruidMysqlUser());
        dataSource.setPassword(config.getDruidMysqlPassword());
        dataSource.setUrl(config.getDruidMysqlUrl());

        dataSource.setValidationQuery(getValidationQuery());
        dataSource.setTestOnBorrow(true);

        return dataSource;
    }

    private String getValidationQuery() {
        return "SELECT 1";
    }
}
