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
package org.apache.kylin.common.persistence.metadata;

import static org.springframework.transaction.TransactionDefinition.ISOLATION_REPEATABLE_READ;
import static org.springframework.transaction.TransactionDefinition.TIMEOUT_DEFAULT;

import org.apache.kylin.common.persistence.transaction.ITransactionManager;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import lombok.val;

public class JdbcTransactionHelper implements ITransactionManager {

    private final DataSourceTransactionManager transactionManager;
    public JdbcTransactionHelper(DataSourceTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    @Override
    public TransactionStatus getTransaction() throws TransactionException {
        val definition = new DefaultTransactionDefinition();
        definition.setIsolationLevel(ISOLATION_REPEATABLE_READ);
        definition.setTimeout(TIMEOUT_DEFAULT);
        TransactionStatus status = transactionManager.getTransaction(definition);
        if (!status.isNewTransaction()) {
            throw new IllegalTransactionStateException("Expect an new transaction here. Please check the code if "
                    + "the UnitOfWork.doInTransactionWithRetry() is wrapped by JdbcUtil.withTransaction()");
        }
        return status;
    }

    @Override
    public void commit(TransactionStatus status) throws TransactionException {
        transactionManager.commit(status);
    }

    @Override
    public void rollback(TransactionStatus status) throws TransactionException {
        transactionManager.rollback(status);
    }
}
