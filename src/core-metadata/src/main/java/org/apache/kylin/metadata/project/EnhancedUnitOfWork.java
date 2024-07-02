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
package org.apache.kylin.metadata.project;

import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.persistence.transaction.UnitOfWorkParams;

public class EnhancedUnitOfWork {

    public static <T> T doInTransactionWithCheckAndRetry(UnitOfWork.Callback<T> f, String unitName) {
        return doInTransactionWithCheckAndRetry(f, unitName, UnitOfWork.DEFAULT_MAX_RETRY);
    }

    public static <T> T doInTransactionWithCheckAndRetry(UnitOfWork.Callback<T> f, String unitName, int retryTimes) {
        return doInTransactionWithCheckAndRetry(f, unitName, retryTimes, UnitOfWork.DEFAULT_EPOCH_ID);
    }

    public static <T> T doInTransactionWithCheckAndRetry(UnitOfWork.Callback<T> f, String unitName, int retryTimes,
            long epochId) {
        return doInTransactionWithCheckAndRetry(UnitOfWorkParams.<T> builder().processor(f).unitName(unitName)
                .maxRetry(retryTimes).epochId(epochId).build());
    }

    public static <T> T doInTransactionWithCheckAndRetry(UnitOfWorkParams<T> params) {
        return UnitOfWork.doInTransactionWithRetry(params);
    }
}
