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

import static org.apache.kylin.common.exception.code.ErrorCodeSystem.EPOCH_DOES_NOT_BELONG_TO_CURRENT_NODE;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.metadata.JdbcMetadataStore;
import org.apache.kylin.common.persistence.metadata.MetadataStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.persistence.transaction.UnitOfWorkParams;
import org.apache.kylin.metadata.epoch.EpochManager;
import org.apache.kylin.metadata.epoch.EpochNotMatchException;

import lombok.val;

public class EnhancedUnitOfWork {

    public static <T> T doInTransactionWithCheckAndRetry(UnitOfWork.Callback<T> f, String unitName) {
        return doInTransactionWithCheckAndRetry(f, UnitOfWork.DEFAULT_EPOCH_ID, unitName);
    }

    public static <T> T doInTransactionWithCheckAndRetry(UnitOfWork.Callback<T> f, long epochId, String unitName) {
        return doInTransactionWithCheckAndRetry(f, unitName, UnitOfWork.DEFAULT_MAX_RETRY, epochId);
    }

    public static <T> T doInTransactionWithCheckAndRetry(UnitOfWork.Callback<T> f, String unitName, int retryTimes) {
        return doInTransactionWithCheckAndRetry(f, unitName, retryTimes, UnitOfWork.DEFAULT_EPOCH_ID);
    }

    public static <T> T doInTransactionWithCheckAndRetry(UnitOfWork.Callback<T> f, String unitName, int retryTimes,
            long epochId) {
        return doInTransactionWithCheckAndRetry(f, unitName, retryTimes, epochId, null);
    }

    public static <T> T doInTransactionWithCheckAndRetry(UnitOfWork.Callback<T> f, String unitName, int retryTimes,
            long epochId, String tempLockName) {
        return doInTransactionWithCheckAndRetry(UnitOfWorkParams.<T> builder().processor(f).unitName(unitName)
                .epochId(epochId).maxRetry(retryTimes).tempLockName(tempLockName).build());
    }

    public static <T> T doInTransactionWithCheckAndRetry(UnitOfWorkParams<T> params) {
        val config = KylinConfig.getInstanceFromEnv();
        MetadataStore metadataStore = ResourceStore.getKylinMetaStore(config).getMetadataStore();
        if (!config.isUTEnv() && metadataStore instanceof JdbcMetadataStore) {
            params.setEpochChecker(() -> {
                if (!EpochManager.getInstance().checkEpochOwner(params.getUnitName())) {
                    throw new EpochNotMatchException(EPOCH_DOES_NOT_BELONG_TO_CURRENT_NODE, params.getUnitName());
                }
                return null;
            });
        }
        return UnitOfWork.doInTransactionWithRetry(params);
    }
}
