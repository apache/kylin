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

import java.util.List;
import java.util.Objects;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;

public abstract class EpochStore {
    public static final String EPOCH_SUFFIX = "_epoch";

    public abstract void update(Epoch epoch);

    public abstract void insert(Epoch epoch);

    public abstract void updateBatch(List<Epoch> epochs);

    public abstract void insertBatch(List<Epoch> epochs);

    public abstract Epoch getEpoch(String epochTarget);

    public abstract List<Epoch> list();

    public abstract void delete(String epochTarget);

    public abstract void createIfNotExist() throws Exception;

    public abstract <T> T executeWithTransaction(Callback<T> callback);

    public Epoch getGlobalEpoch() {
        return getEpoch("_global");
    }

    public static EpochStore getEpochStore(KylinConfig config) throws Exception {
        EpochStore epochStore = Singletons.getInstance(EpochStore.class, clz -> {
            if (Objects.equals(config.getMetadataUrl().getScheme(), "jdbc")) {
                return JdbcEpochStore.getEpochStore(config);
            } else {
                return FileEpochStore.getEpochStore(config);
            }
        });

        if (!config.isMetadataOnlyForRead()) {
            epochStore.createIfNotExist();
        }
        return epochStore;
    }

    public interface Callback<T> {
        T handle() throws Exception;

        default void onError() {
            // do nothing by default
        }
    }
}
