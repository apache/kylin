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
package org.apache.kylin.storage.hybrid;

import java.util.List;

import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.CompoundTupleIterator;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.storage.IStorageEngine;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.StorageEngineFactory;

import com.google.common.collect.Lists;

/**
 */
public class HybridStorageEngine implements IStorageEngine {

    private IRealization[] realizations;
    private IStorageEngine[] storageEngines;

    public HybridStorageEngine(HybridInstance hybridInstance) {
        this.realizations = hybridInstance.getRealizations();
        storageEngines = new IStorageEngine[realizations.length];
        for (int i = 0; i < realizations.length; i++) {
            storageEngines[i] = StorageEngineFactory.getStorageEngine(realizations[i]);
        }
    }

    @Override
    public ITupleIterator search(final StorageContext context, final SQLDigest sqlDigest) {
        List<ITupleIterator> tupleIterators = Lists.newArrayList();
        for (int i = 0; i < realizations.length; i++) {
            if (realizations[i].isReady() && realizations[i].isCapable(sqlDigest)) {
                ITupleIterator dataIterator = storageEngines[i].search(context, sqlDigest);
                tupleIterators.add(dataIterator);
            }
        }
        // combine tuple iterator
        return new CompoundTupleIterator(tupleIterators);
    }

}
