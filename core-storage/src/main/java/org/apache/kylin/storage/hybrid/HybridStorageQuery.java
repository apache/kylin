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
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.StorageFactory;

import com.google.common.collect.Lists;

/**
 */
public class HybridStorageQuery implements IStorageQuery {

    private IRealization[] realizations;
    private IStorageQuery[] storageEngines;

    public HybridStorageQuery(HybridInstance hybridInstance) {
        this.realizations = hybridInstance.getRealizations();
        storageEngines = new IStorageQuery[realizations.length];
        for (int i = 0; i < realizations.length; i++) {
            storageEngines[i] = StorageFactory.createQuery(realizations[i]);
        }
    }

    @Override
    public ITupleIterator search(final StorageContext context, final SQLDigest sqlDigest, final TupleInfo returnTupleInfo) {
        List<ITupleIterator> tupleIterators = Lists.newArrayList();
        for (int i = 0; i < realizations.length; i++) {
            if (realizations[i].isReady() && realizations[i].isCapable(sqlDigest).capable) {
                ITupleIterator dataIterator = storageEngines[i].search(context, sqlDigest, returnTupleInfo);
                tupleIterators.add(dataIterator);
            }
        }
        // combine tuple iterator
        return new CompoundTupleIterator(tupleIterators);
    }

}
