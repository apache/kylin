/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.storage.hbase;

import java.util.*;

import com.kylinolap.invertedindex.IIInstance;
import com.kylinolap.invertedindex.IISegment;
import com.kylinolap.invertedindex.model.IIDesc;
import com.kylinolap.metadata.MetadataManager;
import com.kylinolap.storage.hbase.coprocessor.endpoint.EndpointTupleIterator;
import org.apache.hadoop.hbase.client.HConnection;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.HBaseConnection;
import com.kylinolap.metadata.model.ColumnDesc;
import com.kylinolap.metadata.model.FunctionDesc;
import com.kylinolap.metadata.model.TblColRef;
import com.kylinolap.storage.IStorageEngine;
import com.kylinolap.storage.StorageContext;
import com.kylinolap.storage.filter.TupleFilter;
import com.kylinolap.storage.tuple.ITupleIterator;

/**
 * @author yangli9
 */
public class InvertedIndexStorageEngine implements IStorageEngine {

    private String hbaseUrl;
    private IISegment seg;
    private ColumnDesc[] columnDescs;

    public InvertedIndexStorageEngine(IIInstance ii) {
        this.seg = ii.getFirstSegment();
        IIDesc cubeDesc = this.seg.getIIDesc();
        this.columnDescs = MetadataManager.getInstance(cubeDesc.getConfig()).getTableDesc(cubeDesc.getFactTableName()).getColumns();
        this.hbaseUrl = KylinConfig.getInstanceFromEnv().getStorageUrl();
    }

    @Override
    public ITupleIterator search(Collection<TblColRef> dimensions, TupleFilter filter, Collection<TblColRef> groups, Collection<FunctionDesc> metrics, StorageContext context) {
        String tableName = seg.getStorageLocationIdentifier();

        //HConnection is cached, so need not be closed
        HConnection conn = HBaseConnection.get(context.getConnUrl());
        try {
            return new EndpointTupleIterator(seg, columnDescs, filter, groups, new ArrayList(metrics), context, conn);
        } catch (Throwable e) {
            e.printStackTrace();
            throw new IllegalStateException("Error when connecting to II htable " + tableName, e);
        }
    }

}
