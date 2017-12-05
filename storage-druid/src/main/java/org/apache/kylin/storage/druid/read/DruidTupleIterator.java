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

package org.apache.kylin.storage.druid.read;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeDesc.DeriveInfo;
import org.apache.kylin.dict.lookup.ILookupTable;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.druid.DruidSchema;
import org.apache.kylin.storage.druid.read.cursor.RowCursor;

public class DruidTupleIterator implements ITupleIterator {
    private final List<ColumnFiller> fillers;
    private final RowCursor cursor;
    private final Tuple tuple;
    private final StorageContext context;

    private int scanCount;

    public DruidTupleIterator(CubeInstance cube, LookupTableCache lookupCache, DruidSchema schema, TupleInfo tupleInfo, RowCursor cursor, StorageContext context) {
        this.cursor = cursor;
        this.tuple = new Tuple(tupleInfo);
        this.context = context;

        this.fillers = new ArrayList<>();
        fillers.add(new DruidColumnFiller(schema, tupleInfo));

        // derived columns fillers
        List<DeriveInfo> deriveInfos = cube.getDescriptor().getDeriveInfos(schema.getDimensions());

        for (DeriveInfo info : deriveInfos) {
            DerivedIndexMapping mapping = new DerivedIndexMapping(info, schema, tupleInfo);
            if (!mapping.shouldReturnDerived()) {
                continue;
            }
            if (info.type == CubeDesc.DeriveType.LOOKUP) {
                ILookupTable lookupTable = lookupCache.get(info.join);
                fillers.add(new DerivedColumnFiller(mapping, lookupTable));

            } else if (info.type == CubeDesc.DeriveType.PK_FK) {
                fillers.add(new PkColumnFiller(mapping));
            }
        }
    }

    @Override
    public boolean hasNext() {
        return cursor.hasNext();
    }

    @Override
    public ITuple next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        if (scanCount++ >= 100) {
            flushScanCountDelta();
            context.getQueryContext().checkMillisBeforeDeadline();
        }

        Object[] row = cursor.next();
        for (ColumnFiller filler : fillers) {
            filler.fill(row, tuple);
        }
        return tuple;
    }

    @Override
    public void close() {
        flushScanCountDelta();
        IOUtils.closeQuietly(cursor);
    }

    private void flushScanCountDelta() {
        context.increaseProcessedRowCount(scanCount);
        scanCount = 0;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }
}
