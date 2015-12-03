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

package org.apache.kylin.storage.hbase.cube.v1;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.hbase.steps.RowValueDecoder;
import org.apache.kylin.storage.translate.HBaseKeyRange;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 */
public class CubeSegmentTopNTupleIterator extends CubeSegmentTupleIterator{

    private Iterator<Tuple> innerResultIterator;
    
    public CubeSegmentTopNTupleIterator(CubeSegment cubeSeg, List<HBaseKeyRange> keyRanges, HConnection conn, //
                                    Set<TblColRef> dimensions, TupleFilter filter, Set<TblColRef> groupBy, TblColRef topNCol, //
                                    List<RowValueDecoder> rowValueDecoders, StorageContext context, TupleInfo returnTupleInfo) {
        super(cubeSeg, keyRanges, conn, dimensions, filter, groupBy, rowValueDecoders, context, returnTupleInfo);
        this.tupleConverter = new CubeTupleConverter(cubeSeg, cuboid, rowValueDecoders, returnTupleInfo, topNCol);
    }
    
    @Override
    public boolean hasNext() {
        if (next != null)
            return true;


        if (innerResultIterator == null) {
            if (resultIterator == null) {
                if (rangeIterator.hasNext() == false)
                    return false;

                resultIterator = doScan(rangeIterator.next());
            }

            if (resultIterator.hasNext() == false) {
                closeScanner();
                resultIterator = null;
                innerResultIterator = null;
                return hasNext();
            }

            Result result = resultIterator.next();
            scanCount++;
            if (++scanCountDelta >= 1000)
                flushScanCountDelta();
            innerResultIterator = tupleConverter.translateTopNResult(result, oneTuple);
        }

        if (innerResultIterator.hasNext()) {
            next = innerResultIterator.next();
            return true;
        } else {
            innerResultIterator = null;
            return hasNext();
        }
    }

}
