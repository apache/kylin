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

package org.apache.kylin.storage.hbase.cube.v2;

import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.tuple.Tuple;
import org.apache.kylin.storage.tuple.TupleInfo;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by shaoshi on 10/28/15.
 */
public class SequentialCubeTopNTupleIterator extends SequentialCubeTupleIterator {

    private Iterator<Tuple> innerResultIterator;
    private TblColRef topNCol;

    public SequentialCubeTopNTupleIterator(List<CubeSegmentScanner> scanners, Cuboid cuboid, Set<TblColRef> selectedDimensions, //
            TblColRef topNCol, Set<FunctionDesc> selectedMetrics, TupleInfo returnTupleInfo, StorageContext context) {

        super(scanners, cuboid, selectedDimensions, selectedMetrics, returnTupleInfo, context);
        this.topNCol = topNCol;
    }

    @Override
    public boolean hasNext() {
        if (next != null)
            return true;
        if (innerResultIterator == null) {
            if (curScanner == null) {
                if (scannerIterator.hasNext()) {
                    curScanner = scannerIterator.next();
                    curRecordIterator = curScanner.iterator();
                    curTupleConverter = new CubeTupleConverter(curScanner.cubeSeg, cuboid, selectedDimensions, selectedMetrics, tupleInfo, topNCol);
                } else {
                    return false;
                }
            }
            
            if (curRecordIterator.hasNext()) {
                innerResultIterator = curTupleConverter.translateTopNResult(curRecordIterator.next(), tuple);
                return hasNext();
            } else {
                close(curScanner);
                curScanner = null;
                curRecordIterator = null;
                curTupleConverter = null;
                innerResultIterator = null;
                return hasNext();
            }

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
