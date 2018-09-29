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

package org.apache.kylin.storage.parquet.spark.gtscanner;

import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTScanRequest;

import java.util.BitSet;
import java.util.Iterator;

public class ParquetRecordGTScanner4Cube extends ParquetRecordGTScanner {
    public ParquetRecordGTScanner4Cube(GTInfo info, Iterator<Object[]> iterator, GTScanRequest scanRequest,
                                       long maxScannedBytes) {
        super(info, iterator, scanRequest, maxScannedBytes);
    }

    protected ImmutableBitSet getParquetCoveredColumns(GTScanRequest scanRequest) {
        BitSet bs = new BitSet();

        ImmutableBitSet dimensions = scanRequest.getInfo().getPrimaryKey();
        for (int i = 0; i < dimensions.trueBitCount(); ++i) {
            bs.set(dimensions.trueBitAt(i));
        }

        ImmutableBitSet queriedColumns = scanRequest.getColumns();
        for (int i = 0; i < queriedColumns.trueBitCount(); ++i) {
            bs.set(queriedColumns.trueBitAt(i));
        }
        return new ImmutableBitSet(bs);
    }

    protected ImmutableBitSet[] getParquetCoveredColumnBlocks(GTScanRequest scanRequest) {
        
        ImmutableBitSet selectedColBlocksBitSet = scanRequest.getSelectedColBlocks();
        
        ImmutableBitSet[] selectedColBlocks = new ImmutableBitSet[selectedColBlocksBitSet.trueBitCount()];
        
        for(int i = 0; i < selectedColBlocksBitSet.trueBitCount(); i++) {
            
            selectedColBlocks[i] = scanRequest.getInfo().getColumnBlock(selectedColBlocksBitSet.trueBitAt(i));
            
        }

        return selectedColBlocks;
    }

}
