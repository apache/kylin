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

package org.apache.kylin.storage.gtrecord;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.dict.BuiltInFunctionTransformer;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.ScannerWorker;
import org.apache.kylin.metadata.filter.ITupleFilterTransformer;
import org.apache.kylin.metadata.filter.StringCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilterSerializer;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.StorageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CubeSegmentScanner implements IGTScanner {

    private static final Logger logger = LoggerFactory.getLogger(CubeSegmentScanner.class);

    final CubeSegment cubeSeg;
    final ScannerWorker scanner;
    final Cuboid cuboid;

    final GTScanRequest scanRequest;

    public CubeSegmentScanner(CubeSegment cubeSeg, Cuboid cuboid, Set<TblColRef> dimensions, Set<TblColRef> groups, //
            Collection<FunctionDesc> metrics, TupleFilter originalfilter, StorageContext context, String gtStorage) {
        this.cuboid = cuboid;
        this.cubeSeg = cubeSeg;

        //the filter might be changed later in this CubeSegmentScanner (In ITupleFilterTransformer)
        //to avoid issues like in https://issues.apache.org/jira/browse/KYLIN-1954, make sure each CubeSegmentScanner
        //is working on its own copy
        byte[] serialize = TupleFilterSerializer.serialize(originalfilter, StringCodeSystem.INSTANCE);
        TupleFilter filter = TupleFilterSerializer.deserialize(serialize, StringCodeSystem.INSTANCE);
        
        // translate FunctionTupleFilter to IN clause
        ITupleFilterTransformer translator = new BuiltInFunctionTransformer(cubeSeg.getDimensionEncodingMap());
        filter = translator.transform(filter);

        CubeScanRangePlanner scanRangePlanner;
        try {
            scanRangePlanner = new CubeScanRangePlanner(cubeSeg, cuboid, filter, dimensions, groups, metrics, context);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        scanRequest = scanRangePlanner.planScanRequest();
        scanner = new ScannerWorker(cubeSeg, cuboid, scanRequest, gtStorage);
    }

    @Override
    public Iterator<GTRecord> iterator() {
        return scanner.iterator();
    }

    @Override
    public void close() throws IOException {
        scanner.close();
    }

    @Override
    public GTInfo getInfo() {
        return scanRequest == null ? null : scanRequest.getInfo();
    }

    @Override
    public long getScannedRowCount() {
        return scanner.getScannedRowCount();
    }

    public CubeSegment getSegment() {
        return this.cubeSeg;
    }

}
