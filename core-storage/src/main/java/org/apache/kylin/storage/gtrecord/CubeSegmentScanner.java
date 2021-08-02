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
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.annotation.Clarification;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
//import org.apache.kylin.dict.BuiltInFunctionTransformer;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.metadata.expression.TupleExpression;
//import org.apache.kylin.metadata.filter.ITupleFilterTransformer;
//import org.apache.kylin.metadata.filter.StringCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
//import org.apache.kylin.metadata.filter.TupleFilterSerializer;
import org.apache.kylin.metadata.model.DynamicFunctionDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.StorageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Clarification(deprecated = true, msg = "Only for HBase storage")
public class CubeSegmentScanner implements Iterable<GTRecord> {

    private static final Logger logger = LoggerFactory.getLogger(CubeSegmentScanner.class);

    CubeSegment cubeSeg;
    ScannerWorker scanner;
    Cuboid cuboid;

    GTScanRequest scanRequest;

    public CubeSegmentScanner(CubeSegment cubeSeg, Cuboid cuboid, Set<TblColRef> dimensions, //
            Set<TblColRef> groups, List<TblColRef> dynGroups, List<TupleExpression> dynGroupExprs, //
            Collection<FunctionDesc> metrics, List<DynamicFunctionDesc> dynFuncs, //
            TupleFilter originalfilter, TupleFilter havingFilter, StorageContext context) {

    }

    public boolean isSegmentSkipped() {
        return scanner.isSegmentSkipped();
    }

    @Override
    public Iterator<GTRecord> iterator() {
        return scanner.iterator();
    }

    public void close() throws IOException {
        scanner.close();
    }

    public GTInfo getInfo() {
        return scanRequest == null ? null : scanRequest.getInfo();
    }

    public GTScanRequest getScanRequest() {
        return scanRequest;
    }
}
