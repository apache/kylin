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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.ImmutableBitSet;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.gridtable.CubeGridTable;
import org.apache.kylin.cube.gridtable.CuboidToGridTableMapping;
import org.apache.kylin.cube.gridtable.NotEnoughGTInfoException;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.TupleFilterDictionaryTranslater;
import org.apache.kylin.gridtable.EmptyGTScanner;
import org.apache.kylin.gridtable.GTInfo;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRange;
import org.apache.kylin.gridtable.GTScanRangePlanner;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.GTUtil;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.metadata.filter.ITupleFilterTranslator;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class CubeSegmentScanner implements IGTScanner {

    private static final Logger logger = LoggerFactory.getLogger(CubeSegmentScanner.class);

    private static final int MAX_SCAN_RANGES = 200;

    final CubeSegment cubeSeg;
    final GTInfo info;
    final List<GTScanRequest> scanRequests;
    final Scanner scanner;
    final Cuboid cuboid;

    public CubeSegmentScanner(CubeSegment cubeSeg, Cuboid cuboid, Set<TblColRef> dimensions, Set<TblColRef> groups, //
            Collection<FunctionDesc> metrics, TupleFilter filter, boolean allowPreAggregate) throws NotEnoughGTInfoException {
        this.cuboid = cuboid;
        this.cubeSeg = cubeSeg;
        this.info = CubeGridTable.newGTInfo(cubeSeg, cuboid.getId());

        CuboidToGridTableMapping mapping = cuboid.getCuboidToGridTableMapping();

        // translate FunctionTupleFilter to IN clause
        ITupleFilterTranslator translator = new TupleFilterDictionaryTranslater(this.cubeSeg);
        filter = translator.translate(filter);

        //replace the constant values in filter to dictionary codes 
        TupleFilter gtFilter = GTUtil.convertFilterColumnsAndConstants(filter, info, mapping.getCuboidDimensionsInGTOrder(), groups);

        ImmutableBitSet gtDimensions = makeGridTableColumns(mapping, dimensions);
        ImmutableBitSet gtAggrGroups = makeGridTableColumns(mapping, replaceDerivedColumns(groups, cubeSeg.getCubeDesc()));
        ImmutableBitSet gtAggrMetrics = makeGridTableColumns(mapping, metrics);
        String[] gtAggrFuncs = makeAggrFuncs(mapping, metrics);

        GTScanRangePlanner scanRangePlanner;
        if (cubeSeg.getCubeDesc().getModel().getPartitionDesc().isPartitioned()) {
            TblColRef tblColRef = cubeSeg.getCubeDesc().getModel().getPartitionDesc().getPartitionDateColumnRef();
            TblColRef partitionColOfGT = null;
            Pair<ByteArray, ByteArray> segmentStartAndEnd = null;
            int index = mapping.getIndexOf(tblColRef);
            if (index >= 0) {
                segmentStartAndEnd = getSegmentStartAndEnd(index);
                partitionColOfGT = info.colRef(index);
            }
            scanRangePlanner = new GTScanRangePlanner(info, segmentStartAndEnd, partitionColOfGT);
        } else {
            scanRangePlanner = new GTScanRangePlanner(info, null, null);
        }
        List<GTScanRange> scanRanges = scanRangePlanner.planScanRanges(gtFilter, MAX_SCAN_RANGES);

        scanRequests = Lists.newArrayListWithCapacity(scanRanges.size());

        KylinConfig config = cubeSeg.getCubeInstance().getConfig();
        for (GTScanRange range : scanRanges) {
            GTScanRequest req = new GTScanRequest(info, range, gtDimensions, gtAggrGroups, gtAggrMetrics, gtAggrFuncs, gtFilter, allowPreAggregate, config.getQueryCoprocessorMemGB());
            scanRequests.add(req);
        }

        scanner = new Scanner();
    }

    private Pair<ByteArray, ByteArray> getSegmentStartAndEnd(int index) {
        ByteArray start;
        if (cubeSeg.getDateRangeStart() != Long.MIN_VALUE) {
            start = encodeTime(cubeSeg.getDateRangeStart(), index, 1);
        } else {
            start = new ByteArray();
        }

        ByteArray end;
        if (cubeSeg.getDateRangeEnd() != Long.MAX_VALUE) {
            end = encodeTime(cubeSeg.getDateRangeEnd(), index, -1);
        } else {
            end = new ByteArray();
        }
        return Pair.newPair(start, end);

    }

    private ByteArray encodeTime(long ts, int index, int roundingFlag) {
        String value;
        DataType partitionColType = info.getColumnType(index);
        if (partitionColType.isDate()) {
            value = DateFormat.formatToDateStr(ts);
        } else if (partitionColType.isDatetime() || partitionColType.isTimestamp()) {
            value = DateFormat.formatToTimeWithoutMilliStr(ts);
        } else if (partitionColType.isStringFamily()) {
            String partitionDateFormat = cubeSeg.getCubeDesc().getModel().getPartitionDesc().getPartitionDateFormat();
            if (StringUtils.isEmpty(partitionDateFormat))
                partitionDateFormat = DateFormat.DEFAULT_DATE_PATTERN;
            value = DateFormat.formatToDateStr(ts, partitionDateFormat);
        } else {
            throw new RuntimeException("Type " + partitionColType + " is not valid partition column type");
        }

        ByteBuffer buffer = ByteBuffer.allocate(info.getMaxColumnLength());
        info.getCodeSystem().encodeColumnValue(index, value, roundingFlag, buffer);

        return ByteArray.copyOf(buffer.array(), 0, buffer.position());
    }

    private Set<TblColRef> replaceDerivedColumns(Set<TblColRef> input, CubeDesc cubeDesc) {
        Set<TblColRef> ret = Sets.newHashSet();
        for (TblColRef col : input) {
            if (cubeDesc.isDerived(col)) {
                for (TblColRef host : cubeDesc.getHostInfo(col).columns) {
                    ret.add(host);
                }
            } else {
                ret.add(col);
            }
        }
        return ret;
    }

    private ImmutableBitSet makeGridTableColumns(CuboidToGridTableMapping mapping, Set<TblColRef> dimensions) {
        BitSet result = new BitSet();
        for (TblColRef dim : dimensions) {
            int idx = mapping.getIndexOf(dim);
            if (idx >= 0)
                result.set(idx);
        }
        return new ImmutableBitSet(result);
    }

    private ImmutableBitSet makeGridTableColumns(CuboidToGridTableMapping mapping, Collection<FunctionDesc> metrics) {
        BitSet result = new BitSet();
        for (FunctionDesc metric : metrics) {
            int idx = mapping.getIndexOf(metric);
            if (idx < 0)
                throw new IllegalStateException(metric + " not found in " + mapping);
            result.set(idx);
        }
        return new ImmutableBitSet(result);
    }

    private String[] makeAggrFuncs(final CuboidToGridTableMapping mapping, Collection<FunctionDesc> metrics) {

        //metrics are represented in ImmutableBitSet, which loses order information
        //sort the aggrFuns to align with metrics natural order 
        List<FunctionDesc> metricList = Lists.newArrayList(metrics);
        Collections.sort(metricList, new Comparator<FunctionDesc>() {
            @Override
            public int compare(FunctionDesc o1, FunctionDesc o2) {
                int a = mapping.getIndexOf(o1);
                int b = mapping.getIndexOf(o2);
                return a - b;
            }
        });

        String[] result = new String[metricList.size()];
        int i = 0;
        for (FunctionDesc metric : metricList) {
            result[i++] = metric.getExpression();
        }
        return result;
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
        return info;
    }

    @Override
    public int getScannedRowCount() {
        return scanner.getScannedRowCount();
    }

    private class Scanner {
        IGTScanner internal = null;

        public Scanner() {
            CubeHBaseRPC rpc;
            if ("scan".equalsIgnoreCase(BackdoorToggles.getHbaseCubeQueryProtocol())) {
                rpc = new CubeHBaseScanRPC(cubeSeg, cuboid, info);
            } else {
                rpc = new CubeHBaseEndpointRPC(cubeSeg, cuboid, info);//default behavior
            }
            //change previous line to CubeHBaseRPC rpc = new CubeHBaseScanRPC(cubeSeg, cuboid, info);
            //to debug locally

            try {
                if (scanRequests.size() == 0) {
                    logger.info("Segment {} will be skipped", cubeSeg);
                    internal = new EmptyGTScanner();
                } else {
                    internal = rpc.getGTScanner(scanRequests);
                }
            } catch (IOException e) {
                throw new RuntimeException("error", e);
            }
        }

        public Iterator<GTRecord> iterator() {
            return internal.iterator();
        }

        public void close() throws IOException {
            internal.close();
        }

        public int getScannedRowCount() {
            return internal.getScannedRowCount();
        }

    }

}
