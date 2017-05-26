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

package org.apache.kylin.storage.translate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.AbstractRowKeyEncoder;
import org.apache.kylin.cube.kv.FuzzyKeyEncoder;
import org.apache.kylin.cube.kv.FuzzyMaskEncoder;
import org.apache.kylin.cube.kv.LazyRowKeyEncoder;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * @author xjiang
 */
public class HBaseKeyRange implements Comparable<HBaseKeyRange> {

    private static final Logger logger = LoggerFactory.getLogger(HBaseKeyRange.class);

    private static final int FUZZY_VALUE_CAP = 100;
    private static final byte[] ZERO_TAIL_BYTES = new byte[] { 0 };

    private final CubeSegment cubeSeg;
    private final Cuboid cuboid;
    private final List<Collection<ColumnValueRange>> flatOrAndFilter; // OR-AND filter, (A AND B AND ..) OR (C AND D AND ..) OR ..

    private byte[] startKey;
    private byte[] stopKey;
    private List<Pair<byte[], byte[]>> fuzzyKeys;

    private String startKeyString;
    private String stopKeyString;
    private String fuzzyKeyString;

    private long partitionColumnStartDate = Long.MIN_VALUE;
    private long partitionColumnEndDate = Long.MAX_VALUE;

    public HBaseKeyRange(CubeSegment cubeSeg, Cuboid cuboid, byte[] startKey, byte[] stopKey, List<Pair<byte[], byte[]>> fuzzyKeys, List<Collection<ColumnValueRange>> flatColumnValueFilter, long partitionColumnStartDate, long partitionColumnEndDate) {
        this.cubeSeg = cubeSeg;
        this.cuboid = cuboid;
        this.startKey = startKey;
        this.stopKey = stopKey;
        this.fuzzyKeys = fuzzyKeys;
        this.flatOrAndFilter = flatColumnValueFilter;
        this.partitionColumnStartDate = partitionColumnStartDate;
        this.partitionColumnEndDate = partitionColumnEndDate;
        initDebugString();
    }

    public HBaseKeyRange(Collection<TblColRef> dimensionColumns, Collection<ColumnValueRange> andDimensionRanges, CubeSegment cubeSeg, CubeDesc cubeDesc) {
        this.cubeSeg = cubeSeg;
        long cuboidId = this.calculateCuboidID(cubeDesc, dimensionColumns);
        this.cuboid = Cuboid.findById(cubeDesc, cuboidId);
        this.flatOrAndFilter = Lists.newLinkedList();
        this.flatOrAndFilter.add(andDimensionRanges);
        init(andDimensionRanges);
        initDebugString();
    }

    private long calculateCuboidID(CubeDesc cube, Collection<TblColRef> dimensions) {
        long cuboidID = 0;
        for (TblColRef column : dimensions) {
            int index = cube.getRowkey().getColumnBitIndex(column);
            cuboidID |= 1L << index;
        }
        return cuboidID;
    }

    private void init(Collection<ColumnValueRange> andDimensionRanges) {
        int size = andDimensionRanges.size();
        Map<TblColRef, String> startValues = Maps.newHashMapWithExpectedSize(size);
        Map<TblColRef, String> stopValues = Maps.newHashMapWithExpectedSize(size);
        Map<TblColRef, Set<String>> fuzzyValues = Maps.newHashMapWithExpectedSize(size);
        for (ColumnValueRange dimRange : andDimensionRanges) {
            TblColRef column = dimRange.getColumn();
            startValues.put(column, dimRange.getBeginValue());
            stopValues.put(column, dimRange.getEndValue());
            fuzzyValues.put(column, dimRange.getEqualValues());

            TblColRef partitionDateColumnRef = cubeSeg.getCubeDesc().getModel().getPartitionDesc().getPartitionDateColumnRef();
            if (column.equals(partitionDateColumnRef)) {
                initPartitionRange(dimRange);
            }
        }

        AbstractRowKeyEncoder encoder = new LazyRowKeyEncoder(cubeSeg, cuboid);
        encoder.setBlankByte(RowConstants.ROWKEY_LOWER_BYTE);
        this.startKey = encoder.encode(startValues);
        encoder.setBlankByte(RowConstants.ROWKEY_UPPER_BYTE);
        // In order to make stopRow inclusive add a trailing 0 byte. #See Scan.setStopRow(byte [] stopRow)
        this.stopKey = Bytes.add(encoder.encode(stopValues), ZERO_TAIL_BYTES);

        // always fuzzy match cuboid ID to lock on the selected cuboid
        this.fuzzyKeys = buildFuzzyKeys(fuzzyValues);
    }

    private void initPartitionRange(ColumnValueRange dimRange) {
        if (null != dimRange.getBeginValue()) {
            this.partitionColumnStartDate = DateFormat.stringToMillis(dimRange.getBeginValue());
        }
        if (null != dimRange.getEndValue()) {
            this.partitionColumnEndDate = DateFormat.stringToMillis(dimRange.getEndValue());
        }
    }

    private void initDebugString() {
        this.startKeyString = BytesUtil.toHex(this.startKey);
        this.stopKeyString = BytesUtil.toHex(this.stopKey);
        StringBuilder buf = new StringBuilder();
        for (Pair<byte[], byte[]> fuzzyKey : this.fuzzyKeys) {
            buf.append(BytesUtil.toHex(fuzzyKey.getFirst()));
            buf.append(" ");
            buf.append(BytesUtil.toHex(fuzzyKey.getSecond()));
            buf.append(";");
        }
        this.fuzzyKeyString = buf.toString();
    }

    private List<Pair<byte[], byte[]>> buildFuzzyKeys(Map<TblColRef, Set<String>> fuzzyValueSet) {
        ArrayList<Pair<byte[], byte[]>> result = new ArrayList<Pair<byte[], byte[]>>();

        // debug/profiling purpose
        if (BackdoorToggles.getDisableFuzzyKey()) {
            logger.info("The execution of this query will not use fuzzy key");
            return result;
        }

        FuzzyKeyEncoder fuzzyKeyEncoder = new FuzzyKeyEncoder(cubeSeg, cuboid);
        FuzzyMaskEncoder fuzzyMaskEncoder = new FuzzyMaskEncoder(cubeSeg, cuboid);

        List<Map<TblColRef, String>> fuzzyValues = FuzzyValueCombination.calculate(fuzzyValueSet, FUZZY_VALUE_CAP);
        for (Map<TblColRef, String> fuzzyValue : fuzzyValues) {
            result.add(Pair.newPair(fuzzyKeyEncoder.encode(fuzzyValue), fuzzyMaskEncoder.encode(fuzzyValue)));
        }
        return result;
    }

    public CubeSegment getCubeSegment() {
        return this.cubeSeg;
    }

    public Cuboid getCuboid() {
        return cuboid;
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public byte[] getStopKey() {
        return stopKey;
    }

    public List<Pair<byte[], byte[]>> getFuzzyKeys() {
        return fuzzyKeys;
    }

    public String getStartKeyAsString() {
        return startKeyString;
    }

    public String getStopKeyAsString() {
        return stopKeyString;
    }

    public String getFuzzyKeyAsString() {
        return fuzzyKeyString;
    }

    public List<Collection<ColumnValueRange>> getFlatOrAndFilter() {
        return flatOrAndFilter;
    }

    public long getPartitionColumnStartDate() {
        return partitionColumnStartDate;
    }

    public long getPartitionColumnEndDate() {
        return partitionColumnEndDate;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((cubeSeg == null) ? 0 : cubeSeg.hashCode());
        result = prime * result + ((cuboid == null) ? 0 : cuboid.hashCode());
        result = prime * result + ((fuzzyKeyString == null) ? 0 : fuzzyKeyString.hashCode());
        result = prime * result + ((startKeyString == null) ? 0 : startKeyString.hashCode());
        result = prime * result + ((stopKeyString == null) ? 0 : stopKeyString.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        HBaseKeyRange other = (HBaseKeyRange) obj;
        if (cubeSeg == null) {
            if (other.cubeSeg != null)
                return false;
        } else if (!cubeSeg.equals(other.cubeSeg))
            return false;
        if (cuboid == null) {
            if (other.cuboid != null)
                return false;
        } else if (!cuboid.equals(other.cuboid))
            return false;
        if (fuzzyKeyString == null) {
            if (other.fuzzyKeyString != null)
                return false;
        } else if (!fuzzyKeyString.equals(other.fuzzyKeyString))
            return false;
        if (startKeyString == null) {
            if (other.startKeyString != null)
                return false;
        } else if (!startKeyString.equals(other.startKeyString))
            return false;
        if (stopKeyString == null) {
            if (other.stopKeyString != null)
                return false;
        } else if (!stopKeyString.equals(other.stopKeyString))
            return false;
        return true;
    }

    @Override
    public int compareTo(HBaseKeyRange other) {
        return Bytes.compareTo(this.startKey, other.startKey);
    }

    public boolean hitSegment() {
        return cubeSeg.getDateRangeStart() <= getPartitionColumnEndDate() && cubeSeg.getDateRangeEnd() >= getPartitionColumnStartDate();
    }
}
