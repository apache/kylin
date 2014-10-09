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

package com.kylinolap.storage.hbase.coprocessor;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.cuboid.Cuboid;
import com.kylinolap.cube.kv.RowValueDecoder;
import com.kylinolap.metadata.model.cube.TblColRef;
import com.kylinolap.storage.StorageContext;
import com.kylinolap.storage.filter.TupleFilter;

/**
 * @author yangli9
 */
public class CoprocessorEnabler {

    private static final Logger logger = LoggerFactory.getLogger(CoprocessorEnabler.class);

    static final String FORCE_COPROCESSOR = "forceCoprocessor";
    static final boolean DEBUG_LOCAL_COPROCESSOR = false;
    static final int SERIALIZE_BUFFER_SIZE = 65536;
    static final Map<String, Boolean> CUBE_OVERRIDES = Maps.newConcurrentMap();

    public static ResultScanner scanWithCoprocessorIfBeneficial(CubeSegment segment, Cuboid cuboid,
            TupleFilter tupleFiler, Collection<TblColRef> groupBy,
            Collection<RowValueDecoder> rowValueDecoders, StorageContext context, HTableInterface table,
            Scan scan) throws IOException {

        if (!isCoprocessorBeneficial(segment.getCubeInstance(), groupBy, rowValueDecoders, cuboid, context)) {
            return table.getScanner(scan);
        }

        SRowType type = SRowType.fromCuboid(segment, cuboid);
        SRowFilter filter = SRowFilter.fromFilter(segment, tupleFiler);
        SRowProjector projector = SRowProjector.fromColumns(segment, cuboid, groupBy);
        SRowAggregators aggrs = SRowAggregators.fromValuDecoders(rowValueDecoders);

        if (DEBUG_LOCAL_COPROCESSOR) {
            RegionScanner innerScanner = new RegionScannerAdapter(table.getScanner(scan));
            AggregationScanner aggrScanner =
                    new AggregationScanner(type, filter, projector, aggrs, innerScanner);
            return new ResultScannerAdapter(aggrScanner);
        } else {
            scan.setAttribute(AggregateRegionObserver.COPROCESSOR_ENABLE, new byte[] { 0x01 });
            scan.setAttribute(AggregateRegionObserver.TYPE, SRowType.serialize(type));
            scan.setAttribute(AggregateRegionObserver.PROJECTOR, SRowProjector.serialize(projector));
            scan.setAttribute(AggregateRegionObserver.AGGREGATORS, SRowAggregators.serialize(aggrs));
            scan.setAttribute(AggregateRegionObserver.FILTER, SRowFilter.serialize(filter));
            return table.getScanner(scan);
        }
    }

    private static boolean isCoprocessorBeneficial(CubeInstance cube, Collection<TblColRef> groupBy,
            Collection<RowValueDecoder> rowValueDecoders, Cuboid cuboid, StorageContext context) {

        if (context.isAvoidAggregation()) {
            logger.info("Coprocessor is disabled because context tells to avoid aggregation");
            return false;
        }

        String forceFlag = System.getProperty(FORCE_COPROCESSOR);
        if (forceFlag != null) {
            return Boolean.parseBoolean(forceFlag);
        }

        Boolean cubeOverride = CUBE_OVERRIDES.get(cube.getName());
        if (cubeOverride != null) {
            return cubeOverride.booleanValue();
        }

        if (RowValueDecoder.hasMemHungryCountDistinct(rowValueDecoders)) {
            logger.info("Coprocessor is disabled because there is memory hungry count distinct");
            return false;
        }

        HashSet<TblColRef> aggrCols = new HashSet<TblColRef>(cuboid.getColumns());
        aggrCols.removeAll(groupBy);
        if (aggrCols.isEmpty()) {
            logger.info("Coprocessor is disabled because cuboid is exactly the scan group by");
            return false;
        }

        logger.info("Coprocessor is enabled to aggregate " + aggrCols);
        return true;
    }

    @SuppressWarnings("unused")
    private static int getBitsToScan(byte[] startKey, byte[] stopKey) {
        // find the first bit difference from the beginning
        int totalBits = startKey.length * 8;
        int bitsToScan = totalBits;
        for (int i = 0; i < totalBits; i++) {
            int byteIdx = i / 8;
            int bitIdx = 7 - i % 8;
            byte bitMask = (byte) (1 << bitIdx);
            if ((startKey[byteIdx] & bitMask) == (stopKey[byteIdx] & bitMask))
                bitsToScan--;
            else
                break;
        }
        return bitsToScan;
    }

    public static void forceCoprocessorOn() {
        System.setProperty(FORCE_COPROCESSOR, "true");
    }

    public static void forceCoprocessorOff() {
        System.setProperty(FORCE_COPROCESSOR, "false");
    }

    public static String getForceCoprocessor() {
        return System.getProperty(FORCE_COPROCESSOR);
    }

    public static void forceCoprocessorUnset() {
        System.clearProperty(FORCE_COPROCESSOR);
    }

    public static void updateCubeOverride(String cubeName, String force) {
        if ("null".equalsIgnoreCase(force) || "default".equalsIgnoreCase(force)) {
            CUBE_OVERRIDES.remove(cubeName);
        } else if ("true".equalsIgnoreCase(force)) {
            CUBE_OVERRIDES.put(cubeName, Boolean.TRUE);
        } else if ("false".equalsIgnoreCase(force)) {
            CUBE_OVERRIDES.put(cubeName, Boolean.FALSE);
        }
    }

    public static Map<String, Boolean> getCubeOverrides() {
        return CUBE_OVERRIDES;
    }

}
