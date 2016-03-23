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

package org.apache.kylin.storage.hbase.cube.v1.coprocessor.observer;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.gridtable.StorageSideBehavior;
import org.apache.kylin.storage.hbase.common.coprocessor.CoprocessorFilter;
import org.apache.kylin.storage.hbase.common.coprocessor.CoprocessorProjector;
import org.apache.kylin.storage.hbase.common.coprocessor.CoprocessorRowType;
import org.apache.kylin.storage.hbase.common.coprocessor.FilterDecorator;
import org.apache.kylin.storage.hbase.cube.v1.RegionScannerAdapter;
import org.apache.kylin.storage.hbase.cube.v1.ResultScannerAdapter;
import org.apache.kylin.storage.hbase.steps.RowValueDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * @author yangli9
 */
public class ObserverEnabler {

    private static final Logger logger = LoggerFactory.getLogger(ObserverEnabler.class);

    static final String FORCE_COPROCESSOR = "forceObserver";
    static final Map<String, Boolean> CUBE_OVERRIDES = Maps.newConcurrentMap();

    public static ResultScanner scanWithCoprocessorIfBeneficial(CubeSegment segment, Cuboid cuboid, TupleFilter tupleFiler, //
            Collection<TblColRef> groupBy, Collection<RowValueDecoder> rowValueDecoders, StorageContext context, Table table, Scan scan) throws IOException {

        if (context.isCoprocessorEnabled() == false) {
            return table.getScanner(scan);
        }

        CoprocessorRowType type = CoprocessorRowType.fromCuboid(segment, cuboid);
        CoprocessorFilter filter = CoprocessorFilter.fromFilter(segment.getDimensionEncodingMap(), tupleFiler, FilterDecorator.FilterConstantsTreatment.REPLACE_WITH_GLOBAL_DICT);
        CoprocessorProjector projector = CoprocessorProjector.makeForObserver(segment, cuboid, groupBy);
        ObserverAggregators aggrs = ObserverAggregators.fromValueDecoders(rowValueDecoders);

        boolean localCoprocessor = KylinConfig.getInstanceFromEnv().getQueryRunLocalCoprocessor() || BackdoorToggles.getRunLocalCoprocessor();

        if (localCoprocessor) {
            RegionScanner innerScanner = new RegionScannerAdapter(table.getScanner(scan));
            AggregationScanner aggrScanner = new AggregationScanner(type, filter, projector, aggrs, innerScanner, StorageSideBehavior.SCAN_FILTER_AGGR_CHECKMEM);
            return new ResultScannerAdapter(aggrScanner);
        } else {

            // debug/profiling purpose
            String toggle = BackdoorToggles.getCoprocessorBehavior();
            if (toggle == null) {
                toggle = StorageSideBehavior.SCAN_FILTER_AGGR_CHECKMEM.toString(); //default behavior
            } else {
                logger.info("The execution of this query will use " + toggle + " as observer's behavior");
            }

            scan.setAttribute(AggregateRegionObserver.COPROCESSOR_ENABLE, new byte[] { 0x01 });
            scan.setAttribute(AggregateRegionObserver.BEHAVIOR, toggle.getBytes());
            scan.setAttribute(AggregateRegionObserver.TYPE, CoprocessorRowType.serialize(type));
            scan.setAttribute(AggregateRegionObserver.PROJECTOR, CoprocessorProjector.serialize(projector));
            scan.setAttribute(AggregateRegionObserver.AGGREGATORS, ObserverAggregators.serialize(aggrs));
            scan.setAttribute(AggregateRegionObserver.FILTER, CoprocessorFilter.serialize(filter));
            return table.getScanner(scan);
        }
    }

    public static void enableCoprocessorIfBeneficial(CubeInstance cube, Collection<TblColRef> groupBy, Collection<RowValueDecoder> rowValueDecoders, StorageContext context) {
        if (isCoprocessorBeneficial(cube, groupBy, rowValueDecoders, context)) {
            context.enableCoprocessor();
        }
    }

    private static boolean isCoprocessorBeneficial(CubeInstance cube, Collection<TblColRef> groupBy, Collection<RowValueDecoder> rowValueDecoders, StorageContext context) {

        String forceFlag = System.getProperty(FORCE_COPROCESSOR);
        if (forceFlag != null) {
            boolean r = Boolean.parseBoolean(forceFlag);
            logger.info("Coprocessor is " + (r ? "enabled" : "disabled") + " according to sys prop " + FORCE_COPROCESSOR);
            return r;
        }

        Boolean cubeOverride = CUBE_OVERRIDES.get(cube.getName());
        if (cubeOverride != null) {
            boolean r = cubeOverride.booleanValue();
            logger.info("Coprocessor is " + (r ? "enabled" : "disabled") + " according to cube overrides");
            return r;
        }

        if (RowValueDecoder.hasMemHungryMeasures(rowValueDecoders)) {
            logger.info("Coprocessor is disabled because there is memory hungry count distinct");
            return false;
        }

        if (context.isExactAggregation()) {
            logger.info("Coprocessor is disabled because exactAggregation is true");
            return false;
        }

        Cuboid cuboid = context.getCuboid();
        Set<TblColRef> toAggr = Sets.newHashSet(cuboid.getAggregationColumns());
        toAggr.removeAll(groupBy);
        if (toAggr.isEmpty()) {
            logger.info("Coprocessor is disabled because no additional columns to aggregate");
            return false;
        }

        logger.info("Coprocessor is enabled to aggregate " + toAggr + ", returning " + groupBy);
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
