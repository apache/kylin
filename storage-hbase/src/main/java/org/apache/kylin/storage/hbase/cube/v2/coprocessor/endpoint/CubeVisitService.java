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

package org.apache.kylin.storage.hbase.cube.v2.coprocessor.endpoint;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.CompressionUtils;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.IGTStore;
import org.apache.kylin.storage.hbase.common.coprocessor.CoprocessorBehavior;
import org.apache.kylin.storage.hbase.cube.v2.CellListIterator;
import org.apache.kylin.storage.hbase.cube.v2.CubeHBaseRPC;
import org.apache.kylin.storage.hbase.cube.v2.HBaseReadonlyStore;
import org.apache.kylin.storage.hbase.cube.v2.RawScan;
import org.apache.kylin.storage.hbase.cube.v2.coprocessor.endpoint.generated.CubeVisitProtos;
import org.apache.kylin.storage.hbase.cube.v2.coprocessor.endpoint.generated.CubeVisitProtos.CubeVisitRequest.IntList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.protobuf.HBaseZeroCopyByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.sun.management.OperatingSystemMXBean;

@SuppressWarnings("unused")
//used in hbase endpoint
public class CubeVisitService extends CubeVisitProtos.CubeVisitService implements Coprocessor, CoprocessorService {

    private static final Logger logger = LoggerFactory.getLogger(CubeVisitService.class);
    //TODO limit memory footprint
    private static final int MEMORY_LIMIT = 500 * 1024 * 1024;

    private RegionCoprocessorEnvironment env;

    private long serviceStartTime;

    static class InnerScannerAsIterator implements CellListIterator {
        private RegionScanner regionScanner;
        private List<Cell> nextOne = Lists.newArrayList();
        private List<Cell> ret = Lists.newArrayList();

        private boolean hasMore;

        public InnerScannerAsIterator(RegionScanner regionScanner) {
            this.regionScanner = regionScanner;

            try {
                hasMore = regionScanner.nextRaw(nextOne);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean hasNext() {
            return !nextOne.isEmpty();
        }

        @Override
        public List<Cell> next() {

            if (nextOne.size() < 1) {
                throw new IllegalStateException();
            }
            ret.clear();
            ret.addAll(nextOne);
            nextOne.clear();
            try {
                if (hasMore) {
                    hasMore = regionScanner.nextRaw(nextOne);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return ret;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {
            //does not need to close as regionScanner will be closed in finally block
        }
    }

    private void updateRawScanByCurrentRegion(RawScan rawScan, HRegion region, int shardLength) {
        if (shardLength == 0) {
            return;
        }
        byte[] regionStartKey = ArrayUtils.isEmpty(region.getStartKey()) ? new byte[shardLength] : region.getStartKey();
        Bytes.putBytes(rawScan.startKey, 0, regionStartKey, 0, shardLength);
        Bytes.putBytes(rawScan.endKey, 0, regionStartKey, 0, shardLength);
    }

    private void appendProfileInfo(StringBuilder sb) {
        sb.append(System.currentTimeMillis() - this.serviceStartTime);
        sb.append(",");
    }

    @Override
    public void visitCube(RpcController controller, CubeVisitProtos.CubeVisitRequest request, RpcCallback<CubeVisitProtos.CubeVisitResponse> done) {
        RegionScanner innerScanner = null;
        HRegion region = null;

        StringBuilder sb = new StringBuilder();
        byte[] allRows;

        try {
            this.serviceStartTime = System.currentTimeMillis();

            region = env.getRegion();
            region.startRegionOperation();

            GTScanRequest scanReq = GTScanRequest.serializer.deserialize(ByteBuffer.wrap(HBaseZeroCopyByteString.zeroCopyGetBytes(request.getGtScanRequest())));
            RawScan hbaseRawScan = RawScan.serializer.deserialize(ByteBuffer.wrap(HBaseZeroCopyByteString.zeroCopyGetBytes(request.getHbaseRawScan())));

            List<List<Integer>> hbaseColumnsToGT = Lists.newArrayList();
            for (IntList intList : request.getHbaseColumnsToGTList()) {
                hbaseColumnsToGT.add(intList.getIntsList());
            }

            if (request.getRowkeyPreambleSize() - RowConstants.ROWKEY_CUBOIDID_LEN > 0) {
                //if has shard, fill region shard to raw scan start/end
                updateRawScanByCurrentRegion(hbaseRawScan, region, request.getRowkeyPreambleSize() - RowConstants.ROWKEY_CUBOIDID_LEN);
            }
            
            Scan scan = CubeHBaseRPC.buildScan(hbaseRawScan);

            appendProfileInfo(sb);

            innerScanner = region.getScanner(scan);
            CoprocessorBehavior behavior = CoprocessorBehavior.valueOf(request.getBehavior());

            InnerScannerAsIterator cellListIterator = new InnerScannerAsIterator(innerScanner);
            if (behavior.ordinal() < CoprocessorBehavior.SCAN_FILTER_AGGR_CHECKMEM.ordinal()) {
                scanReq.setAggrCacheGB(0); // disable mem check if so told
            }

            IGTStore store = new HBaseReadonlyStore(cellListIterator, scanReq, hbaseRawScan.hbaseColumns, hbaseColumnsToGT, request.getRowkeyPreambleSize());
            IGTScanner rawScanner = store.scan(scanReq);

            IGTScanner finalScanner = scanReq.decorateScanner(rawScanner,//
                    behavior.ordinal() >= CoprocessorBehavior.SCAN_FILTER.ordinal(),//
                    behavior.ordinal() >= CoprocessorBehavior.SCAN_FILTER_AGGR.ordinal());

            ByteBuffer buffer = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(RowConstants.ROWVALUE_BUFFER_SIZE);//ByteArrayOutputStream will auto grow
            int finalRowCount = 0;
            for (GTRecord oneRecord : finalScanner) {
                buffer.clear();
                oneRecord.exportColumns(scanReq.getColumns(), buffer);
                buffer.flip();

                outputStream.write(buffer.array(), buffer.arrayOffset() - buffer.position(), buffer.remaining());
                finalRowCount++;
            }

            appendProfileInfo(sb);

            //outputStream.close() is not necessary
            allRows = outputStream.toByteArray();
            byte[] compressedAllRows = CompressionUtils.compress(allRows);

            appendProfileInfo(sb);

            OperatingSystemMXBean operatingSystemMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            double systemCpuLoad = operatingSystemMXBean.getSystemCpuLoad();
            double freePhysicalMemorySize = operatingSystemMXBean.getFreePhysicalMemorySize();
            double freeSwapSpaceSize = operatingSystemMXBean.getFreeSwapSpaceSize();

            appendProfileInfo(sb);

            CubeVisitProtos.CubeVisitResponse.Builder responseBuilder = CubeVisitProtos.CubeVisitResponse.newBuilder();
            done.run(responseBuilder.//
                    setCompressedRows(HBaseZeroCopyByteString.wrap(compressedAllRows)).//too many array copies 
                    setStats(CubeVisitProtos.CubeVisitResponse.Stats.newBuilder().//
                            setAggregatedRowCount(finalScanner.getScannedRowCount() - finalRowCount).//
                            setScannedRowCount(finalScanner.getScannedRowCount()).//
                            setServiceStartTime(serviceStartTime).//
                            setServiceEndTime(System.currentTimeMillis()).//
                            setSystemCpuLoad(systemCpuLoad).//
                            setFreePhysicalMemorySize(freePhysicalMemorySize).//
                            setFreeSwapSpaceSize(freeSwapSpaceSize).//
                            setHostname(InetAddress.getLocalHost().getHostName()).// 
                            setEtcMsg(sb.toString()).//
                            build()).//
                    build());

        } catch (IOException ioe) {
            logger.error(ioe.toString());
            ResponseConverter.setControllerException(controller, ioe);
        } finally {
            IOUtils.closeQuietly(innerScanner);
            if (region != null) {
                try {
                    region.closeRegionOperation();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
    }

    @Override
    public Service getService() {
        return this;
    }
}
