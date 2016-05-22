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
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.mutable.MutableBoolean;
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
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.CompressionUtils;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.IGTStore;
import org.apache.kylin.measure.BufferedMeasureEncoder;
import org.apache.kylin.metadata.filter.UDF.MassInTupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.apache.kylin.storage.hbase.common.coprocessor.CoprocessorBehavior;
import org.apache.kylin.storage.hbase.cube.v2.CellListIterator;
import org.apache.kylin.storage.hbase.cube.v2.CubeHBaseRPC;
import org.apache.kylin.storage.hbase.cube.v2.HBaseReadonlyStore;
import org.apache.kylin.storage.hbase.cube.v2.RawScan;
import org.apache.kylin.storage.hbase.cube.v2.coprocessor.endpoint.generated.CubeVisitProtos;
import org.apache.kylin.storage.hbase.cube.v2.coprocessor.endpoint.generated.CubeVisitProtos.CubeVisitRequest.IntList;
import org.apache.kylin.storage.hbase.cube.v2.filter.MassInValueProviderFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
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

    private List<RawScan> deserializeRawScans(ByteBuffer in) {
        int rawScanCount = BytesUtil.readVInt(in);
        List<RawScan> ret = Lists.newArrayList();
        for (int i = 0; i < rawScanCount; i++) {
            RawScan temp = RawScan.serializer.deserialize(in);
            ret.add(temp);
        }
        return ret;
    }

    private void appendProfileInfo(StringBuilder sb, String info) {
        if (info != null) {
            sb.append(info);
        }
        sb.append("@" + (System.currentTimeMillis() - this.serviceStartTime));
        sb.append(",");
    }

    @Override
    public void visitCube(final RpcController controller, CubeVisitProtos.CubeVisitRequest request, RpcCallback<CubeVisitProtos.CubeVisitResponse> done) {

        List<RegionScanner> regionScanners = Lists.newArrayList();
        HRegion region = null;

        StringBuilder sb = new StringBuilder();
        byte[] allRows;
        String debugGitTag = "";

        try {
            this.serviceStartTime = System.currentTimeMillis();

            region = env.getRegion();
            region.startRegionOperation();
            debugGitTag = region.getTableDesc().getValue(IRealizationConstants.HTableGitTag);

            final GTScanRequest scanReq = GTScanRequest.serializer.deserialize(ByteBuffer.wrap(HBaseZeroCopyByteString.zeroCopyGetBytes(request.getGtScanRequest())));
            List<List<Integer>> hbaseColumnsToGT = Lists.newArrayList();
            for (IntList intList : request.getHbaseColumnsToGTList()) {
                hbaseColumnsToGT.add(intList.getIntsList());
            }
            CoprocessorBehavior behavior = CoprocessorBehavior.valueOf(request.getBehavior());
            final List<RawScan> hbaseRawScans = deserializeRawScans(ByteBuffer.wrap(HBaseZeroCopyByteString.zeroCopyGetBytes(request.getHbaseRawScan())));

            appendProfileInfo(sb, "start latency: " + (this.serviceStartTime - request.getStartTime()));

            MassInTupleFilter.VALUE_PROVIDER_FACTORY = new MassInValueProviderFactoryImpl(new MassInValueProviderFactoryImpl.DimEncAware() {
                @Override
                public DimensionEncoding getDimEnc(TblColRef col) {
                    return scanReq.getInfo().getCodeSystem().getDimEnc(col.getColumnDesc().getZeroBasedIndex());
                }
            });

            final List<InnerScannerAsIterator> cellListsForeachRawScan = Lists.newArrayList();

            for (RawScan hbaseRawScan : hbaseRawScans) {
                if (request.getRowkeyPreambleSize() - RowConstants.ROWKEY_CUBOIDID_LEN > 0) {
                    //if has shard, fill region shard to raw scan start/end
                    updateRawScanByCurrentRegion(hbaseRawScan, region, request.getRowkeyPreambleSize() - RowConstants.ROWKEY_CUBOIDID_LEN);
                }

                Scan scan = CubeHBaseRPC.buildScan(hbaseRawScan);
                RegionScanner innerScanner = region.getScanner(scan);
                regionScanners.add(innerScanner);

                InnerScannerAsIterator cellListIterator = new InnerScannerAsIterator(innerScanner);
                cellListsForeachRawScan.add(cellListIterator);
            }

            final Iterator<List<Cell>> allCellLists = Iterators.concat(cellListsForeachRawScan.iterator());

            if (behavior.ordinal() < CoprocessorBehavior.SCAN.ordinal()) {
                //this is only for CoprocessorBehavior.RAW_SCAN case to profile hbase scan speed
                List<Cell> temp = Lists.newArrayList();
                int counter = 0;
                for (RegionScanner innerScanner : regionScanners) {
                    while (innerScanner.nextRaw(temp)) {
                        counter++;
                    }
                }
                appendProfileInfo(sb, "scanned " + counter);
            }

            if (behavior.ordinal() < CoprocessorBehavior.SCAN_FILTER_AGGR_CHECKMEM.ordinal()) {
                scanReq.setAggrCacheGB(0); // disable mem check if so told
            }

            final MutableBoolean scanNormalComplete = new MutableBoolean(true);
            final long startTime = this.serviceStartTime;
            final long timeout = request.getTimeout();

            final CellListIterator cellListIterator = new CellListIterator() {

                int counter = 0;

                @Override
                public void close() throws IOException {
                    for (CellListIterator closeable : cellListsForeachRawScan) {
                        closeable.close();
                    }
                }

                @Override
                public boolean hasNext() {
                    if (counter % 1000 == 1) {
                        if (System.currentTimeMillis() - startTime > timeout) {
                            scanNormalComplete.setValue(false);
                            logger.error("scanner aborted because timeout");
                            return false;
                        }
                    }

                    if (counter % 100000 == 1) {
                        logger.info("Scanned " + counter + " rows.");
                    }
                    counter++;
                    return allCellLists.hasNext();
                }

                @Override
                public List<Cell> next() {
                    return allCellLists.next();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };

            IGTStore store = new HBaseReadonlyStore(cellListIterator, scanReq, hbaseRawScans.get(0).hbaseColumns, hbaseColumnsToGT, request.getRowkeyPreambleSize());

            IGTScanner rawScanner = store.scan(scanReq);
            IGTScanner finalScanner = scanReq.decorateScanner(rawScanner, //
                    behavior.ordinal() >= CoprocessorBehavior.SCAN_FILTER.ordinal(), //
                    behavior.ordinal() >= CoprocessorBehavior.SCAN_FILTER_AGGR.ordinal());

            ByteBuffer buffer = ByteBuffer.allocate(BufferedMeasureEncoder.DEFAULT_BUFFER_SIZE);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(BufferedMeasureEncoder.DEFAULT_BUFFER_SIZE);//ByteArrayOutputStream will auto grow
            int finalRowCount = 0;
            for (GTRecord oneRecord : finalScanner) {

                if (!scanNormalComplete.booleanValue()) {
                    logger.error("aggregate iterator aborted because input iterator aborts");
                    break;
                }

                if (finalRowCount % 1000 == 1) {
                    if (System.currentTimeMillis() - startTime > timeout) {
                        logger.error("aggregate iterator aborted because timeout");
                        break;
                    }
                }

                buffer.clear();
                try {
                    oneRecord.exportColumns(scanReq.getColumns(), buffer);
                } catch (BufferOverflowException boe) {
                    buffer = ByteBuffer.allocate((int) (oneRecord.sizeOf(scanReq.getColumns()) * 1.5));
                    oneRecord.exportColumns(scanReq.getColumns(), buffer);
                }

                outputStream.write(buffer.array(), 0, buffer.position());
                finalRowCount++;
            }
            finalScanner.close();

            appendProfileInfo(sb, "agg done");

            //outputStream.close() is not necessary
            byte[] compressedAllRows;
            if (scanNormalComplete.booleanValue()) {
                allRows = outputStream.toByteArray();
            } else {
                allRows = new byte[0];
            }
            if (request.hasUseCompression() && request.getUseCompression() == false) {
                compressedAllRows = allRows;
            } else {
                compressedAllRows = CompressionUtils.compress(allRows);
            }

            appendProfileInfo(sb, "compress done");

            OperatingSystemMXBean operatingSystemMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            double systemCpuLoad = operatingSystemMXBean.getSystemCpuLoad();
            double freePhysicalMemorySize = operatingSystemMXBean.getFreePhysicalMemorySize();
            double freeSwapSpaceSize = operatingSystemMXBean.getFreeSwapSpaceSize();

            appendProfileInfo(sb, "server stats done");
            sb.append(" debugGitTag:" + debugGitTag);

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
                            setNormalComplete(scanNormalComplete.booleanValue() ? 1 : 0).build())
                    .//
                    build());

        } catch (IOException ioe) {
            logger.error(ioe.toString());
            IOException wrapped = new IOException("Error in coprocessor " + debugGitTag, ioe);
            ResponseConverter.setControllerException(controller, wrapped);
        } finally {
            for (RegionScanner innerScanner : regionScanners) {
                IOUtils.closeQuietly(innerScanner);
            }
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
