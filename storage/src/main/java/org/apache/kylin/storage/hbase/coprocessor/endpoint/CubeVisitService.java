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

package org.apache.kylin.storage.hbase.coprocessor.endpoint;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.io.IOUtils;
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
import org.apache.kylin.common.util.CompressionUtils;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.util.KryoUtils;
import org.apache.kylin.gridtable.GTRecord;
import org.apache.kylin.gridtable.GTScanRequest;
import org.apache.kylin.gridtable.IGTScanner;
import org.apache.kylin.gridtable.IGTStore;
import org.apache.kylin.storage.cube.CellListIterator;
import org.apache.kylin.storage.cube.CubeHBaseRPC;
import org.apache.kylin.storage.cube.HBaseReadonlyStore;
import org.apache.kylin.storage.cube.RawScan;
import org.apache.kylin.storage.hbase.coprocessor.endpoint.generated.CubeVisitProtos;
import org.apache.kylin.storage.hbase.coprocessor.endpoint.generated.CubeVisitProtos.CubeVisitResponse.Builder;
import org.apache.kylin.storage.hbase.coprocessor.endpoint.generated.CubeVisitProtos.CubeVisitResponse.Stats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

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

    @Override
    public void visitCube(RpcController controller, CubeVisitProtos.CubeVisitRequest request, RpcCallback<CubeVisitProtos.CubeVisitResponse> done) {
        RegionScanner innerScanner = null;
        HRegion region = null;

        try {
            this.serviceStartTime = System.currentTimeMillis();

            GTScanRequest scanReq = KryoUtils.deserialize(request.getGtScanRequest().toByteArray(), GTScanRequest.class);
            RawScan hbaseRawScan = KryoUtils.deserialize(request.getHbaseRawScan().toByteArray(), RawScan.class);
            //TODO: rewrite own start/end
            Scan scan = CubeHBaseRPC.buildScan(hbaseRawScan);

            region = env.getRegion();
            region.startRegionOperation();

            innerScanner = region.getScanner(scan);
            InnerScannerAsIterator cellListIterator = new InnerScannerAsIterator(innerScanner);

            IGTStore store = new HBaseReadonlyStore(cellListIterator, scanReq, hbaseRawScan.hbaseColumns);
            IGTScanner rawScanner = store.scan(scanReq);
            IGTScanner finalScanner = scanReq.decorateScanner(rawScanner);

            ByteBuffer buffer = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(RowConstants.ROWVALUE_BUFFER_SIZE);
            for (GTRecord oneRecord : finalScanner) {
                buffer.clear();
                oneRecord.exportAllColumns(buffer);
                buffer.flip();
                outputStream.write(buffer.array(), buffer.arrayOffset() - buffer.position(), buffer.remaining());
            }
            //outputStream.close() is not necessary
            byte[] allRows = outputStream.toByteArray();
            Builder responseBuilder = CubeVisitProtos.CubeVisitResponse.newBuilder();
            done.run(responseBuilder.//
                    setCompressedRows(ByteString.copyFrom(CompressionUtils.compress(allRows))).//too many array copies 
                    setStats(Stats.newBuilder().//
                            setAggregatedRowCount(0).//
                            setScannedRowCount(0).//
                            setServiceStartTime(serviceStartTime).//
                            setServiceEndTime(System.currentTimeMillis()).build()).//
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
