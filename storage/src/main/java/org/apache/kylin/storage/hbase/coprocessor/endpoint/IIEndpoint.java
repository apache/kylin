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

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import it.uniroma3.mat.extendedset.intset.ConciseSet;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.kylin.cube.kv.RowKeyColumnIO;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.invertedindex.index.RawTableRecord;
import org.apache.kylin.invertedindex.index.Slice;
import org.apache.kylin.invertedindex.index.TableRecordInfoDigest;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.invertedindex.model.IIKeyValueCodec;
import org.apache.kylin.metadata.measure.MeasureAggregator;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.filter.BitMapFilterEvaluator;
import org.apache.kylin.storage.hbase.coprocessor.*;
import org.apache.kylin.storage.hbase.coprocessor.endpoint.generated.IIProtos;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by honma on 11/7/14.
 */
public class IIEndpoint extends IIProtos.RowsService implements Coprocessor, CoprocessorService {

    private RegionCoprocessorEnvironment env;

    public IIEndpoint() {
    }

    private Scan buildScan() {
        Scan scan = new Scan();
        scan.addColumn(IIDesc.HBASE_FAMILY_BYTES, IIDesc.HBASE_QUALIFIER_BYTES);
        scan.addColumn(IIDesc.HBASE_FAMILY_BYTES, IIDesc.HBASE_DICTIONARY_BYTES);

        return scan;
    }

    //TODO: protobuf does not provide built-in compression
    @Override
    public void getRows(RpcController controller, IIProtos.IIRequest request, RpcCallback<IIProtos.IIResponse> done) {

        RegionScanner innerScanner = null;
        HRegion region = null;

        try {
            region = env.getRegion();
            region.startRegionOperation();
            innerScanner = region.getScanner(buildScan());

            CoprocessorRowType type;
            CoprocessorProjector projector;
            EndpointAggregators aggregators;
            CoprocessorFilter filter;

            type = CoprocessorRowType.deserialize(request.getType().toByteArray());
            projector = CoprocessorProjector.deserialize(request.getProjector().toByteArray());
            aggregators = EndpointAggregators.deserialize(request.getAggregator().toByteArray());
            filter = CoprocessorFilter.deserialize(request.getFilter().toByteArray());

            IIProtos.IIResponse response = getResponse(innerScanner, type, projector, aggregators, filter);
            done.run(response);
        } catch (IOException ioe) {
            System.out.println(ioe.toString());
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

    public IIProtos.IIResponse getResponse(RegionScanner innerScanner, CoprocessorRowType type, CoprocessorProjector projector, EndpointAggregators aggregators, CoprocessorFilter filter) {

        TableRecordInfoDigest tableRecordInfoDigest = aggregators.getTableRecordInfoDigest();

        IIProtos.IIResponse response;

        synchronized (innerScanner) {
            IIKeyValueCodec codec = new IIKeyValueCodec(tableRecordInfoDigest);
            //TODO pass projector to codec to skip loading columns
            Iterable<Slice> slices = codec.decodeKeyValue(new HbaseServerKVIterator(innerScanner));

            if (aggregators.isEmpty()) {
                response = getNonAggregatedResponse(slices, tableRecordInfoDigest, filter, type);
            } else {
                response = getAggregatedResponse(slices, tableRecordInfoDigest, filter, type, projector, aggregators);
            }
        }
        return response;
    }

    //TODO check current memory checking is good enough
    private IIProtos.IIResponse getAggregatedResponse(Iterable<Slice> slices, TableRecordInfoDigest recordInfo, CoprocessorFilter filter, CoprocessorRowType type, CoprocessorProjector projector, EndpointAggregators aggregators) {
        EndpointAggregationCache aggCache = new EndpointAggregationCache(aggregators);
        IIProtos.IIResponse.Builder responseBuilder = IIProtos.IIResponse.newBuilder();
        final byte[] buffer = new byte[CoprocessorConstants.METRIC_SERIALIZE_BUFFER_SIZE];
        ClearTextDictionary clearTextDictionary = new ClearTextDictionary(recordInfo, type);
        RowKeyColumnIO rowKeyColumnIO = new RowKeyColumnIO(clearTextDictionary);
        byte[] recordBuffer = new byte[recordInfo.getByteFormLen()];
        for (Slice slice : slices) {

            //TODO localdict
            //dictionaries for fact table columns can not be determined while streaming.
            //a piece of dict coincide with each Slice, we call it "local dict"
            final Map<Integer, Dictionary<?>> localDictionaries = slice.getLocalDictionaries();
            CoprocessorFilter newFilter = CoprocessorFilter.fromFilter(new LocalDictionary(localDictionaries, type, slice.getInfo()), filter.getFilter(), FilterDecorator.FilterConstantsTreatment.REPLACE_WITH_LOCAL_DICT);

            ConciseSet result = null;
            if (filter != null) {
                result = new BitMapFilterEvaluator(new SliceBitMapProvider(slice, type)).evaluate(newFilter.getFilter());
            }

            Iterator<RawTableRecord> iterator = slice.iterateWithBitmap(result);
            while (iterator.hasNext()) {
                final RawTableRecord rawTableRecord = iterator.next();
                decodeWithDictionary(recordBuffer, rawTableRecord, localDictionaries, recordInfo, buffer, rowKeyColumnIO, type);
                CoprocessorProjector.AggrKey aggKey = projector.getAggrKey(recordBuffer);
                MeasureAggregator[] bufs = aggCache.getBuffer(aggKey);
                aggregators.aggregate(bufs, recordBuffer);
                aggCache.checkMemoryUsage();
            }
        }

        for (Map.Entry<CoprocessorProjector.AggrKey, MeasureAggregator[]> entry : aggCache.getAllEntries()) {
            CoprocessorProjector.AggrKey aggrKey = entry.getKey();
            IIProtos.IIResponse.IIRow.Builder rowBuilder = IIProtos.IIResponse.IIRow.newBuilder().setColumns(ByteString.copyFrom(aggrKey.get(), aggrKey.offset(), aggrKey.length()));
            int length = aggregators.serializeMetricValues(entry.getValue(), buffer);
            rowBuilder.setMeasures(ByteString.copyFrom(buffer, 0, length));
            responseBuilder.addRows(rowBuilder.build());
        }

        return responseBuilder.build();
    }

    private void decodeWithDictionary(byte[] recordBuffer, RawTableRecord encodedRecord, Map<Integer, Dictionary<?>> localDictionaries, TableRecordInfoDigest digest, byte[] buffer, RowKeyColumnIO rowKeyColumnIO, CoprocessorRowType coprocessorRowType) {
        final TblColRef[] columns = coprocessorRowType.columns;
        final int columnSize = columns.length;
        final boolean[] isMetric = digest.isMetrics();
        for (int i = 0; i < columnSize; i++) {
            final TblColRef column = columns[i];
            if (isMetric[i]) {
                System.arraycopy(encodedRecord.getBytes(), encodedRecord.offset(i), buffer, 0, encodedRecord.length(i));
                rowKeyColumnIO.writeColumn(column, buffer, encodedRecord.length(i), Dictionary.NULL, recordBuffer, digest.offset(i));
            } else {
                final int length = localDictionaries.get(i).getValueBytesFromId(encodedRecord.getValueID(i), buffer, 0);
                rowKeyColumnIO.writeColumn(column, buffer, length, Dictionary.NULL, recordBuffer, digest.offset(i));
            }
        }
    }

    private IIProtos.IIResponse getNonAggregatedResponse(Iterable<Slice> slices, TableRecordInfoDigest recordInfo, CoprocessorFilter filter, CoprocessorRowType type) {
        IIProtos.IIResponse.Builder responseBuilder = IIProtos.IIResponse.newBuilder();
        final byte[] buffer = new byte[CoprocessorConstants.METRIC_SERIALIZE_BUFFER_SIZE];
        ClearTextDictionary clearTextDictionary = new ClearTextDictionary(recordInfo, type);
        RowKeyColumnIO rowKeyColumnIO = new RowKeyColumnIO(clearTextDictionary);
        byte[] recordBuffer = new byte[recordInfo.getByteFormLen()];
        for (Slice slice : slices) {
            CoprocessorFilter newFilter = CoprocessorFilter.fromFilter(new LocalDictionary(slice.getLocalDictionaries(), type, slice.getInfo()), filter.getFilter(), FilterDecorator.FilterConstantsTreatment.REPLACE_WITH_LOCAL_DICT);
            ConciseSet result = null;
            if (filter != null) {
                result = new BitMapFilterEvaluator(new SliceBitMapProvider(slice, type)).evaluate(newFilter.getFilter());
            }

            Iterator<RawTableRecord> iterator = slice.iterateWithBitmap(result);
            while (iterator.hasNext()) {
                final RawTableRecord rawTableRecord = iterator.next();
                decodeWithDictionary(recordBuffer, rawTableRecord, slice.getLocalDictionaries(), recordInfo, buffer, rowKeyColumnIO, type);
                IIProtos.IIResponse.IIRow.Builder rowBuilder = IIProtos.IIResponse.IIRow.newBuilder().setColumns(ByteString.copyFrom(recordBuffer));
                responseBuilder.addRows(rowBuilder.build());
            }
        }

        return responseBuilder.build();
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
