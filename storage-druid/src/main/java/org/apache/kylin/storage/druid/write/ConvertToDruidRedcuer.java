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

package org.apache.kylin.storage.druid.write;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.collections.bitmap.WrappedImmutableRoaringBitmap;
import io.druid.common.guava.ThreadRenamingRunnable;
import io.druid.concurrent.Execs;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.granularity.NoneGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.data.CompressionFactory;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.timeline.DataSegment;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.CubeDimEncMap;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.engine.mr.KylinReducer;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.measure.MeasureCodec;
import org.apache.kylin.measure.bitmap.RoaringBitmapCounter;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.druid.DruidSchema;
import org.apache.kylin.storage.druid.NameMapping;
import org.apache.kylin.storage.druid.NameMappingFactory;
import org.apache.kylin.storage.druid.common.DruidSerdeHelper;
import org.apache.kylin.storage.druid.common.NumberedShardSpec;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ConvertToDruidRedcuer extends KylinReducer<Text, Text, BytesWritable, Text> {
    protected static final Logger logger = LoggerFactory.getLogger(ConvertToDruidRedcuer.class);
    private static final IndexMergerV9 INDEX_MERGER_V9;
    private static final IndexIO INDEX_IO;
    private static final ObjectMapper JSON_MAPPER = DruidSerdeHelper.JSON_MAPPER;

    static {
        INDEX_IO = new IndexIO(JSON_MAPPER, new ColumnConfig() {
            @Override
            public int columnCacheSizeBytes() {
                return 0;
            }
        });
        INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO);

        //DruidSerdeHelper.registerDruidSerde();
    }

    private CubeDesc cubeDesc;
    private CubeSegment cubeSegment;
    private Map<TblColRef, Integer> dimOrdinals;
    private int[] dimLens;
    private MeasureCodec measureCodec;
    private int[] measureLens;
    private IDimensionEncodingMap dimEncodings;
    private DruidSchema cubeSchema;
    private TimestampSpec timestampSpec;
    private DimensionsSpec dimensionsSpec;
    private IndexSpec indexSpec;
    private InputRowParser<Map<String, Object>> parser;
    private IncrementalIndex index;
    private Interval interval;
    private ListeningExecutorService persistExecutor = null;
    private List<ListenableFuture<?>> persistFutures = Lists.newArrayList();
    private File baseFlushFile;
    private String outputPath;
    private Set<File> toMerge = Sets.newTreeSet();
    private int indexCount = 0;
    private int lineCount = 0;
    private int runningTotalLineCount = 0;

    private final String druidTimeColumn = "_time_";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        initKylinMeta(context);

        initDict();

        initDruidConfig(context);

        initFile();

        initBackThread(cubeSegment.getConfig());
    }

    private void initKylinMeta(Context context) throws IOException {
        final String cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
        final String segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);

        logger.info("cube name = {}, segment id = {}", cubeName, segmentID);

        // KylinConfig was loaded in KylinWriteSupport.init
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        CubeManager manager = CubeManager.getInstance(config);
        CubeInstance cube = manager.getCube(cubeName);
        cubeSegment = cube.getSegmentById(segmentID);
        this.cubeDesc = cube.getDescriptor();

        Cuboid cuboid = Cuboid.getBaseCuboid(cubeDesc);
        NameMapping mapping = NameMappingFactory.getDefault(cubeDesc);
        cubeSchema = new DruidSchema(mapping, cuboid.getColumns(), cubeDesc.getMeasures());
    }

    private void initDict() {
        // determine length of each dimensions
        dimEncodings = new CubeDimEncMap(cubeSegment);
        RowKeyColDesc[] rowkeys = cubeDesc.getRowkey().getRowKeyColumns();
        this.dimOrdinals = new HashMap<>();
        this.dimLens = new int[rowkeys.length];
        for (int i = 0; i < rowkeys.length; i++) {
            TblColRef col = rowkeys[i].getColRef();
            dimOrdinals.put(col, i);
            dimLens[i] = dimEncodings.get(col).getLengthOfEncoding();
        }

        List<MeasureDesc> measures = cubeDesc.getMeasures();
        this.measureCodec = new MeasureCodec(measures);
        this.measureLens = new int[measures.size()];
    }

    private void initDruidConfig(Context context) {
        timestampSpec = new TimestampSpec(druidTimeColumn, null, null);
        dimensionsSpec = new DimensionsSpec(cubeSchema.getDimensionSchemas(), null, null);
        TimeAndDimsParseSpec timeAndDimsParseSpec = new TimeAndDimsParseSpec(timestampSpec, dimensionsSpec);
        parser = new MapInputRowParser(timeAndDimsParseSpec);
        indexSpec = new IndexSpec(new RoaringBitmapSerdeFactory(true), null, null, CompressionFactory.LongEncodingStrategy.AUTO);
        interval = DruidSchema.segmentInterval(cubeSegment);
        index = makeIncrementalIndex(cubeSchema.getAggregators());

        outputPath = context.getConfiguration().get(BatchConstants.CFG_OUTPUT_PATH);
    }

    private void initFile() throws IOException {
        baseFlushFile = File.createTempFile("base", "flush");
        baseFlushFile.delete();
        baseFlushFile.mkdirs();
    }

    private void initBackThread(KylinConfig config) {
        int numBackgroundPersistThreads = config.getDruidReducerThreadsNum();
        logger.info("numBackgroundPersistThreads: " + numBackgroundPersistThreads);
        if (numBackgroundPersistThreads > 0) {
            final BlockingQueue<Runnable> queue = new SynchronousQueue<>();
            ExecutorService executorService = new ThreadPoolExecutor(numBackgroundPersistThreads, numBackgroundPersistThreads, 0L, TimeUnit.MILLISECONDS, queue, Execs.makeThreadFactory("IndexGeneratorJob_persist_%d"), new RejectedExecutionHandler() {
                @Override
                public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                    try {
                        executor.getQueue().put(r);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RejectedExecutionException("Got Interrupted while adding to the Queue");
                    }
                }
            });
            persistExecutor = MoreExecutors.listeningDecorator(executorService);
        } else {
            persistExecutor = MoreExecutors.sameThreadExecutor();
        }
    }

    @Override
    protected void doReduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();

        for (Text value : values) {
            Map<String, Object> druidEvent = new HashMap<>();
            Cuboid cuboid = getCuboid(key);

            druidEvent.put(druidTimeColumn, interval.getStart());

            writeCuboid(cuboid, druidEvent);
            writeDimensions(key, cuboid, druidEvent);
            writeMeasures(value, druidEvent);

            InputRow druidRow = parser.parse(druidEvent);

            int numRows = index.add(druidRow);

            ++lineCount;

            if (!index.canAppendRow()) {
                logger.info(index.getOutOfRowsReason());
                logger.info("{} lines to {} rows in {} millis", lineCount - runningTotalLineCount, numRows, System.currentTimeMillis() - startTime);

                runningTotalLineCount = lineCount;

                final File file = new File(baseFlushFile, String.format(Locale.ROOT, "index%05d", indexCount));
                toMerge.add(file);

                context.progress();

                final IncrementalIndex persistIndex = index;
                persistFutures.add(persistExecutor.submit(new ThreadRenamingRunnable(String.format(Locale.ROOT, "%s-persist", file.getName())) {
                    @Override
                    public void doRun() {
                        try {
                            INDEX_MERGER_V9.persist(persistIndex, interval, file, indexSpec);
                        } catch (Exception e) {
                            logger.error("persist index error", e);
                            throw Throwables.propagate(e);
                        } finally {
                            // close this index
                            persistIndex.close();
                        }
                    }
                }));

                index = makeIncrementalIndex(cubeSchema.getAggregators());
                startTime = System.currentTimeMillis();
                ++indexCount;
            }
        }
    }

    private Cuboid getCuboid(Text key) {
        long id = Bytes.toLong(key.getBytes(), RowConstants.ROWKEY_SHARDID_LEN, RowConstants.ROWKEY_CUBOIDID_LEN);
        return Cuboid.findById(cubeDesc, id);
    }

    private void writeCuboid(Cuboid cuboid, Map<String, Object> druidEvent) {
        druidEvent.put(DruidSchema.ID_COL, Long.toString(cuboid.getId()));
    }

    private void writeDimensions(Text key, Cuboid cuboid, Map<String, Object> druidEvent) {
        int offset = RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN;

        for (TblColRef dim : cuboid.getColumns()) {
            int len = dimLens[dimOrdinals.get(dim)];
            String rowValue = dimEncodings.get(dim).decode(key.getBytes(), offset, len);
            druidEvent.put(cubeSchema.getDimFieldName(dim), rowValue);
            offset += len;
        }
    }

    private void writeMeasures(Text value, Map<String, Object> druidEvent) {
        // determine offsets and lengths of each measure
        ByteBuffer valBuf = ByteBuffer.wrap(value.getBytes(), 0, value.getLength());
        measureCodec.getPeekLength(valBuf, measureLens);

        int i = 0, offset = 0;
        for (MeasureDesc measure : cubeDesc.getMeasures()) {
            int len = measureLens[i];
            Object data = measureCodec.decode(ByteBuffer.wrap(value.getBytes(), offset, len), i);

            if (data instanceof RoaringBitmapCounter) {
                RoaringBitmapCounter counter = (RoaringBitmapCounter) data;
                data = new WrappedImmutableRoaringBitmap(counter.getImmutableBimap());
            }

            // for extend-column measure
            if (data instanceof ByteArray) {
                ByteArray array = (ByteArray) data;
                //the byte array for extend-column maybe NULL
                if (array.array() == null) {
                    data = new String(new byte[] {}, Charset.forName("UTF-8"));
                } else {
                    data = Bytes.toString(array.array());
                }
            }

            druidEvent.put(cubeSchema.getMeasureFieldName(measure), data);

            i++;
            offset += len;
        }
    }

    @Override
    protected void doCleanup(Context context) throws IOException, InterruptedException {
        if (lineCount == 0) {
            logger.info("The input for this reducer is zero!");
            return;
        }

        int taskId = context.getTaskAttemptID().getTaskID().getId();

        try {
            logger.info("{} lines completed.", lineCount);

            List<QueryableIndex> indexes = Lists.newArrayListWithCapacity(indexCount);
            final File mergedBase;

            if (toMerge.size() == 0) {
                if (index.isEmpty()) {
                    throw new IAE("If you try to persist empty indexes you are going to have a bad time");
                }

                mergedBase = new File(baseFlushFile, "merged");
                INDEX_MERGER_V9.persist(index, interval, mergedBase, indexSpec);
            } else {
                if (!index.isEmpty()) {
                    final File finalFile = new File(baseFlushFile, "final");
                    INDEX_MERGER_V9.persist(index, interval, finalFile, indexSpec);
                    toMerge.add(finalFile);
                }

                Futures.allAsList(persistFutures).get(1, TimeUnit.HOURS);
                persistExecutor.shutdown();

                for (File file : toMerge) {
                    indexes.add(INDEX_IO.loadIndex(file));
                }

                logger.info("starting merge of intermediate persisted segments.");
                long mergeStartTime = System.currentTimeMillis();
                mergedBase = INDEX_MERGER_V9.mergeQueryableIndex(indexes, false, cubeSchema.getAggregators(), new File(baseFlushFile, "merged"), indexSpec);
                logger.info("finished merge of intermediate persisted segments. time taken {} ms.", (System.currentTimeMillis() - mergeStartTime));

            }

            Pair<Path, Long> results = zipAndUploadFile(context, mergedBase, outputPath, taskId);

            DataSegment segment = getDataSegment(cubeSegment, results.getFirst(), taskId, results.getSecond());
            writeSegmentDescriptor(context, segment, outputPath, taskId);
        } catch (ExecutionException | TimeoutException e) {
            throw Throwables.propagate(e);
        }
    }

    private final int fsBufferSize = 1 << 18;

    private Pair<Path, Long> zipAndUploadFile(Context context, File localFile, String outputPath, int shardId) throws IOException, InterruptedException {
        long totalSize = 0;

        // zip & uploading to hdfs
        Path zipPath = new Path(outputPath, shardId + ".zip");
        FileSystem fs = zipPath.getFileSystem(context.getConfiguration());
        logger.info("uploading index to {}", zipPath);

        try (OutputStream os = fs.create(zipPath, true, fsBufferSize, context); ZipOutputStream out = new ZipOutputStream(os)) {
            File[] files = localFile.listFiles();
            for (File file : files) {
                if (Files.isRegularFile(file.toPath())) {
                    ZipEntry entry = new ZipEntry(file.getName());
                    out.putNextEntry(entry);
                    long size = Files.copy(file.toPath(), out);
                    context.progress();
                    logger.info("Added ZipEntry[{}] of {} bytes", entry.getName(), size);
                    out.closeEntry();

                    totalSize += size;
                }
            }
            out.flush();
        }

        logger.info("total size: " + totalSize);
        return new Pair<>(zipPath, totalSize);
    }

    private void writeSegmentDescriptor(Context context, DataSegment segment, String outputPath, int shardId) throws IOException {
        Path descriptorPath = new Path(outputPath, shardId + "-" + "descriptor");
        FileSystem outputFS = descriptorPath.getFileSystem(context.getConfiguration());

        if (outputFS.exists(descriptorPath)) {
            if (!outputFS.delete(descriptorPath, false)) {
                throw new IOException(String.format(Locale.ROOT, "Failed to delete descriptor at [%s]", descriptorPath));
            }
        }

        logger.info("will write " + segment + " to " + descriptorPath);

        try (final OutputStream descriptorOut = outputFS.create(descriptorPath, true, fsBufferSize)) {
            JSON_MAPPER.writeValue(descriptorOut, segment);
            descriptorOut.flush();
        }

        logger.info("write " + segment + " done");
    }

    private DataSegment getDataSegment(CubeSegment segment, Path shardPath, int shardId, long size) {
        ImmutableMap<String, Object> loadSpec = ImmutableMap.<String, Object> of("type", "hdfs", "path", shardPath.toUri().toString());

        int shardNum = segment.getTotalShards();

        NumberedShardSpec shardSpec = new NumberedShardSpec(shardId, shardNum);

        List<String> dimensionNames = new ArrayList<>();
        dimensionNames.add(DruidSchema.ID_COL);
        for (TblColRef dim : cubeSchema.getDimensions()) {
            dimensionNames.add(cubeSchema.getDimFieldName(dim));
        }

        List<String> measureNames = new ArrayList<>();
        for (MeasureDesc met : cubeSchema.getMeasures()) {
            measureNames.add(cubeSchema.getMeasureFieldName(met));
        }

        return new DataSegment(DruidSchema.getDataSource(segment.getCubeDesc()), interval, segment.getCreateTimeUTCStr(), loadSpec, dimensionNames, measureNames, shardSpec, 9, size);
    }

    private IncrementalIndex makeIncrementalIndex(AggregatorFactory[] metrics) {
        final IncrementalIndexSchema indexSchema = new IncrementalIndexSchema.Builder().withTimestampSpec(timestampSpec).withMinTimestamp(interval.getStartMillis()).withDimensionsSpec(dimensionsSpec).withQueryGranularity(new NoneGranularity()).withMetrics(metrics).withRollup(false).build();

        return new IncrementalIndex.Builder()
                .setIndexSchema(indexSchema)
                .setReportParseExceptions(true)
                .setConcurrentEventAdd(true)
                .setMaxRowCount(100000)
                .buildOnheap();
    }
}
