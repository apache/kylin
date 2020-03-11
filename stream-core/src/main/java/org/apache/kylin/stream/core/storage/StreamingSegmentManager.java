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

package org.apache.kylin.stream.core.storage;

import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.measure.bitmap.BitmapMeasureType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.stream.core.consumer.IConsumerProvider;
import org.apache.kylin.stream.core.consumer.StreamingConsumerChannel;
import org.apache.kylin.stream.core.dict.StreamingDictionaryClient;
import org.apache.kylin.stream.core.dict.StreamingDistributedDictionary;
import org.apache.kylin.stream.core.model.StreamingMessage;
import org.apache.kylin.stream.core.model.stats.LongLatencyInfo;
import org.apache.kylin.stream.core.model.stats.SegmentStats;
import org.apache.kylin.stream.core.model.stats.SegmentStoreStats;
import org.apache.kylin.stream.core.query.StreamingCubeDataSearcher;
import org.apache.kylin.stream.core.source.ISourcePosition;
import org.apache.kylin.stream.core.source.ISourcePositionHandler;
import org.apache.kylin.stream.core.source.ISourcePositionHandler.MergeStrategy;
import org.apache.kylin.stream.core.storage.columnar.ColumnarSegmentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Function;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class StreamingSegmentManager implements Closeable {
    private static Logger logger = LoggerFactory.getLogger(StreamingSegmentManager.class);
    private final String cubeName;
    private final CubeInstance cubeInstance;
    private AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Cube window defines how streaming events are divided and put into different segments , for example 1 hour per segment(indexer).
     * If the received events' timestamp is completely out of order and belongs to a very wide range,
     * there will be multiple active segment indexers created and serve the indexing and querying.
     * */
    private final long cubeWindow;

    /**
     * Cube duration defines how long the oldest streaming segment becomes immutable and does not allow additional modification.
     * Any further long latency events that can't find a corresponding segment to serve the index,
     * the events will be put to a specific segment for long latency events only.
     * */
    public final long cubeDuration;

    private final long maxCubeDuration;

    private final int checkPointIntervals;
    private final int maxImmutableSegments;

    private final String baseStorePath;
    private final File cubeDataFolder;
    private final CheckPointStore checkPointStore;

    private final IConsumerProvider consumerProvider;

    private final Map<Long, StreamingCubeSegment> activeSegments = new ConcurrentSkipListMap<>();
    private final Map<Long, StreamingCubeSegment> immutableSegments = new ConcurrentSkipListMap<>();
    private Map<Long, ISourcePosition> segmentSourceStartPositions = Maps.newConcurrentMap();

    private final ISourcePositionHandler sourcePositionHandler;
    private final ISourcePosition consumePosition;
    private static final long TIME_ZONE_OFFSET = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getTimeZone())
            .getRawOffset();
    private volatile LongLatencyInfo longLatencyInfo;
    private volatile long nextCheckPoint = 0;
    private volatile long lastCheckPointCount = 0;

    private Pair<Long, Long> latestRemoteSegmentRange;

    private AtomicLong ingestCount = new AtomicLong();
    private AtomicLong dropCounts = new AtomicLong();
    private volatile long latestEventTime = 0;
    private volatile long latestEventIngestTime = 0;
    private Map<TblColRef, Dictionary<String>> dictionaryMap = new HashMap<>();
    private StreamingDictionaryClient streamingDictionaryClient;

    public StreamingSegmentManager(String baseStorePath, CubeInstance cubeInstance, ISourcePositionHandler sourcePosHandler, IConsumerProvider consumerProvider) {
        this.baseStorePath = baseStorePath;
        this.cubeName = cubeInstance.getName();
        this.cubeInstance = cubeInstance;
        this.cubeWindow = cubeInstance.getConfig().getStreamingCubeWindowInSecs() * 1000L;
        this.cubeDuration = cubeInstance.getConfig().getStreamingCubeDurationInSecs() * 1000L;
        this.maxCubeDuration = cubeInstance.getConfig().getStreamingCubeMaxDurationInSecs() * 1000L;
        this.checkPointIntervals = cubeInstance.getConfig().getStreamingCheckPointIntervalsInSecs() * 1000;
        this.maxImmutableSegments = cubeInstance.getConfig().getStreamingMaxImmutableSegments();
        this.consumerProvider = consumerProvider;
        this.cubeDataFolder = new File(baseStorePath, cubeName);
        this.sourcePositionHandler = sourcePosHandler;
        this.consumePosition = sourcePositionHandler.createEmptyPosition();
        if (!cubeDataFolder.exists()) {
            cubeDataFolder.mkdirs();
        }
        this.longLatencyInfo = new LongLatencyInfo();
        this.checkPointStore = new CheckPointStore(cubeName, cubeDataFolder, cubeInstance.getConfig()
                .getStreamingCheckPointFileMaxNum());

        // Prepare for realtime dictionary encoder.
        CubeInstance cube = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(cubeName);
        List<MeasureDesc> bitmapMeasureList = cube.getDescriptor().getMeasures().stream()
                .filter(measureDesc -> measureDesc.getFunction().getMeasureType() instanceof BitmapMeasureType)
                .collect(Collectors.toList());
        if (!bitmapMeasureList.isEmpty()) {
            List<String> realtimeDictColumn = bitmapMeasureList.stream()
                    .map(measureDesc -> measureDesc.getFunction().getParameter().getColRef().getIdentity())
                    .collect(Collectors.toList());
            String str = String.join(", ", realtimeDictColumn);
            logger.info("Find these columns {} need to be encoded realtime.", str);
            List<TblColRef> tblColRefs = bitmapMeasureList.stream()
                    .map(measureDesc -> measureDesc.getFunction().getParameter().getColRef())
                    .collect(Collectors.toList());

            streamingDictionaryClient = new StreamingDictionaryClient(cubeName,
                    realtimeDictColumn.toArray(new String[0]));
            tblColRefs.forEach(col -> dictionaryMap.put(col,
                    new StreamingDistributedDictionary(col.getIdentity(), streamingDictionaryClient)));
        }
    }

    public void addEvent(StreamingMessage event) {
        long eventTime = event.getTimestamp();
        long segmentStart = truncateTime(eventTime, cubeWindow);
        long segmentEnd = segmentStart + cubeWindow;
        StreamingCubeSegment segment = activeSegments.get(segmentStart);
        if (segment == null) {
            // Before creating new segment, check the current active segments, if
            // They too old and past the cube duration, make them to immutable
            List<StreamingCubeSegment> toBeImmutableSegments = findSegmentsToBeImmutable();
            if (!toBeImmutableSegments.isEmpty()) {
                convertImmutable(toBeImmutableSegments);
            }
            // Before creating new segment, check whether it belongs to
            // immutable segments, if true, drop the event
            if (isLongLatencyEvent(segmentStart)) {
                longLatencyInfo.incLongLatencyEvent(CubeSegment.makeSegmentName(new TSRange(segmentStart, segmentEnd), null, cubeInstance.getModel()));
                ingestCount.incrementAndGet();
                consumePosition.update(event.getSourcePosition());
                return;
            }

            //Create new segment
            if (segment == null) {
                segment = createSegment(segmentStart, segmentEnd);
                if (logger.isInfoEnabled()) {
                    logger.info("Create new segment:{}", segment);
                }

                activeSegments.put(segmentStart, segment);
                // when current active segments exceed tolerance, some unpredictable accident may happend,
                // but is should be configurable or computed on the fly
                if (activeSegments.size() > 12) {
                    logger.warn("Two many active segments, segments size = " + activeSegments.keySet());
                }
                if (immutableSegments.size() > maxImmutableSegments) {
                    logger.info("Two many immutable segments, segments size:{}, pause the cube consume",
                            immutableSegments.size());
                    pauseCubeConsumer();
                }
            }
        }
        long currentTime = System.currentTimeMillis();
        latestEventIngestTime = currentTime;
        segment.addEvent(event);
        segment.setLastUpdateTime(currentTime);
        if (eventTime > latestEventTime) {
            latestEventTime = eventTime;
        }
        ingestCount.incrementAndGet();
        // update the segment start source position
        ISourcePosition segmentSourceStartPos = segmentSourceStartPositions.get(segment.getDateRangeStart());
        if (segmentSourceStartPos == null) {
            segmentSourceStartPos = sourcePositionHandler.createEmptyPosition();
            segmentSourceStartPositions.put(segment.getDateRangeStart(), segmentSourceStartPos);
        }
        segmentSourceStartPos.updateWhenPartitionNotExist(event.getSourcePosition());

        consumePosition.update(event.getSourcePosition());

        if (nextCheckPoint == 0) {
            nextCheckPoint = currentTime + checkPointIntervals;
        }

        if (currentTime > nextCheckPoint) {
            checkpoint();
            nextCheckPoint = System.currentTimeMillis() + checkPointIntervals;
        }
    }

    public void restoreConsumerStates(ISourcePosition sourcePosition) {
        logger.info("restore consume state:{}", sourcePosition);
        this.consumePosition.copy(sourcePosition);
    }

    public void restoreSegmentsFromLocal() {
        logger.info("restore segments from local files for cube:{}", cubeName);
        List<File> segmentFolders = getSegmentFolders(cubeDataFolder);
        Map<Long, String> checkpointStoreStats = null;
        CheckPoint checkpoint = checkPointStore.getLatestCheckPoint();
        if (checkpoint != null) {
            checkpointStoreStats = checkpoint.getPersistedIndexes();
            logger.info("checkpoint found for the cube:{}, checkpoint:{}", cubeName, checkpoint);
            ingestCount.set(checkpoint.getTotalCount());
            if (checkpoint.getLongLatencyInfo() != null) {
                longLatencyInfo = checkpoint.getLongLatencyInfo();
            }
        }
        CubeSegment latestRemoteSegment = cubeInstance.getLatestReadySegment();
        if (checkpointStoreStats == null || checkpointStoreStats.isEmpty()) {
            logger.warn("no checkpoint for cube:{} store state, remove all local segments folders", cubeName);
            // purge all segment folders if there is no checkpoint store stats
            for (File segmentFolder : segmentFolders) {
                removeSegmentFolder(segmentFolder);
            }
        } else {
            restoreSegmentsFromCP(segmentFolders, checkpointStoreStats, checkpoint.getSegmentSourceStartPosition(), latestRemoteSegment);
        }
    }

    private void restoreSegmentsFromCP(List<File> segmentFolders, Map<Long, String> checkpointStoreStats,
                                       Map<Long, String> segmentSourceStartPositions, CubeSegment latestRemoteSegment) {
        if (segmentSourceStartPositions != null) {
            this.segmentSourceStartPositions.putAll(Maps.transformValues(segmentSourceStartPositions, new Function<String, ISourcePosition>() {
                @Nullable
                @Override
                public ISourcePosition apply(@Nullable String input) {
                    return sourcePositionHandler.parsePosition(input);
                }
            }));
        }
        for (File segmentFolder : segmentFolders) {
            try {
                IStreamingSegmentStore segmentStore = getSegmentStore(segmentFolder.getName());
                StreamingCubeSegment segment = StreamingCubeSegment.parseSegment(cubeInstance, segmentFolder,
                        segmentStore);

                if (latestRemoteSegment != null && segment.getDateRangeEnd() <= latestRemoteSegment.getTSRange().end.v) {
                    logger.info("remove segment:{} because it is late than remote segment", segment);
                    removeSegmentFolder(segmentFolder);
                    continue;
                }

                if (segment.isImmutable()) {
                    immutableSegments.put(segment.getDateRangeStart(), segment);
                } else {
                    // restore the active segment
                    String segmentCheckpoint = checkpointStoreStats.get(segment.getDateRangeStart());
                    if (segmentCheckpoint == null) {
                        removeSegmentFolder(segmentFolder);
                    } else {
                        segmentStore.restoreFromCheckpoint(segmentCheckpoint);
                    }
                    activeSegments.put(segment.getDateRangeStart(), segment);
                }
            } catch (Exception e) {
                logger.error("fail to restore segment from file:" + segmentFolder.getName(), e);
            }
        }
    }

    private void removeSegmentFolder(File segmentFolder) {
        logger.info("Remove segment folder: {} for the cube:{} ", segmentFolder, cubeName);
        try {
            FileUtils.deleteDirectory(segmentFolder);
        } catch (IOException e) {
            logger.error("error happens when purge segment", e);
        }
    }

    private List<File> getSegmentFolders(File cubeDataFolder) {
        File[] segmentFolders = cubeDataFolder.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.isDirectory() && !".cp".equals(file.getName());
            }
        });
        if (segmentFolders.length == 0) {
            return Lists.newArrayList();
        }
        return Arrays.asList(segmentFolders);
    }

    /**
     *
     * @return the consume state that has been checkpoint in the local store
     */
    public String getCheckPointSourcePosition() {
        CheckPoint latestCheckPoint = checkPointStore.getLatestCheckPoint();
        if (latestCheckPoint == null) {
            return null;
        }
        return latestCheckPoint.getSourceConsumePosition();
    }

    private void pauseCubeConsumer() {
        if (consumerProvider == null) {
            return;
        }
        StreamingConsumerChannel consumer = consumerProvider.getConsumer(cubeName);
        if (consumer != null) {
            consumer.pause(false);
        }
    }

    private boolean isLongLatencyEvent(long segmentStart) {
        if (immutableSegments.containsKey(segmentStart)
                || (latestRemoteSegmentRange != null && segmentStart <= latestRemoteSegmentRange.getFirst())) {
            return true;
        }

        if (immutableSegments.isEmpty()) {
            return false;
        }

        long maxImmutableSegmentStart = Collections.max(immutableSegments.keySet());
        if (segmentStart <= maxImmutableSegmentStart) {
            return true;
        }
        return false;
    }

    private List<StreamingCubeSegment> findSegmentsToBeImmutable() {
        List<StreamingCubeSegment> toBeImmutableSegments = Lists.newArrayList();
        if (activeSegments.size() <= 0) {
            return toBeImmutableSegments;
        }

        for (Map.Entry<Long, StreamingCubeSegment> item : activeSegments.entrySet()) {
            StreamingCubeSegment segment = item.getValue();
            if (segment != null) {
                if ((System.currentTimeMillis() - segment.getLastUpdateTime()) > cubeDuration) {
                    toBeImmutableSegments.add(segment);
                } else if ((System.currentTimeMillis() - segment.getCreateTime()) > maxCubeDuration) {
                    logger.warn("OldestSegment:{} pass maxCubeDuration", segment);
                    toBeImmutableSegments.add(segment);
                } else {
                    return toBeImmutableSegments;
                }
            }
        }
        return toBeImmutableSegments;
    }

    private StreamingCubeSegment createSegment(long segmentStart, long segmentEnd) {
        String segmentName = CubeSegment.makeSegmentName(new TSRange(segmentStart, segmentEnd), null, cubeInstance.getModel());
        IStreamingSegmentStore store = getSegmentStore(segmentName);
        StreamingCubeSegment newSegment = new StreamingCubeSegment(cubeInstance, store, segmentStart, segmentEnd);
        return newSegment;
    }

    private IStreamingSegmentStore getSegmentStore(String segmentName) {
        String storeClassName = cubeInstance.getConfig().getStreamingStoreClass();
        IStreamingSegmentStore segmentStore;
        try {
            Class clazz = Class.forName(storeClassName);
            Constructor<IStreamingSegmentStore> constructor = clazz.getConstructor(String.class, CubeInstance.class,
                    String.class);
            segmentStore = constructor.newInstance(baseStorePath, cubeInstance, segmentName);
            segmentStore.addExternalDict(dictionaryMap);
        } catch (Exception e) {
            logger.warn("Fail to construct an instance for " + storeClassName
                    + ". Will use the default store: ColumnarSegmentStore");
            segmentStore = new ColumnarSegmentStore(baseStorePath, cubeInstance, segmentName);
        }
        segmentStore.init();
        return segmentStore;
    }

    public Collection<StreamingCubeSegment> getAllSegments() {
        List<StreamingCubeSegment> result = Lists.newArrayList();
        result.addAll(activeSegments.values());
        result.addAll(immutableSegments.values());
        return result;
    }

    private Collection<StreamingCubeSegment> getSegmentsLateThan(long segmentStart) {
        List<StreamingCubeSegment> result = Lists.newArrayList();
        for (StreamingCubeSegment segment : immutableSegments.values()) {
            if (segment.getDateRangeStart() > segmentStart) {
                result.add(segment);
            }
        }
        for (StreamingCubeSegment segment : activeSegments.values()) {
            if (segment.getDateRangeStart() > segmentStart) {
                result.add(segment);
            }
        }
        return result;
    }

    /**
     * get segment according to the segment name
     * @param segmentName
     * @return null if segment not exist
     */
    public StreamingCubeSegment getSegmentByName(String segmentName) {
        Collection<StreamingCubeSegment> allSegments = getAllSegments();
        for (StreamingCubeSegment segment : allSegments) {
            if (segmentName.equals(segment.getSegmentName())) {
                return segment;
            }
        }
        return null;
    }

    public Collection<StreamingCubeSegment> getActiveSegments() {
        return activeSegments.values();
    }

    public Collection<StreamingCubeSegment> getRequireRemotePersistSegments() {
        Collection<StreamingCubeSegment> allImmutableSegments = immutableSegments.values();
        List<StreamingCubeSegment> result = Lists.newArrayList();
        for (StreamingCubeSegment cubeSegment : allImmutableSegments) {
            if (!cubeSegment.isPersistToRemote()) {
                result.add(cubeSegment);
            }
        }
        if (result.size() > 1) {
            Collections.sort(result);
        }
        return result;
    }

    /**
     * get the smallest source offsets in all other segments that larger than the specified segment
     */
    public ISourcePosition getSmallestSourcePosition(StreamingCubeSegment currSegment) {
        List<ISourcePosition> allSegmentStartPositions = Lists.newArrayList();
        for (Entry<Long, ISourcePosition> segmentSourceStartPosEntry : segmentSourceStartPositions.entrySet()) {
            if (currSegment.getDateRangeStart() < segmentSourceStartPosEntry.getKey()) {
                allSegmentStartPositions.add(segmentSourceStartPosEntry.getValue());
            }
        }
        return sourcePositionHandler.mergePositions(allSegmentStartPositions, MergeStrategy.KEEP_SMALL);
    }

    public void purgeSegment(String segmentName) {
        Pair<Long, Long> segmentRange = CubeSegment.parseSegmentName(segmentName);
        StreamingCubeSegment segment = activeSegments.remove(segmentRange.getFirst());
        if (segment == null) {
            segment = immutableSegments.remove(segmentRange.getFirst());
        }
        segmentSourceStartPositions.remove(segmentRange.getFirst());
        if (segment != null) {
            segment.purge();
        }
    }

    public void makeAllSegmentsImmutable() {
        Collection<StreamingCubeSegment> activeSegments = getActiveSegments();
        if (activeSegments != null && !activeSegments.isEmpty()) {
            List<StreamingCubeSegment> segments = Lists.newArrayList(activeSegments);
            for (StreamingCubeSegment segment : segments) {
                convertImmutable(segment);
            }
        }
    }

    public void makeSegmentImmutable(String segmentName) {
        StreamingCubeSegment segment = getSegmentByName(segmentName);
        convertImmutable(segment);
    }

    public List<String> remoteSegmentBuildComplete(String segmentName) {
        Pair<Long, Long> segmentRange = CubeSegment.parseSegmentName(segmentName);
        List<Long> immutableSegmentStarts = Lists.newArrayList(immutableSegments.keySet());
        List<String> removedSegments = Lists.newArrayList();
        for (Long immutableSegmentStart : immutableSegmentStarts) {
            if (immutableSegmentStart <= segmentRange.getFirst()) {
                StreamingCubeSegment segment = immutableSegments.get(immutableSegmentStart);
                immutableSegments.remove(immutableSegmentStart);
                segment.purge();
                removedSegments.add(segment.getSegmentName());
            }
        }

        List<Long> activeSegmentStarts = Lists.newArrayList(activeSegments.keySet());
        for (Long activeSegmentStart : activeSegmentStarts) {
            if (activeSegmentStart <= segmentRange.getFirst()) {
                StreamingCubeSegment segment = activeSegments.get(activeSegmentStart);
                activeSegments.remove(activeSegmentStart);
                segment.purge();
                removedSegments.add(segment.getSegmentName());
            }
        }

        this.latestRemoteSegmentRange = segmentRange;
        logger.info("cube:{} segments:{} has been removed", cubeName, removedSegments);
        return removedSegments;
    }

    public void purgeAllSegments() {
        try {
            FileUtils.deleteDirectory(cubeDataFolder);
        } catch (IOException e) {
            logger.error("error happens when purge cube", e);
        }
    }

    public String getConsumePositionStr() {
        return sourcePositionHandler.serializePosition(consumePosition);
    }

    public ISourcePosition getConsumePosition() {
        return consumePosition;
    }

    public long getIngestCount() {
        return ingestCount.get();
    }

    public long getLatestEventTime() {
        return latestEventTime;
    }

    public long getLatestEventIngestTime() {
        return latestEventIngestTime;
    }

    public LongLatencyInfo getLongLatencyInfo() {
        return longLatencyInfo;
    }

    public CubeInstance getCubeInstance() {
        return cubeInstance;
    }

    public StreamingCubeDataSearcher getSearcher() {
        return new StreamingCubeDataSearcher(this);
    }

    public Map<String, SegmentStats> getSegmentStats() {
        Map<String, SegmentStats> result = Maps.newLinkedHashMap();
        Collection<StreamingCubeSegment> allSegments = getAllSegments();
        for (StreamingCubeSegment segment : allSegments) {
            SegmentStats segmentStats = new SegmentStats();
            segmentStats.setSegmentState(segment.getState().name());
            segmentStats.setSegmentCreateTime(resetTimestampByTimeZone(segment.getCreateTime()));
            segmentStats.setSegmentLastUpdateTime(resetTimestampByTimeZone(segment.getLastUpdateTime()));
            segmentStats.setLatestEventTime(resetTimestampByTimeZone(segment.getLatestEventTimeStamp()));
            segmentStats.setLatestEventLatency(segment.getLatestEventLatecy());
            SegmentStoreStats storeStats = segment.getSegmentStore().getStoreStats();
            segmentStats.setStoreStats(storeStats);
            result.put(segment.getSegmentName(), segmentStats);
        }
        return result;
    }

    @Override
    public void close() {
        if (closed.get()){
            logger.debug("Already close it, skip.");
            return;
        }
        logger.warn("Closing Streaming Cube store, cubeName={}", cubeName);
        checkpoint();
        logger.warn("{} ingested {} , dropped {}, long latency {}", cubeName, ingestCount.get(), dropCounts.get(),
                longLatencyInfo.getTotalLongLatencyEventCnt());
        for (StreamingCubeSegment cubesegment : getAllSegments()) {
            try {
                cubesegment.close();
            } catch (IOException e) {
                logger.error("fail to close cube segment, segment :" + cubesegment.getSegmentName(), e);
            }
        }
        if (streamingDictionaryClient != null) {
            streamingDictionaryClient.close();
        }
        closed.set(true);
    }

    public synchronized void checkpoint() {
        Map<Long, String> persistedIndexes = Maps.newHashMap();

        for (Map.Entry<Long, StreamingCubeSegment> segmentEntry : activeSegments.entrySet()) {
            IStreamingSegmentStore segmentStore = segmentEntry.getValue().getSegmentStore();
            String largestFragmentID = segmentStore.checkpoint().toString();
            persistedIndexes.put(segmentEntry.getKey(), largestFragmentID);
        }

        persistCheckPoint(consumePosition, persistedIndexes);
    }

    //Checkpoint is used to record the the persisted segments windows position and consume stats like partitions/offsets
    //It must be called from the same thread which is doing indexing to make sure the state is consistent.
    private void persistCheckPoint(ISourcePosition consumePosition, Map<Long, String> persistedIndexes) {
        CheckPoint cp = new CheckPoint();
        cp.setCheckPointTime(System.currentTimeMillis());
        cp.setSourceConsumePosition(sourcePositionHandler.serializePosition(consumePosition));
        cp.setPersistedIndexes(persistedIndexes);
        cp.setTotalCount(ingestCount.get());
        cp.setCheckPointCount(ingestCount.get() - lastCheckPointCount);
        lastCheckPointCount = ingestCount.get();
        cp.setLongLatencyInfo(longLatencyInfo.truncate(100));
        Collection<StreamingCubeSegment> allSegments = getAllSegments();
        Map<Long, String> segmentSourceStartPosStr = Maps.newHashMap();
        for (StreamingCubeSegment segment : allSegments) {
            segmentSourceStartPosStr.put(segment.getDateRangeStart(), sourcePositionHandler.serializePosition(this.segmentSourceStartPositions.get(segment.getDateRangeStart())));
        }
        cp.setSegmentSourceStartPosition(segmentSourceStartPosStr);
        if (logger.isInfoEnabled()) {
            logger.info("Print check point for cube {}", cubeName + " ," + cp.toString());
        }

        checkPointStore.saveCheckPoint(cp);
    }

    private void convertImmutable(List<StreamingCubeSegment> oldestSegments) {
        for (StreamingCubeSegment segment : oldestSegments) {
            convertImmutable(segment);
        }
    }

    private void convertImmutable(StreamingCubeSegment segment) {
        activeSegments.remove(segment.getDateRangeStart());
        segment.immutable();
        immutableSegments.put(segment.getDateRangeStart(), segment);
        if (logger.isInfoEnabled()) {
            logger.info("Convert active segment to immutable: {}", segment);
        }
    }

    private long truncateTime(long timestamp, long windowLength) {
        return (timestamp / windowLength) * windowLength;
    }

    public static long resetTimestampByTimeZone(long ts) {
        return ts + TIME_ZONE_OFFSET;
    }
}
