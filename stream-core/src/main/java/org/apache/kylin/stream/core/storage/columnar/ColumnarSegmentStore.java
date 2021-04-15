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

package org.apache.kylin.stream.core.storage.columnar;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.stream.core.exception.IllegalStorageException;
import org.apache.kylin.stream.core.metrics.StreamingMetrics;
import org.apache.kylin.stream.core.model.StreamingMessage;
import org.apache.kylin.stream.core.model.stats.SegmentStoreStats;
import org.apache.kylin.stream.core.storage.IStreamingSegmentStore;
import org.apache.kylin.stream.core.query.ResultCollector;
import org.apache.kylin.stream.core.query.ResultCollector.CloseListener;
import org.apache.kylin.stream.core.storage.StreamingCubeSegment;
import org.apache.kylin.stream.core.query.StreamingSearchContext;
import org.apache.kylin.stream.core.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.apache.kylin.shaded.com.google.common.base.Charsets;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Sets;
import org.apache.kylin.shaded.com.google.common.io.Files;

public class ColumnarSegmentStore implements IStreamingSegmentStore {
    private static final String STATE_FILE = "_STATE";
    private static Logger logger = LoggerFactory.getLogger(ColumnarSegmentStore.class);

    private static ExecutorService fragmentMergeExecutor;
    {
        fragmentMergeExecutor = new ThreadPoolExecutor(0, 10, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(), new NamedThreadFactory("fragments-merge"));
    }

    private volatile SegmentMemoryStore activeMemoryStore;
    private volatile SegmentMemoryStore persistingMemoryStore;
    private ReentrantReadWriteLock persistLock = new ReentrantReadWriteLock();
    private ReadLock persistReadLock = persistLock.readLock();
    private WriteLock persistWriteLock = persistLock.writeLock();

    private ReentrantReadWriteLock mergeLock = new ReentrantReadWriteLock();
    private ReadLock mergeReadLock = mergeLock.readLock();
    private WriteLock mergeWriteLock = mergeLock.writeLock();

    private volatile boolean persisting = false;
    private volatile boolean inMerging = false;

    private ColumnarMemoryStorePersister memoryStorePersister;
    private String baseStorePath;
    private File dataSegmentFolder;

    private int maxRowsInMemory;
    private ParsedStreamingCubeInfo parsedStreamingCubeInfo;
    private String cubeName;
    private String segmentName;
    private boolean autoMergeEnabled;

    private List<DataSegmentFragment> fragments = Lists.newCopyOnWriteArrayList();
    protected int latestCheckpointFragment = 0;

    private Map<TblColRef, Dictionary<String>> dictionaryMap;

    public ColumnarSegmentStore(String baseStorePath, CubeInstance cubeInstance, String segmentName) {
        this.maxRowsInMemory = cubeInstance.getConfig().getStreamingIndexMaxRows();
        this.baseStorePath = baseStorePath;
        this.parsedStreamingCubeInfo = new ParsedStreamingCubeInfo(cubeInstance);
        this.cubeName = cubeInstance.getName();
        this.segmentName = segmentName;

        this.dataSegmentFolder = new File(baseStorePath + File.separator + cubeName + File.separator + segmentName);
        if (!dataSegmentFolder.exists()) {
            dataSegmentFolder.mkdirs();
        }
        this.activeMemoryStore = new SegmentMemoryStore(parsedStreamingCubeInfo, segmentName);
        this.memoryStorePersister = new ColumnarMemoryStorePersister(parsedStreamingCubeInfo, segmentName);
        this.autoMergeEnabled = cubeInstance.getConfig().isStreamingFragmentsAutoMergeEnabled();
        try {
            StreamingMetrics
                    .getInstance()
                    .getMetricRegistry()
                    .register(MetricRegistry.name("streaming.inMem.row.cnt", cubeInstance.getName(), segmentName),
                            new Gauge<Integer>() {
                                @Override
                                public Integer getValue() {
                                    return activeMemoryStore.getRowCount();
                                }
                            });
        } catch (Exception e) {
            logger.warn("metrics register failed", e);
        }
    }

    @Override
    public void init() {
        fragments.addAll(getFragmentsFromFileSystem());
    }

    public int addEvent(StreamingMessage event) {
        if (activeMemoryStore == null) {
            throw new IllegalStateException("the segment has not opened:" + segmentName);
        }
        int rowsIndexed = activeMemoryStore.index(event);
        if (rowsIndexed >= maxRowsInMemory) {
            persist();
        }
        return rowsIndexed;
    }

    @Override
    public void addExternalDict(Map<TblColRef, Dictionary<String>> dictMap) {
        this.dictionaryMap = dictMap;
        this.activeMemoryStore.setDictionaryMap(dictMap);
    }

    @Override
    public File getStorePath() {
        return dataSegmentFolder;
    }

    @Override
    public Object checkpoint() {
        persist();
        latestCheckpointFragment = getLargestFragmentID();
        return String.valueOf(latestCheckpointFragment);
    }

    @Override
    public void persist() {
        if (activeMemoryStore.getRowCount() <= 0) {
            logger.info("no data in the memory store, skip persist.");
            return;
        }
        DataSegmentFragment newFragment;
        persistWriteLock.lock();
        try {
            persisting = true;
            newFragment = createNewFragment();
            persistingMemoryStore = activeMemoryStore;
            activeMemoryStore = new SegmentMemoryStore(parsedStreamingCubeInfo, segmentName);
            activeMemoryStore.setDictionaryMap(dictionaryMap);
        } finally {
            persistWriteLock.unlock();
        }

        memoryStorePersister.persist(persistingMemoryStore, newFragment);

        persistWriteLock.lock();
        try {
            persistingMemoryStore = null;
            fragments.add(newFragment);
            persisting = false;
        } finally {
            persistWriteLock.unlock();
        }
        checkRequireMerge();
    }

    private void checkRequireMerge() {
        if (!autoMergeEnabled || inMerging) {
            return;
        }
        KylinConfig config = parsedStreamingCubeInfo.cubeDesc.getConfig();
        int maxFragmentNum = config.getStreamingMaxFragmentsInSegment();
        if (fragments.size() <= maxFragmentNum) {
            return;
        }
        final List<DataSegmentFragment> fragmentsToMerge = chooseFragmentsToMerge(config, Lists.newArrayList(fragments));
        if (fragmentsToMerge.size() <= 1) {
            return;
        }
        logger.info("found some fragments need to merge:{}", fragmentsToMerge);
        inMerging = true;
        fragmentMergeExecutor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    doMergeFragments(fragmentsToMerge);
                } catch (Exception e) {
                    logger.error("error happens when merge fragments:" + fragmentsToMerge, e);
                }
            }
        });
    }

    protected void doMergeFragments(final List<DataSegmentFragment> fragmentsToMerge) throws IOException {
        logger.info("start to merge fragments:{}", fragmentsToMerge);
        FragmentFilesMerger fragmentsMerger = new FragmentFilesMerger(parsedStreamingCubeInfo, dataSegmentFolder);
        FragmentsMergeResult mergeResult = fragmentsMerger.merge(fragmentsToMerge);
        logger.info("finish to merge fragments, try to commit the merge result");
        commitFragmentsMerge(mergeResult);
        fragmentsMerger.cleanMergeDirectory();
        inMerging = false;
    }

    protected List<DataSegmentFragment> chooseFragmentsToMerge(KylinConfig config,
            List<DataSegmentFragment> allFragments) {
        Collections.sort(allFragments);
        List<DataSegmentFragment> result = doChooseFragments(config, allFragments, true);
        //        if (result.size() > 1) {
        //            return result;
        //        } else {
        //            logger.info("recheck existing merged fragments");
        //            result = doChooseFragments(config, allFragments, false);
        //        }
        return result;
    }

    protected List<DataSegmentFragment> doChooseFragments(KylinConfig config, List<DataSegmentFragment> allFragments,
            boolean ignoreMergedFragments) {
        List<DataSegmentFragment> result = Lists.newArrayList();
        int originFragmentsNum = allFragments.size();
        int minFragments = config.getStreamingMinFragmentsInSegment();
        long maxFragmentSize = config.getStreamingMaxFragmentSizeInMb() * 1024 * 1024L;
        long toMergeDataSize = 0;
        for (int i = 0; i < originFragmentsNum; i++) {
            DataSegmentFragment fragment = allFragments.get(i);
            if (originFragmentsNum - result.size() <= minFragments - 1) {
                return result;
            }
            if (fragment.getFragmentId().getEndId() > latestCheckpointFragment) {
                return result;
            }
            if (ignoreMergedFragments && fragment.isMergedFragment()) {
                if (result.size() > 1) {
                    return result;
                } else if (result.size() == 1) {
                    toMergeDataSize = 0;
                    result.clear();
                }
                continue;
            }
            long fragmentDataSize = fragment.getDataFileSize();
            if (toMergeDataSize + fragmentDataSize <= maxFragmentSize) {
                toMergeDataSize += fragmentDataSize;
                result.add(fragment);
            } else if (result.size() > 1) {
                return result;
            } else if (result.size() == 1) {
                toMergeDataSize = 0;
                result.clear();
            }
        }
        return result;
    }

    private void commitFragmentsMerge(FragmentsMergeResult mergeResult) throws IOException {
        mergeWriteLock.lock();
        try {
            mergeResult.getOrigFragments();
            removeFragments(mergeResult.getOrigFragments());
            DataSegmentFragment fragment = new DataSegmentFragment(baseStorePath, cubeName, segmentName,
                    mergeResult.getMergedFragmentId());
            FileUtils.moveFileToDirectory(mergeResult.getMergedFragmentDataFile(), fragment.getFragmentFolder(), true);
            FileUtils.moveFileToDirectory(mergeResult.getMergedFragmentMetaFile(), fragment.getFragmentFolder(), true);
            fragments.add(fragment);
        } finally {
            mergeWriteLock.unlock();
        }
    }

    @Override
    public void purge() {
        try {
            FileUtils.deleteDirectory(dataSegmentFolder);
            logger.info("removed segment data, cube-{} segment-{}", cubeName, segmentName);
            ColumnarStoreCache.getInstance().removeFragmentsCache(fragments);
            fragments = Lists.newCopyOnWriteArrayList();
            logger.info("removed segment cache, cube-{} segment-{}", cubeName, segmentName);
        } catch (IOException e) {
            logger.error("error happens when purge segment", e);
        }
    }

    @Override
    public void restoreFromCheckpoint(Object checkpoint) {
        String checkpointFragmentIDString = (String) checkpoint;
        FragmentId checkpointFragmentID = FragmentId.parse(checkpointFragmentIDString);
        List<DataSegmentFragment> fragments = getFragmentsFromFileSystem();
        for (DataSegmentFragment fragment : fragments) {
            if (fragment.getFragmentId().compareTo(checkpointFragmentID) > 0) {
                fragment.purge();
            }
        }
    }

    @Override
    public String getSegmentName() {
        return segmentName;
    }

    @Override
    public StreamingCubeSegment.State getSegmentState() {
        File stateFile = new File(dataSegmentFolder, STATE_FILE);
        if (stateFile.exists()) {
            StreamingCubeSegment.State state = parseStateFile(stateFile);
            return state;
        }
        return StreamingCubeSegment.State.ACTIVE;
    }

    @Override
    public void setSegmentState(StreamingCubeSegment.State state) {
        File stateFile = new File(dataSegmentFolder, STATE_FILE);
        FileOutputStream outPut = null;
        try {
            if (!stateFile.exists()) {
                stateFile.createNewFile();
            }
            outPut = new FileOutputStream(stateFile);
            outPut.write(Bytes.toBytes(state.name()));
            outPut.flush();
        } catch (IOException e) {
            throw new IllegalStorageException(e);
        } finally {
            if (outPut != null) {
                try {
                    outPut.close();
                } catch (IOException e) {
                    logger.error("error when close", e);
                }
            }
        }
    }

    public DataSegmentFragment createNewFragment() {
        int largestFragID = getLargestFragmentID();
        DataSegmentFragment newFragment = new DataSegmentFragment(baseStorePath, cubeName, segmentName, new FragmentId(
                ++largestFragID));
        return newFragment;
    }

    public List<DataSegmentFragment> getAllFragments() {
        return fragments;
    }

    private void removeFragments(List<DataSegmentFragment> fragmentsToRemove) {
        fragments.removeAll(Sets.newHashSet(fragmentsToRemove));
        for (DataSegmentFragment fragment : fragmentsToRemove) {
            ColumnarStoreCache.getInstance().removeFragmentCache(fragment);
            fragment.purge();
        }
    }

    private int getLargestFragmentID() {
        List<DataSegmentFragment> existingFragments = getFragmentsFromFileSystem();
        int largestFragId = 0;
        for (DataSegmentFragment existingFragment : existingFragments) {
            int id = existingFragment.getFragmentId().getEndId();
            if (id > largestFragId) {
                largestFragId = id;
            }
        }
        return largestFragId;
    }

    private List<DataSegmentFragment> getFragmentsFromFileSystem() {
        List<DataSegmentFragment> fragments = Lists.newArrayList();
        File dataSegmentFolder = getStorePath();
        File[] fragmentFolders = dataSegmentFolder.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                if (!file.isDirectory()) {
                    return false;
                }
                if (file.getName().equalsIgnoreCase("_SUCCESS")) {
                    return false;
                }
                try {
                    FragmentId.parse(file.getName());
                } catch (Exception e) {
                    return false;
                }
                return true;
            }
        });
        if (fragmentFolders != null) {
            for (File fragmentFolder : fragmentFolders) {
                fragments.add(new DataSegmentFragment(baseStorePath, cubeName, segmentName, FragmentId
                        .parse(fragmentFolder.getName())));
            }
        }
        return fragments;
    }

    @Override
    public SegmentStoreStats getStoreStats() {
        SegmentStoreStats storeStats = new SegmentStoreStats();
        storeStats.setNumRowsInMem(activeMemoryStore.getRowCount());
        storeStats.setNumFragments(getFragmentsFromFileSystem().size());
        return storeStats;
    }

    public SegmentMemoryStore getActiveMemoryStore() {
        return activeMemoryStore;
    }

    @Override
    public void search(final StreamingSearchContext searchContext, ResultCollector collector) throws IOException {
        SegmentMemoryStore searchMemoryStore;
        List<DataSegmentFragment> searchFragments;
        mergeReadLock.lock();
        collector.addCloseListener(new CloseListener() {
            @Override
            public void onClose() {
                mergeReadLock.unlock();
            }
        });
        persistReadLock.lock();
        try {
            searchFragments = getAllFragments();
            if (persisting) {
                searchMemoryStore = persistingMemoryStore;
            } else {
                searchMemoryStore = activeMemoryStore;
            }
        } finally {
            persistReadLock.unlock();
        }
        new ColumnarSegmentStoreFilesSearcher(segmentName, searchFragments).search(searchContext, collector);
        searchMemoryStore.search(searchContext, collector);
    }

    public void close() throws IOException {
        logger.warn("closing the streaming cube segment, cube {}, segment {}.", cubeName, segmentName);
        StreamingMetrics.getInstance().getMetricRegistry()
                .remove(MetricRegistry.name("streaming.inMem.row.cnt", cubeName, segmentName));
    }

    private StreamingCubeSegment.State parseStateFile(File stateFile) {
        StreamingCubeSegment.State result = StreamingCubeSegment.State.ACTIVE;
        try {
            String stateName = Files.toString(stateFile, Charsets.UTF_8);
            result = StreamingCubeSegment.State.valueOf(stateName.trim());
        } catch (IOException e) {
            logger.error("error when parse state file", e);
        }
        return result;
    }

}
