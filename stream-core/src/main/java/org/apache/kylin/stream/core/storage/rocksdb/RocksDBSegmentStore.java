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

package org.apache.kylin.stream.core.storage.rocksdb;

import java.io.File;
import java.io.IOException;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.stream.core.model.StreamingMessage;
import org.apache.kylin.stream.core.model.stats.SegmentStoreStats;
import org.apache.kylin.stream.core.query.ResultCollector;
import org.apache.kylin.stream.core.query.StreamingSearchContext;
import org.apache.kylin.stream.core.storage.IStreamingSegmentStore;
import org.apache.kylin.stream.core.storage.StreamingCubeSegment;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDBSegmentStore implements IStreamingSegmentStore {
    private static Logger logger = LoggerFactory.getLogger(RocksDBSegmentStore.class);

    private File dataSegmentFolder;

    private CubeInstance cubeInstance;
    private String cubeName;
    private String segmentName;
    private RocksDB db;

    public RocksDBSegmentStore(String baseStorePath, CubeInstance cubeInstance, String segmentName) {
        this.cubeInstance = cubeInstance;
        this.cubeName = cubeInstance.getName();
        this.segmentName = segmentName;

        this.dataSegmentFolder = new File(baseStorePath + File.separator + cubeName + File.separator + segmentName);
        if (!dataSegmentFolder.exists()) {
            dataSegmentFolder.mkdirs();
        }
    }

    @Override
    public void init() {
        Options options = getOptions().setCreateIfMissing(true);
        try {
            String dataPath = dataSegmentFolder.getAbsolutePath() + "/data";
            db = RocksDB.open(options, dataPath);
        } catch (RocksDBException e) {
            logger.error("init rocks db fail");
        }
    }

    private Options getOptions() {
        return new Options();
    }

    @Override
    public int addEvent(StreamingMessage event) {
        return 0;
    }

    @Override
    public File getStorePath() {
        return null;
    }

    @Override
    public void persist() {
        try {
            db.flush(new FlushOptions());
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Object checkpoint() {
        return null;
    }

    @Override
    public void purge() {

    }

    @Override
    public void restoreFromCheckpoint(Object checkpoint) {

    }

    @Override
    public String getSegmentName() {
        return null;
    }

    @Override
    public StreamingCubeSegment.State getSegmentState() {
        return null;
    }

    @Override
    public void setSegmentState(StreamingCubeSegment.State state) {

    }

    @Override
    public void search(final StreamingSearchContext searchContext, ResultCollector collector) throws IOException {
    }

    @Override
    public SegmentStoreStats getStoreStats() {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
