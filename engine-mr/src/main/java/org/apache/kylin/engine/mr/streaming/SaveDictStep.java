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

package org.apache.kylin.engine.mr.streaming;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.shaded.com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.lock.DistributedLock;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryInfoSerializer;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.engine.mr.common.CubeJobLockUtil;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.ExecuteResult.State;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class SaveDictStep extends AbstractExecutable {
    private static final Logger logger = LoggerFactory.getLogger(SaveDictStep.class);

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        logger.info("job {} start to run SaveDictStep", getJobFlowJobId());
        final CubeManager mgr = CubeManager.getInstance(context.getConfig());
        final DictionaryManager dictManager = DictionaryManager.getInstance(context.getConfig());

        final CubeInstance cube = mgr.getCube(CubingExecutableUtil.getCubeName(this.getParams()));
        final CubeInstance cubeForUpdate = cube.latestCopyForWrite();
        final CubeSegment cubeSeg = cubeForUpdate.getSegmentById(CubingExecutableUtil.getSegmentId(this.getParams()));
        final CubeDesc cubeDesc = cube.getDescriptor();
        final Configuration conf = HadoopUtil.getCurrentConfiguration();
        DictionaryInfoSerializer serializer = DictionaryInfoSerializer.FULL_SERIALIZER;

        final Set<TblColRef> colRefs = cubeDesc.getAllColumnsNeedDictionaryBuilt();
        Map<String, TblColRef> colRefMap = Maps.newHashMap();
        for (TblColRef colRef : colRefs) {
            colRefMap.put(colRef.getName(), colRef);
        }

        try {
            Path dictsDirPath = new Path(CubingExecutableUtil.getDictsPath(this.getParams()));
            Configuration hadoopConf = HadoopUtil.getCurrentConfiguration();
            FileSystem fs = FileSystem.get(hadoopConf);
            if (!fs.exists(dictsDirPath)) {
                throw new IOException("DictsFilePath " + dictsDirPath + " does not exists");
            }

            if (!fs.isDirectory(dictsDirPath)) {
                throw new IOException("DictsFilePath " + dictsDirPath + " is not a directory");
            }

            RemoteIterator<LocatedFileStatus> outputs = fs.listFiles(dictsDirPath, true);
            while (outputs.hasNext()) {
                logger.info("mapreduce out put file: {}", outputs.next().getPath());
            }

            FileStatus[] files = fs.listStatus(dictsDirPath, new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    //                return path.getName().contains("_" + BatchConstants.CFG_COLUMN_DICT_FILENAME);
                    //TODO: get the MR output file, to be enhanced
                    return path.getName().contains("-");

                }
            });

            for (FileStatus file : files) {
                try (SequenceFile.Reader reader = new SequenceFile.Reader(fs, file.getPath(), conf)) {
                    Text colName = new Text();
                    Text dictInfo = new Text();
                    while (reader.next(colName, dictInfo)) {
                        TblColRef colRef = colRefMap.get(colName.toString());
                        if (colRef == null) {
                            throw new IllegalArgumentException("Invalid column name " + colName
                                    + " or it need not build dictionary!");
                        }
                        DictionaryInfo dictionaryInfo = serializer.deserialize(new DataInputStream(
                                new ByteArrayInputStream(dictInfo.getBytes())));

                        Dictionary dict = dictionaryInfo.getDictionaryObject();
                        if (dict != null) {
                            dictionaryInfo = dictManager.trySaveNewDict(dict, dictionaryInfo);
                            cubeSeg.putDictResPath(colRef, dictionaryInfo.getResourcePath());
                            if (cubeSeg.getRowkeyStats() != null) {
                                cubeSeg.getRowkeyStats().add(
                                        new Object[]{colRef.getName(), dict.getSize(), dict.getSizeOfId()});
                            } else {
                                logger.error("rowkey_stats field not found!");
                            }
                        } else {
                            logger.error("dictionary of column {} not found! ", colRef.getName());
                        }
                    }
                }
            }
            CubeUpdate cubeBuilder = new CubeUpdate(cubeForUpdate);
            cubeBuilder.setToUpdateSegs(cubeSeg);
            mgr.updateCube(cubeBuilder);

            fs.delete(dictsDirPath, true);
            for (FileStatus fileElem : files) {
                fs.delete(fileElem.getPath(), true);
            }

            if (getIsNeedReleaseLock()) {
                releaseLock();
            }

            return new ExecuteResult();
        } catch (IOException e) {
            logger.error("fail to save cuboid dictionaries", e);
            return new ExecuteResult(State.ERROR, e.getLocalizedMessage(), e);
        }
    }

    @Override
    public void cleanup() throws ExecuteException {
        super.cleanup();
    }

    public void setIsNeedReleaseLock(Boolean isNeedReleaseLock) {
        setParam("isNeedReleaseLock", String.valueOf(isNeedReleaseLock));
    }

    public boolean getIsNeedReleaseLock() {
        String isNeedReleaseLock = getParam("isNeedReleaseLock");
        return Strings.isNullOrEmpty(isNeedReleaseLock) ? false : Boolean.parseBoolean(isNeedReleaseLock);
    }

    public void setLockPathName(String pathName) {
        setParam("lockPathName", pathName);
    }

    public String getLockPathName() {
        return getParam("lockPathName");
    }

    public void setJobFlowJobId(String jobId) {
        setParam("jobFlowJobId", jobId);
    }

    public String getJobFlowJobId() {
        return getParam("jobFlowJobId");
    }

    private void releaseLock() {
        DistributedLock lock = KylinConfig.getInstanceFromEnv().getDistributedLockFactory().lockForCurrentThread();
        String parentLockPath = getCubeJobLockParentPathName();
        String ephemeralLockPath = getEphemeralLockPathName();

        if (lock.isLocked(getCubeJobLockPathName())) {
            lock.purgeLocks(parentLockPath);
            logger.info("{} unlock full lock path :{} success", getId(), parentLockPath);
        }

        if (lock.isLocked(ephemeralLockPath)) {
            lock.purgeLocks(ephemeralLockPath);
            logger.info("{} unlock full lock path :{} success", getId(), ephemeralLockPath);
        }
    }

    private String getEphemeralLockPathName() {
        String pathName = getLockPathName();
        if (Strings.isNullOrEmpty(pathName)) {
            throw new IllegalArgumentException("cube job lock path name is null");
        }

        return CubeJobLockUtil.getEphemeralLockPath(pathName);
    }

    private String getCubeJobLockPathName() {
        String pathName = getLockPathName();
        if (Strings.isNullOrEmpty(pathName)) {
            throw new IllegalArgumentException("cube job lock path name is null");
        }

        String flowJobId = getJobFlowJobId();
        if (Strings.isNullOrEmpty(flowJobId)) {
            throw new IllegalArgumentException("cube job lock path flowJobId is null");
        }
        return CubeJobLockUtil.getLockPath(pathName, flowJobId);
    }

    private String getCubeJobLockParentPathName() {
        String pathName = getLockPathName();
        if (Strings.isNullOrEmpty(pathName)) {
            throw new IllegalArgumentException(" create mr hive dict lock path name is null");
        }
        return CubeJobLockUtil.getLockPath(pathName, null);
    }

}
