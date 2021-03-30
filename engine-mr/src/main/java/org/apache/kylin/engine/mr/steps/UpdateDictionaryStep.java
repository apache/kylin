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

package org.apache.kylin.engine.mr.steps;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateDictionaryStep extends AbstractExecutable {
    private static final Logger logger = LoggerFactory.getLogger(UpdateDictionaryStep.class);

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final CubeManager cubeMgr = CubeManager.getInstance(context.getConfig());
        final DictionaryManager dictMgrHdfs;
        final DictionaryManager dictMgrHbase;
        final CubeInstance cube = cubeMgr.getCube(CubingExecutableUtil.getCubeName(this.getParams()));
        final CubeSegment newSegment = cube.getSegmentById(CubingExecutableUtil.getSegmentId(this.getParams()));
        final List<CubeSegment> mergingSegments = getMergingSegments(cube);
        final String dictInfoPath = this.getParams().get(BatchConstants.ARG_DICT_PATH);
        final String metadataUrl = this.getParams().get(BatchConstants.ARG_META_URL);

        final KylinConfig kylinConfHbase = cube.getConfig();
        final KylinConfig kylinConfHdfs = AbstractHadoopJob.loadKylinConfigFromHdfs(metadataUrl);

        Collections.sort(mergingSegments);

        try {
            Configuration conf = HadoopUtil.getCurrentConfiguration();
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            ResourceStore hbaseRS = ResourceStore.getStore(kylinConfHbase);
            ResourceStore hdfsRS = ResourceStore.getStore(kylinConfHdfs);
            dictMgrHdfs = DictionaryManager.getInstance(kylinConfHdfs);
            dictMgrHbase = DictionaryManager.getInstance(kylinConfHbase);

            // work on copy instead of cached objects
            CubeInstance cubeCopy = cube.latestCopyForWrite();
            CubeSegment newSegCopy = cubeCopy.getSegmentById(newSegment.getUuid());

            // update cube segment dictionary

            FileStatus[] fileStatuss = fs.listStatus(new Path(dictInfoPath), new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    return path.getName().startsWith("part") || path.getName().startsWith("tmp");
                }
            });

            for (FileStatus fileStatus : fileStatuss) {
                Path filePath = fileStatus.getPath();

                SequenceFile.Reader reader = new SequenceFile.Reader(fs, filePath, conf);
                Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                Text value = (Text) ReflectionUtils.newInstance(reader.getValueClass(), conf);

                while (reader.next(key, value)) {
                    String tblCol = key.toString();
                    String dictInfoResource = value.toString();

                    if (StringUtils.isNotEmpty(dictInfoResource)) {
                        logger.info(dictInfoResource);
                        // put dictionary file to metadata store
                        DictionaryInfo dictInfoHdfs = dictMgrHdfs.getDictionaryInfo(dictInfoResource);
                        DictionaryInfo dicInfoHbase = dictMgrHbase.trySaveNewDict(dictInfoHdfs.getDictionaryObject(), dictInfoHdfs);

                        if (dicInfoHbase != null){
                            TblColRef tblColRef = cube.getDescriptor().findColumnRef(tblCol.split(":")[0], tblCol.split(":")[1]);
                            newSegCopy.putDictResPath(tblColRef, dicInfoHbase.getResourcePath());
                        }
                    }
                }

                IOUtils.closeStream(reader);
            }

            CubeSegment lastSeg = mergingSegments.get(mergingSegments.size() - 1);
            for (Map.Entry<String, String> entry : lastSeg.getSnapshots().entrySet()) {
                newSegCopy.putSnapshotResPath(entry.getKey(), entry.getValue());
            }

            // update statistics
            // put the statistics to metadata store
            String statisticsFileName = newSegment.getStatisticsResourcePath();
            hbaseRS.putResource(statisticsFileName, hdfsRS.getResource(newSegment.getStatisticsResourcePath()).content(), System.currentTimeMillis());

            CubeUpdate update = new CubeUpdate(cubeCopy);
            update.setToUpdateSegs(newSegCopy);
            cubeMgr.updateCube(update);

            return ExecuteResult.createSucceed();
        } catch (IOException e) {
            logger.error("fail to merge dictionary", e);
            return ExecuteResult.createError(e);
        }
    }

    private List<CubeSegment> getMergingSegments(CubeInstance cube) {
        List<String> mergingSegmentIds = CubingExecutableUtil.getMergingSegmentIds(this.getParams());
        List<CubeSegment> result = Lists.newArrayListWithCapacity(mergingSegmentIds.size());
        for (String id : mergingSegmentIds) {
            result.add(cube.getSegmentById(id));
        }
        return result;
    }
}
