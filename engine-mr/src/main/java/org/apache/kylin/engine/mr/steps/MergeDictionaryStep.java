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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class MergeDictionaryStep extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(MergeDictionaryStep.class);

    public MergeDictionaryStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final CubeManager mgr = CubeManager.getInstance(context.getConfig());
        final CubeInstance cube = mgr.getCube(CubingExecutableUtil.getCubeName(this.getParams()));
        final CubeSegment newSegment = cube.getSegmentById(CubingExecutableUtil.getSegmentId(this.getParams()));
        final List<CubeSegment> mergingSegments = getMergingSegments(cube);
        KylinConfig conf = cube.getConfig();

        Collections.sort(mergingSegments);

        try {
            checkLookupSnapshotsMustIncremental(mergingSegments);

            // work on copy instead of cached objects
            CubeInstance cubeCopy = cube.latestCopyForWrite();
            CubeSegment newSegCopy = cubeCopy.getSegmentById(newSegment.getUuid());
            
            makeDictForNewSegment(conf, cubeCopy, newSegCopy, mergingSegments);
            makeSnapshotForNewSegment(cubeCopy, newSegCopy, mergingSegments);

            CubeUpdate update = new CubeUpdate(cubeCopy);
            update.setToUpdateSegs(newSegCopy);
            mgr.updateCube(update);
            return ExecuteResult.createSucceed();
        } catch (IOException e) {
            logger.error("fail to merge dictionary or lookup snapshots", e);
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

    private void checkLookupSnapshotsMustIncremental(List<CubeSegment> mergingSegments) {

        // FIXME check each newer snapshot has only NEW rows but no MODIFIED rows
    }

    /**
     * For the new segment, we need to create new dimension dictionaries by merging underlying
     * dictionaries. (https://issues.apache.org/jira/browse/KYLIN-2457, https://issues.apache.org/jira/browse/KYLIN-2800)
     * @param cube
     * @param newSeg
     * @throws IOException
     */
    private void makeDictForNewSegment(KylinConfig conf, CubeInstance cube, CubeSegment newSeg, List<CubeSegment> mergingSegments) throws IOException {
        DictionaryManager dictMgr = DictionaryManager.getInstance(conf);
        CubeDesc cubeDesc = cube.getDescriptor();

        for (TblColRef col : cubeDesc.getAllColumnsNeedDictionaryBuilt()) {
            logger.info("Merging fact table dictionary on : " + col);
            List<DictionaryInfo> dictInfos = new ArrayList<DictionaryInfo>();
            for (CubeSegment segment : mergingSegments) {
                logger.info("Including fact table dictionary of segment : " + segment);
                if (segment.getDictResPath(col) != null) {
                    DictionaryInfo dictInfo = dictMgr.getDictionaryInfo(segment.getDictResPath(col));
                    if (dictInfo != null && !dictInfos.contains(dictInfo)) {
                        dictInfos.add(dictInfo);
                    } else {
                        logger.warn("Failed to load DictionaryInfo from " + segment.getDictResPath(col));
                    }
                }
            }
            mergeDictionaries(dictMgr, newSeg, dictInfos, col);
        }
    }

    private DictionaryInfo mergeDictionaries(DictionaryManager dictMgr, CubeSegment cubeSeg, List<DictionaryInfo> dicts, TblColRef col) throws IOException {
        DictionaryInfo dictInfo = dictMgr.mergeDictionary(dicts);
        if (dictInfo != null)
            cubeSeg.putDictResPath(col, dictInfo.getResourcePath());

        return dictInfo;
    }

    /**
     * make snapshots for the new segment by copying from the latest one of the underlying
     * merging segments. It's guaranteed to be consistent under the assumption that lookup tables
     * would be either static or incremental.
     *
     * @param cube
     * @param newSeg
     */
    private void makeSnapshotForNewSegment(CubeInstance cube, CubeSegment newSeg, List<CubeSegment> mergingSegments) {
        CubeSegment lastSeg = mergingSegments.get(mergingSegments.size() - 1);
        for (Map.Entry<String, String> entry : lastSeg.getSnapshots().entrySet()) {
            newSeg.putSnapshotResPath(entry.getKey(), entry.getValue());
        }
    }

}
