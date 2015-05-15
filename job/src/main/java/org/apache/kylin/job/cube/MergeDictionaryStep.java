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

package org.apache.kylin.job.cube;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.model.TblColRef;

import java.io.IOException;
import java.util.*;

public class MergeDictionaryStep extends AbstractExecutable {

    private static final String CUBE_NAME = "cubeName";
    private static final String SEGMENT_ID = "segmentId";
    private static final String MERGING_SEGMENT_IDS = "mergingSegmentIds";

    public MergeDictionaryStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        KylinConfig conf = context.getConfig();
        final CubeManager mgr = CubeManager.getInstance(conf);
        final CubeInstance cube = mgr.getCube(getCubeName());
        final CubeSegment newSegment = cube.getSegmentById(getSegmentId());
        final List<CubeSegment> mergingSegments = getMergingSegments(cube);
        
        Collections.sort(mergingSegments);
        
        try {
            checkLookupSnapshotsMustIncremental(mergingSegments);
            
            makeDictForNewSegment(conf, cube, newSegment, mergingSegments);
            makeSnapshotForNewSegment(cube, newSegment, mergingSegments);
            
            mgr.updateCube(cube,false);
            return new ExecuteResult(ExecuteResult.State.SUCCEED, "succeed");
        } catch (IOException e) {
            logger.error("fail to merge dictionary or lookup snapshots", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }
    
    private List<CubeSegment> getMergingSegments(CubeInstance cube) {
        List<String> mergingSegmentIds = getMergingSegmentIds();
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
     * For the new segment, we need to create dictionaries for it, too. For
     * those dictionaries on fact table, create it by merging underlying
     * dictionaries For those dictionaries on lookup table, just copy it from
     * any one of the merging segments, it's guaranteed to be consistent(checked
     * in CubeSegmentValidator)
     *
     * @param cube
     * @param newSeg
     * @throws IOException
     */
    private void makeDictForNewSegment(KylinConfig conf, CubeInstance cube, CubeSegment newSeg, List<CubeSegment> mergingSegments) throws IOException {
        HashSet<TblColRef> colsNeedMeringDict = new HashSet<TblColRef>();
        HashSet<TblColRef> colsNeedCopyDict = new HashSet<TblColRef>();
        DictionaryManager dictMgr = DictionaryManager.getInstance(conf);

        CubeDesc cubeDesc = cube.getDescriptor();
        for (DimensionDesc dim : cubeDesc.getDimensions()) {
            for (TblColRef col : dim.getColumnRefs()) {
                if (newSeg.getCubeDesc().getRowkey().isUseDictionary(col)) {
                    String dictTable = (String) dictMgr.decideSourceData(cubeDesc.getModel(), cubeDesc.getRowkey().getDictionary(col), col, null)[0];
                    if (cubeDesc.getFactTable().equalsIgnoreCase(dictTable)) {
                        colsNeedMeringDict.add(col);
                    } else {
                        colsNeedCopyDict.add(col);
                    }
                }
            }
        }

        for (TblColRef col : colsNeedMeringDict) {
            logger.info("Merging fact table dictionary on : " + col);
            List<DictionaryInfo> dictInfos = new ArrayList<DictionaryInfo>();
            for (CubeSegment segment : mergingSegments) {
                logger.info("Including fact table dictionary of segment : " + segment);
                DictionaryInfo dictInfo = dictMgr.getDictionaryInfo(segment.getDictResPath(col));
                dictInfos.add(dictInfo);
            }
            mergeDictionaries(dictMgr, newSeg, dictInfos, col);
        }

        for (TblColRef col : colsNeedCopyDict) {
            String path = mergingSegments.get(0).getDictResPath(col);
            newSeg.putDictResPath(col, path);
        }
    }

    private DictionaryInfo mergeDictionaries(DictionaryManager dictMgr, CubeSegment cubeSeg, List<DictionaryInfo> dicts, TblColRef col) throws IOException {
        DictionaryInfo dictInfo = dictMgr.mergeDictionary(dicts);
        cubeSeg.putDictResPath(col, dictInfo.getResourcePath());

        return dictInfo;
    }

    /**
     * make snapshots for the new segment by copying from one of the underlying
     * merging segments. it's guaranteed to be consistent(checked in
     * CubeSegmentValidator)
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

    public void setCubeName(String cubeName) {
        this.setParam(CUBE_NAME, cubeName);
    }

    private String getCubeName() {
        return getParam(CUBE_NAME);
    }

    public void setSegmentId(String segmentId) {
        this.setParam(SEGMENT_ID, segmentId);
    }

    private String getSegmentId() {
        return getParam(SEGMENT_ID);
    }

    public void setMergingSegmentIds(List<String> ids) {
        setParam(MERGING_SEGMENT_IDS, StringUtils.join(ids, ","));
    }

    private List<String> getMergingSegmentIds() {
        final String ids = getParam(MERGING_SEGMENT_IDS);
        if (ids != null) {
            final String[] splitted = StringUtils.split(ids, ",");
            ArrayList<String> result = Lists.newArrayListWithExpectedSize(splitted.length);
            for (String id: splitted) {
                result.add(id);
            }
            return result;
        } else {
            return Collections.emptyList();
        }
    }

}
