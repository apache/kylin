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

package org.apache.kylin.engine.spark;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.engine.spark.job.NSparkMergingJob;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.query.util.SlowQueryDetector;
import org.junit.After;
import org.junit.Before;
import org.sparkproject.guava.collect.Sets;

public class NLocalWithSparkSessionTest extends NLocalWithSparkSessionTestBase {

    protected IndexDataConstructor indexDataConstructor = new IndexDataConstructor(getProject());

    @Before
    public void setUp() throws Exception {
        super.setUp();
        indexDataConstructor = new IndexDataConstructor(getProject());
        SlowQueryDetector.getRunningQueries().clear();
        ContextUtil.clearThreadLocalContexts();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
        SlowQueryDetector.getRunningQueries().clear();
        ContextUtil.clearThreadLocalContexts();
    }

    protected void fullBuild(String dfName) throws Exception {
        indexDataConstructor.buildDataflow(dfName);
    }

    public void buildMultiSegs(String dfName, long... layoutID) throws Exception {
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dsMgr.getDataflow(dfName);
        List<LayoutEntity> layouts = new ArrayList<>();
        IndexPlan indexPlan = df.getIndexPlan();
        if (layoutID.length == 0) {
            layouts = indexPlan.getAllLayouts();
        } else {
            for (long id : layoutID) {
                layouts.add(indexPlan.getLayoutEntity(id));
            }
        }
        long start = SegmentRange.dateToLong("2009-01-01 00:00:00");
        long end = SegmentRange.dateToLong("2011-01-01 00:00:00");
        indexDataConstructor.buildIndex(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.newLinkedHashSet(layouts), true);

        start = SegmentRange.dateToLong("2011-01-01 00:00:00");
        end = SegmentRange.dateToLong("2013-01-01 00:00:00");
        indexDataConstructor.buildIndex(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.newLinkedHashSet(layouts), true);

        start = SegmentRange.dateToLong("2013-01-01 00:00:00");
        end = SegmentRange.dateToLong("2015-01-01 00:00:00");
        indexDataConstructor.buildIndex(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.newLinkedHashSet(layouts), true);
    }

    public void buildMultiSegAndMerge(String dfName, long... layoutID) throws Exception {
        buildMultiSegs(dfName, layoutID);
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dsMgr.getDataflow(dfName);
        List<LayoutEntity> layouts = new ArrayList<>();
        IndexPlan indexPlan = df.getIndexPlan();
        if (layoutID.length == 0) {
            layouts = indexPlan.getAllLayouts();
        } else {
            for (long id : layoutID) {
                layouts.add(indexPlan.getLayoutEntity(id));
            }
        }
        mergeSegments(dfName, Sets.newLinkedHashSet(layouts));
    }

    public void mergeSegments(String dfName, Set<LayoutEntity> toBuildLayouts) throws Exception {
        NDataSegment firstMergeSeg = UnitOfWork.doInTransactionWithRetry(() -> {
            NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
            NDataflow df = dsMgr.getDataflow(dfName);
            return dsMgr.mergeSegments(df,
                    new SegmentRange.TimePartitionedSegmentRange(SegmentRange.dateToLong("2011-01-01 00:00:00"),
                            SegmentRange.dateToLong("2015-01-01 00:00:00")),
                    false);
        }, getProject());
        NSparkMergingJob job = NSparkMergingJob.merge(firstMergeSeg, Sets.newLinkedHashSet(toBuildLayouts), "ADMIN",
                RandomUtil.randomUUIDStr());

        ExecutableManager execMgr = ExecutableManager.getInstance(getTestConfig(), getProject());
        // launch the job
        execMgr.addJob(job);

        if (!Objects.equals(IndexDataConstructor.wait(job), ExecutableState.SUCCEED)) {
            throw new IllegalStateException(IndexDataConstructor.firstFailedJobErrorMessage(execMgr, job));
        }
    }

    public void exchangeStorageType(String project, int storageType) {
        final NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), project);
        modelManager.listAllModelIds().forEach(modelId -> {
            modelManager.updateDataModel(modelId, copyForWrite -> {
                copyForWrite.setStorageType(storageType);
            });
        });
    }

    public void cleanupSegments(String dfName, String project) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowManager dsMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            NDataflow df = dsMgr.getDataflow(dfName);
            NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
            update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
            return dsMgr.updateDataflow(update);
        }, project);
    }
}
