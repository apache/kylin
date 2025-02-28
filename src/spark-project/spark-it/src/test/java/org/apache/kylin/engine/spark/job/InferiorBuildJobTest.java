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

package org.apache.kylin.engine.spark.job;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.engine.spark.storage.ParquetStorage;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.storage.IStorage;
import org.apache.kylin.storage.IStorageQuery;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sparkproject.guava.collect.Sets;

import scala.Option;
import scala.runtime.AbstractFunction1;

@SuppressWarnings("serial")
public class InferiorBuildJobTest extends NLocalWithSparkSessionTest {

    private KylinConfig config;

    @Override
    public String getProject() {
        return "test_inferior_job";
    }

    @Before
    public void setUp() throws Exception {
        JobContextUtil.cleanUp();
        super.setUp();
        ss.sparkContext().setLogLevel("ERROR");
        overwriteSystemProp("kylin.engine.persist-flattable-threshold", "0");
        overwriteSystemProp("kylin.engine.persist-flatview", "true");

        config = getTestConfig();
        JobContextUtil.getJobContext(config);
    }

    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        super.tearDown();
    }

    @Test
    public void testBuildFromInferiorTable() throws Exception {

        final String conventionId = "bb4e7e15-06f5-519d-c36f-1af5d05f7b60";

        // prepare segment
        final NDataflowManager dfMgr = NDataflowManager.getInstance(config, getProject());
        cleanupSegments(dfMgr, conventionId);

        NDataflow df = dfMgr.getDataflow(conventionId);
        List<LayoutEntity> layoutList = df.getIndexPlan().getAllLayouts();

        indexDataConstructor.buildIndex(conventionId, SegmentRange.TimePartitionedSegmentRange.createInfinite(),
                Sets.newLinkedHashSet(layoutList), true);

        NDataflowManager dataflowManager = NDataflowManager.getInstance(config, getProject());
        NDataflow dataflow = dataflowManager.getDataflow(conventionId);
        Assert.assertEquals(64, dataflow.getFirstSegment().getSegDetails().getLayouts().size());
    }

    private void cleanupSegments(NDataflowManager dsMgr, String dfName) {
        NDataflow df = dsMgr.getDataflow(dfName);

        // cleanup all segments first
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
    }

    public static class MockParquetStorage extends ParquetStorage {

        @Override
        public Dataset<Row> getFrom(String path, SparkSession ss) {
            return super.getFrom(path, ss);
        }

        @Override
        public void saveTo(String path, Dataset<Row> data, SparkSession ss) {
            Option<LogicalPlan> option = data.queryExecution().optimizedPlan()
                    .find(new AbstractFunction1<LogicalPlan, Object>() {
                        @Override
                        public Object apply(LogicalPlan v1) {
                            return v1 instanceof Join;
                        }
                    });
            Assert.assertFalse(option.isDefined());
            super.saveTo(path, data, ss);
        }
    }

    public static class MockupStorageEngine implements IStorage {

        @Override
        public IStorageQuery createQuery(IRealization realization) {
            return null;
        }

        @Override
        public <I> I adaptToBuildEngine(Class<I> engineInterface) {
            Class clz;
            try {
                clz = Class.forName("org.apache.kylin.engine.spark.NSparkCubingEngine$NSparkCubingStorage");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            if (engineInterface == clz) {
                return (I) ClassUtil
                        .newInstance("NSparkCubingJobTest$MockParquetStorage");
            } else {
                throw new RuntimeException("Cannot adapt to " + engineInterface);
            }
        }
    }

}
