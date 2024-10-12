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

package org.apache.kylin.query.routing;

import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.metadata.cube.model.LayoutPartition;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.model.MultiPartitionDesc;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.query.exception.UserStopQueryException;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.util.SlowQueryDetector;
import org.apache.kylin.util.OlapContextTestUtil;
import org.junit.Assert;
import org.junit.Test;

public class PartitionPruningRuleTest extends NLocalWithSparkSessionTest {

    private Candidate prepareCandidate() throws SqlParseException {
        String project = "multi_level_partition";
        String modelId = "747f864b-9721-4b97-acde-0aa8e8656cba";
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), project);

        String sql = "select cal_dt, sum(price) from test_kylin_fact" + " inner join test_account"
                + " on test_kylin_fact.seller_id = test_account.account_id"
                + " where cal_dt > '2012-01-01' and cal_dt < '2012-01-04' and lstg_site_id = 1 group by cal_dt";
        OlapContext olapContext = OlapContextTestUtil.getOlapContexts(project, sql, true).get(0);

        int newPartitionsNum = 100_000;

        modelManager.updateDataModel(modelId, copied -> {
            for (int i = 0; i < newPartitionsNum; i++) {
                copied.getMultiPartitionDesc().getPartitions()
                        .add(new MultiPartitionDesc.PartitionInfo(i + 100, new String[] { String.valueOf(i + 100) }));
            }
        });

        NDataflowManager dataflowManager = NDataflowManager.getInstance(getTestConfig(), project);
        NDataflow dataflowCopied = dataflowManager.getDataflow(modelId).copy();

        // append new partitions
        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(modelId);
        List<NDataLayout> toUpdateLayouts = new ArrayList<>();
        for (NDataSegment segment : dataflowCopied.getSegments()) {
            segment = segment.copy();
            toUpdateLayouts.add(segment.getLayout(1));
            for (int i = 0; i < newPartitionsNum; i++) {
                LayoutPartition layoutPartition = new LayoutPartition(i + 100);
                layoutPartition.setBucketId(i + 100);
                segment.getLayout(1).getMultiPartition().add(layoutPartition);
            }
        }
        dataflowUpdate.setToAddOrUpdateLayouts(toUpdateLayouts.toArray(new NDataLayout[0]));
        dataflowManager.updateDataflow(dataflowUpdate);

        NDataflow dataflow = dataflowManager.getDataflow(modelId);
        Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
        olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);

        Candidate candidate = new Candidate(dataflow, olapContext, sqlAlias2ModelNameMap);
        candidate.getQueryableSeg()
                .setBatchSegments(ImmutableList.of(dataflow.getSegment("8892fa3f-f607-4eec-8159-7c5ae2f16942")));
        return candidate;
    }

    @Test
    public void testCancelAndInterruptPruning() throws SqlParseException {
        Candidate candidate = prepareCandidate();
        testCancelQuery(candidate);
        testCancelQuery(candidate, queryEntry -> queryEntry.getPlannerCancelFlag().requestCancel());
        testCancelQuery(candidate, queryEntry -> queryEntry.setStopByUser(true));
        testCancelAsyncQuery(candidate);
        testInterrupt(candidate);
        testTimeout(candidate);
    }

    private void testCancelQuery(Candidate candidate) {
        AtomicReference<Exception> exp = new AtomicReference<>(null);
        AtomicBoolean res = new AtomicBoolean(false);
        AtomicReference<SlowQueryDetector> slowQueryDetector = new AtomicReference<>(null);
        AtomicReference<SlowQueryDetector.QueryEntry> queryEntry = new AtomicReference<>(null);

        Thread t = new Thread(() -> {
            try {
                slowQueryDetector.set(new SlowQueryDetector(100, 10_000));
                slowQueryDetector.get().queryStart("pruning");
                queryEntry.set(SlowQueryDetector.getRunningQueries().get(Thread.currentThread()));
                PartitionPruningRule rule = new PartitionPruningRule();
                rule.apply(candidate);
            } catch (Exception e) {
                exp.set(e);
            } finally {
                slowQueryDetector.get().queryEnd();
            }
        });
        t.start();
        await().pollInterval(10, TimeUnit.MILLISECONDS).atMost(60, TimeUnit.SECONDS)
                .until(() -> queryEntry.get() != null);

        slowQueryDetector.get().stopQuery("pruning");

        Assert.assertFalse(queryEntry.get().isAsyncQuery());
        Assert.assertTrue(
                queryEntry.get().isStopByUser() && queryEntry.get().getPlannerCancelFlag().isCancelRequested());

        try {
            t.join();
        } catch (InterruptedException e) {
            // ignored
        }

        await().pollInterval(10, TimeUnit.MILLISECONDS).atMost(5, TimeUnit.SECONDS).until(() -> exp.get() != null);

        Assert.assertFalse(res.get());
        Assert.assertTrue("Unexpected exception " + exp.get().getMessage(),
                exp.get() instanceof UserStopQueryException);
    }

    private void testCancelQuery(Candidate candidate, Consumer<SlowQueryDetector.QueryEntry> updater) {
        AtomicReference<Exception> exp = new AtomicReference<>(null);
        AtomicBoolean res = new AtomicBoolean(false);
        AtomicReference<SlowQueryDetector> slowQueryDetector = new AtomicReference<>(null);
        AtomicReference<SlowQueryDetector.QueryEntry> queryEntry = new AtomicReference<>(null);

        Thread t = new Thread(() -> {
            try {
                slowQueryDetector.set(new SlowQueryDetector(100, 10_000));
                slowQueryDetector.get().queryStart("pruning");
                queryEntry.set(SlowQueryDetector.getRunningQueries().get(Thread.currentThread()));
                PartitionPruningRule rule = new PartitionPruningRule();
                rule.apply(candidate);
            } catch (Exception e) {
                exp.set(e);
            } finally {
                slowQueryDetector.get().queryEnd();
            }
        });
        t.start();
        await().pollInterval(10, TimeUnit.MILLISECONDS).atMost(60, TimeUnit.SECONDS)
                .until(() -> queryEntry.get() != null);

        Assert.assertTrue(t.isAlive());

        updater.accept(queryEntry.get());

        try {
            t.join();
        } catch (InterruptedException e) {
            // ignored
        }

        await().pollInterval(10, TimeUnit.MILLISECONDS).atMost(5, TimeUnit.SECONDS).until(() -> exp.get() != null);

        Assert.assertFalse(res.get());
        Assert.assertTrue("Unexpected exception " + exp.get().getMessage(),
                exp.get() instanceof UserStopQueryException);
        Assert.assertTrue(exp.get().getMessage().contains("inconsistent states"));
    }

    private void testCancelAsyncQuery(Candidate candidate) {
        AtomicReference<Exception> exp = new AtomicReference<>(null);
        AtomicBoolean res = new AtomicBoolean(false);
        AtomicReference<SlowQueryDetector> slowQueryDetector = new AtomicReference<>(null);
        AtomicReference<SlowQueryDetector.QueryEntry> queryEntry = new AtomicReference<>(null);

        Thread t = new Thread(() -> {
            try {
                QueryContext.current().getQueryTagInfo().setAsyncQuery(true);
                slowQueryDetector.set(new SlowQueryDetector(100, 10_000));
                slowQueryDetector.get().queryStart("pruning");
                queryEntry.set(SlowQueryDetector.getRunningQueries().get(Thread.currentThread()));
                PartitionPruningRule rule = new PartitionPruningRule();
                rule.apply(candidate);
                res.set(true);
            } catch (Exception e) {
                exp.set(e);
            } finally {
                slowQueryDetector.get().queryEnd();
            }
        });
        t.start();

        await().pollInterval(10, TimeUnit.MILLISECONDS).atMost(60, TimeUnit.SECONDS)
                .until(() -> queryEntry.get() != null);

        String queryId = queryEntry.get().getQueryId();
        slowQueryDetector.get().stopQuery(queryId);

        Assert.assertTrue(queryEntry.get().isAsyncQuery());
        Assert.assertTrue(
                queryEntry.get().isStopByUser() && queryEntry.get().getPlannerCancelFlag().isCancelRequested());
        try {
            t.join();
        } catch (InterruptedException e) {
            // ignored
        }

        await().pollInterval(10, TimeUnit.MILLISECONDS).atMost(5, TimeUnit.SECONDS).until(() -> exp.get() != null);

        Assert.assertFalse(res.get());
        Assert.assertTrue("Unexpected exception " + exp.get().getMessage(),
                exp.get() instanceof UserStopQueryException);
    }

    private void testInterrupt(Candidate candidate) {
        AtomicReference<Exception> exp = new AtomicReference<>(null);
        AtomicBoolean res = new AtomicBoolean(false);

        Thread t = new Thread(() -> {
            try {
                PartitionPruningRule rule = new PartitionPruningRule();
                rule.apply(candidate);
                res.set(true);
            } catch (Exception e) {
                exp.set(e);
            }
        });
        t.start();

        await().pollInterval(10, TimeUnit.MILLISECONDS).atMost(60, TimeUnit.SECONDS).until(t::isAlive);

        t.interrupt();

        try {
            t.join();
        } catch (InterruptedException e) {
            // ignored
        }

        await().pollInterval(10, TimeUnit.MILLISECONDS).atMost(5, TimeUnit.SECONDS).until(() -> exp.get() != null);

        Assert.assertFalse(res.get());
        Assert.assertTrue("Unexpected exception " + exp.get().getMessage(), exp.get() instanceof KylinRuntimeException);
        Assert.assertTrue(exp.get().getCause() instanceof InterruptedException);
    }

    private void testTimeout(Candidate candidate) {
        AtomicReference<Exception> exp = new AtomicReference<>(null);
        AtomicBoolean res = new AtomicBoolean(false);
        AtomicReference<SlowQueryDetector> slowQueryDetector = new AtomicReference<>(null);
        AtomicReference<SlowQueryDetector.QueryEntry> queryEntry = new AtomicReference<>(null);

        Thread t = new Thread(() -> {
            try {
                QueryContext.current().getQueryTagInfo().setAsyncQuery(false);
                slowQueryDetector.set(new SlowQueryDetector(10, 100));
                slowQueryDetector.get().queryStart("pruning");
                queryEntry.set(SlowQueryDetector.getRunningQueries().get(Thread.currentThread()));
                PartitionPruningRule rule = new PartitionPruningRule();
                rule.apply(candidate);
                res.set(true);
            } catch (Exception e) {
                exp.set(e);
            } finally {
                slowQueryDetector.get().queryEnd();
            }
        });
        t.start();

        await().pollInterval(10, TimeUnit.MILLISECONDS).pollDelay(500, TimeUnit.MILLISECONDS)
                .atMost(60, TimeUnit.SECONDS).until(() -> queryEntry.get() != null);

        Assert.assertTrue(queryEntry.get().setInterruptIfTimeout());

        try {
            t.join();
        } catch (InterruptedException e) {
            // ignored
        }

        Assert.assertTrue(!t.isAlive() && queryEntry.get().getPlannerCancelFlag().isCancelRequested());

        await().pollInterval(10, TimeUnit.MILLISECONDS).atMost(5, TimeUnit.SECONDS).until(() -> exp.get() != null);

        Assert.assertFalse(res.get());
        Assert.assertTrue("Unexpected exception " + exp.get().getMessage(), exp.get() instanceof KylinTimeoutException);
    }
}
