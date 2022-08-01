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
package org.apache.kylin.query.engine;

import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.ForceToTieredStorage;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.query.engine.exec.ExecuteResult;
import org.apache.kylin.query.engine.exec.sparder.QueryEngine;
import org.apache.kylin.query.engine.exec.sparder.SparderQueryPlanExec;
import org.apache.kylin.query.engine.meta.MutableDataContext;
import org.apache.spark.SparkException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import lombok.SneakyThrows;

import java.sql.SQLException;

public class SparderQueryPlanExecAbnormalTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    static class ThrowForceToTieredStorageException implements ThrownExceptionEngine.EngineAction {
        ThrowForceToTieredStorageException() {
        }

        @SneakyThrows
        @Override
        public boolean apply() {
            return ExceptionUtils.rethrow(new SQLException(QueryContext.ROUTE_USE_FORCEDTOTIEREDSTORAGE));
        }
    }

    static class ThrowExceptionAtFirstTime implements ThrownExceptionEngine.EngineAction {
        public int callNumber = 0;
        private final boolean setSecondStorageUsageMap;

        ThrowExceptionAtFirstTime(boolean setSecondStorageUsageMap) {
            this.setSecondStorageUsageMap = setSecondStorageUsageMap;
        }

        @SneakyThrows
        @Override
        public boolean apply() {
            callNumber++;
            if (callNumber == 1) {
                if (setSecondStorageUsageMap) {
                    QueryContext.current().getSecondStorageUsageMap().put(1L, true);
                }
                throw new SparkException("");
            }
            return true;
        }
    }

    static class TestSparderQueryPlanExec extends SparderQueryPlanExec {
        private QueryEngine engine;

        TestSparderQueryPlanExec(QueryEngine engine) {
            this.engine = engine;
        }

        public void updateEngine(QueryEngine engine) {
            this.engine = engine;
        }

        @Override
        public ExecuteResult executeToIterable(RelNode rel, MutableDataContext dataContext) {
            return internalCompute(engine, dataContext, rel);
        }
    }

    @After
    public void tearDown() {
        QueryContext.reset();
    }

    @Test
    public void testQueryRouteWithSecondStorage() {
        ThrowExceptionAtFirstTime throwExceptionAtFirstTime = new ThrowExceptionAtFirstTime(true);
        ThrownExceptionEngine engine = new ThrownExceptionEngine(throwExceptionAtFirstTime);
        TestSparderQueryPlanExec exec = new TestSparderQueryPlanExec(engine);
        QueryContext.current().setRetrySecondStorage(false);
        Assert.assertNull(exec.executeToIterable(null, null).getRows());
        Assert.assertEquals(2, throwExceptionAtFirstTime.callNumber);
        Assert.assertTrue(QueryContext.current().isForceTableIndex());
        Assert.assertTrue(QueryContext.current().getSecondStorageUsageMap().isEmpty());

        //Now QueryContext.current().isForceTableIndex() == true
        ThrowExceptionAtFirstTime throwExceptionAtFirstTime2 = new ThrowExceptionAtFirstTime(true);
        ThrownExceptionEngine engine2 = new ThrownExceptionEngine(throwExceptionAtFirstTime2);
        exec.updateEngine(engine2);

        thrown.expect(SparkException.class);
        try {
            exec.executeToIterable(null, null);
        } finally {
            Assert.assertEquals(1, throwExceptionAtFirstTime2.callNumber);
            Assert.assertTrue(QueryContext.current().isForceTableIndex());
            Assert.assertTrue(QueryContext.current().getSecondStorageUsageMap().size() > 0);
        }
    }

    @Test
    public void testQueryRouteWithoutSecondStorage() {
        ThrowExceptionAtFirstTime throwExceptionAtFirstTime = new ThrowExceptionAtFirstTime(false);
        ThrownExceptionEngine engine = new ThrownExceptionEngine(throwExceptionAtFirstTime);
        TestSparderQueryPlanExec exec = new TestSparderQueryPlanExec(engine);

        thrown.expect(SparkException.class);
        try {
            exec.executeToIterable(null, null);
        } finally {
            Assert.assertEquals(1, throwExceptionAtFirstTime.callNumber);
            Assert.assertFalse(QueryContext.current().isForceTableIndex());
            Assert.assertEquals(0, QueryContext.current().getSecondStorageUsageMap().size());
        }
    }

    @Test(expected = SQLException.class)
    public void testQueryRouteWithForceToTieredStoragePushDown() {
        ThrowForceToTieredStorageException throwExceptionAtFirstTime = new ThrowForceToTieredStorageException();
        ThrownExceptionEngine engine = new ThrownExceptionEngine(throwExceptionAtFirstTime);
        TestSparderQueryPlanExec exec = new TestSparderQueryPlanExec(engine);
        QueryContext.current().setForcedToTieredStorage(ForceToTieredStorage.CH_FAIL_TO_PUSH_DOWN);
        Assert.assertNull(exec.executeToIterable(null, null).getRows());
    }

    @Test(expected = KylinException.class)
    public void testQueryRouteWithForceToTieredStorageReturn() {
        ThrowForceToTieredStorageException throwExceptionAtFirstTime = new ThrowForceToTieredStorageException();
        ThrownExceptionEngine engine = new ThrownExceptionEngine(throwExceptionAtFirstTime);
        TestSparderQueryPlanExec exec = new TestSparderQueryPlanExec(engine);
        QueryContext.current().setForcedToTieredStorage(ForceToTieredStorage.CH_FAIL_TO_RETURN);
        Assert.assertNull(exec.executeToIterable(null, null).getRows());
    }

    @Test(expected = KylinException.class)
    public void testQueryRouteWithForceToTieredStorageInvalid() {
        ThrowForceToTieredStorageException throwExceptionAtFirstTime = new ThrowForceToTieredStorageException();
        ThrownExceptionEngine engine = new ThrownExceptionEngine(throwExceptionAtFirstTime);
        TestSparderQueryPlanExec exec = new TestSparderQueryPlanExec(engine);
        QueryContext.current().setForcedToTieredStorage(ForceToTieredStorage.CH_FAIL_TO_PUSH_DOWN);
        QueryContext.current().setForceTableIndex(true);
        Assert.assertNull(exec.executeToIterable(null, null).getRows());
    }

    @Test(expected = KylinException.class)
    public void testQueryRouteWithForceToTieredStorageOther() {
        ThrowForceToTieredStorageException throwExceptionAtFirstTime = new ThrowForceToTieredStorageException();
        ThrownExceptionEngine engine = new ThrownExceptionEngine(throwExceptionAtFirstTime);
        TestSparderQueryPlanExec exec = new TestSparderQueryPlanExec(engine);
        QueryContext.current().setForcedToTieredStorage(ForceToTieredStorage.CH_FAIL_TAIL);
        Assert.assertNull(exec.executeToIterable(null, null).getRows());
    }

}
