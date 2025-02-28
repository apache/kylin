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

package org.apache.kylin.rec;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.metadata.model.NDataModel;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

@Ignore
@Slf4j
public class SmartMasterRetryTest extends NLocalWithSparkSessionTest {

    @Override
    public String getProject() {
        return "newten";
    }

    @Test
    public void transactionExceptionInNSmartMaster() throws ExecutionException, InterruptedException {
        String[] sqlArray1 = { "select item_count, sum(price) from test_kylin_fact group by item_count" };
        String[] sqlArray2 = { "select lstg_format_name, sum(price) from test_kylin_fact group by lstg_format_name" };

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Future<AbstractContext> submit1 = executorService.submit(() -> {
            Thread.currentThread().setName("First thread");
            return ProposerJob.proposeForAutoMode(getTestConfig(), getProject(), sqlArray1);
        });

        Future<AbstractContext> submit2 = executorService.submit(() -> {
            Thread.currentThread().setName("Second thread");
            return ProposerJob.proposeForAutoMode(getTestConfig(), getProject(), sqlArray2);
        });

        AbstractContext context1 = submit1.get();
        AbstractContext context2 = submit2.get();

        NDataModel targetModel1 = context1.getModelContexts().get(0).getTargetModel();
        NDataModel targetModel2 = context2.getModelContexts().get(0).getTargetModel();
        if (targetModel1.getMvcc() == 1) {
            Assert.assertTrue(targetModel1.getLastModified() > targetModel2.getLastModified());
            Assert.assertEquals(2, targetModel1.getEffectiveDimensions().size());
            Assert.assertEquals(0, targetModel2.getMvcc());
            Assert.assertEquals(1, targetModel2.getEffectiveDimensions().size());
        } else {
            Assert.assertEquals(1, targetModel1.getEffectiveDimensions().size());
            Assert.assertEquals(1, targetModel2.getMvcc());
            Assert.assertEquals(2, targetModel2.getEffectiveDimensions().size());
            Assert.assertTrue(targetModel1.getLastModified() < targetModel2.getLastModified());
        }
    }
}
