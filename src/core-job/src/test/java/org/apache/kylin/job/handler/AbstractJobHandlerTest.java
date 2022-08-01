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

package org.apache.kylin.job.handler;

import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import lombok.val;

public class AbstractJobHandlerTest extends NLocalFileMetadataTestCase {

    public static final String DEFAULT_PROJECT = "default";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    static class DummyJobHandler extends AbstractJobHandler {

        @Override
        protected AbstractExecutable createJob(final JobParam jobParam) {
            return null;
        }
    }

    static class DummyJobHandlerWithMLP extends AbstractJobHandler {

        @Override
        protected boolean needComputeJobBucket() {
            return false;
        }

        @Override
        protected AbstractExecutable createJob(final JobParam jobParam) {
            return null;
        }
    }

    @Test
    public void doHandle() {
        val jobHandler = new DummyJobHandler();
        jobHandler.doHandle(new JobParam());
    }

    @Test
    public void doHandleWithMLP() {
        val jobHandler = new DummyJobHandlerWithMLP();
        jobHandler.doHandle(new JobParam());
    }
}
