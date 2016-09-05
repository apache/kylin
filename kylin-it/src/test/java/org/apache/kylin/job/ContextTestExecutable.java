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

package org.apache.kylin.job;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.impl.threadpool.DefaultContext;

public class ContextTestExecutable extends AbstractExecutable {
    public ContextTestExecutable() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {

        DefaultContext defaultContext = (DefaultContext) context;
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        if (getHashCode(defaultContext.getConfig()) == getHashCode(KylinConfig.getInstanceFromEnv())) {
            return new ExecuteResult(ExecuteResult.State.SUCCEED, "succeed");
        } else {
            return new ExecuteResult(ExecuteResult.State.ERROR, "error");
        }
    }

    private int getHashCode(KylinConfig config) {
        return System.identityHashCode(config);
    }
}
