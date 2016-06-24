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

package org.apache.kylin.engine.mr.common;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;

import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 */
public class HadoopShellExecutable extends AbstractExecutable {
    private static final Logger logger = LoggerFactory.getLogger(HadoopShellExecutable.class);

    private static final String KEY_MR_JOB = "HADOOP_SHELL_JOB_CLASS";
    private static final String KEY_PARAMS = "HADOOP_SHELL_JOB_PARAMS";

    public HadoopShellExecutable() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final String mapReduceJobClass = getJobClass();
        String params = getJobParams();
        Preconditions.checkNotNull(mapReduceJobClass);
        Preconditions.checkNotNull(params);
        try {
            final Constructor<? extends AbstractHadoopJob> constructor = ClassUtil.forName(mapReduceJobClass, AbstractHadoopJob.class).getConstructor();
            final AbstractHadoopJob job = constructor.newInstance();
            String[] args = params.trim().split("\\s+");
            logger.info("parameters of the HadoopShellExecutable:");
            logger.info(params);
            int result;
            StringBuilder log = new StringBuilder();
            try {
                result = ToolRunner.run(job, args);
            } catch (Exception ex) {
                logger.error("error execute " + this.toString(), ex);
                StringWriter stringWriter = new StringWriter();
                ex.printStackTrace(new PrintWriter(stringWriter));
                log.append(stringWriter.toString()).append("\n");
                result = 2;
            }
            log.append("result code:").append(result);
            return result == 0 ? new ExecuteResult(ExecuteResult.State.SUCCEED, log.toString()) : new ExecuteResult(ExecuteResult.State.FAILED, log.toString());
        } catch (ReflectiveOperationException e) {
            logger.error("error getMapReduceJobClass, class name:" + getParam(KEY_MR_JOB), e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        } catch (Exception e) {
            logger.error("error execute " + this.toString(), e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }

    public void setJobClass(Class<? extends AbstractHadoopJob> clazzName) {
        setParam(KEY_MR_JOB, clazzName.getName());
    }

    public String getJobClass() throws ExecuteException {
        return getParam(KEY_MR_JOB);
    }

    public void setJobParams(String param) {
        setParam(KEY_PARAMS, param);
    }

    public String getJobParams() {
        return getParam(KEY_PARAMS);
    }

}
