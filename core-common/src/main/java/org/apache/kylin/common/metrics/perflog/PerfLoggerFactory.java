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

package org.apache.kylin.common.metrics.perflog;

import org.apache.commons.lang3.ClassUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.threadlocal.InternalThreadLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerfLoggerFactory {

    protected static final InternalThreadLocal<IPerfLogger> perfLogger = new InternalThreadLocal<IPerfLogger>();
    static final private Logger LOG = LoggerFactory.getLogger(PerfLoggerFactory.class.getName());

    public static IPerfLogger getPerfLogger() {
        return getPerfLogger(false);
    }

    public static void setPerfLogger(IPerfLogger iPerfLogger) {
        perfLogger.set(iPerfLogger);
    }

    public static IPerfLogger getPerfLogger(boolean resetPerfLogger) {
        IPerfLogger result = perfLogger.get();
        if (resetPerfLogger || result == null) {
            try {
                result = (IPerfLogger) ClassUtils.getClass(KylinConfig.getInstanceFromEnv().getPerfLoggerClassName())
                        .newInstance();
            } catch (ClassNotFoundException e) {
                LOG.error("Performance Logger Class not found:" + e.getMessage());
                result = new SimplePerfLogger();
            } catch (IllegalAccessException | InstantiationException e) {
                e.printStackTrace();
            }
            perfLogger.set(result);
        }
        return result;
    }

}
