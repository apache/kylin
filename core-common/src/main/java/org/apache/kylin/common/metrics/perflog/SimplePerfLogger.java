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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimplePerfLogger implements IPerfLogger {

    static final private Logger LOG = LoggerFactory.getLogger(SimplePerfLogger.class.getName());
    protected final Map<String, Long> startTimes = new HashMap<String, Long>();
    protected final Map<String, Long> endTimes = new HashMap<String, Long>();

    protected SimplePerfLogger() {
    }

    public void perfLogBegin(String callerName, String method) {
        long startTime = System.currentTimeMillis();
        startTimes.put(method, new Long(startTime));
        if (LOG.isDebugEnabled()) {
            LOG.debug("<PERFLOG method=" + method + " from=" + callerName + ">");
        }
    }

    public long perfLogEnd(String callerName, String method) {
        return perfLogEnd(callerName, method, null);
    }

    public long perfLogEnd(String callerName, String method, String additionalInfo) {
        Long startTime = startTimes.get(method);
        long endTime = System.currentTimeMillis();
        endTimes.put(method, new Long(endTime));
        long duration = startTime == null ? -1 : endTime - startTime.longValue();

        if (LOG.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder("</PERFLOG method=").append(method);
            if (startTime != null) {
                sb.append(" start=").append(startTime);
            }
            sb.append(" end=").append(endTime);
            if (startTime != null) {
                sb.append(" duration=").append(duration);
            }
            sb.append(" from=").append(callerName);
            if (additionalInfo != null) {
                sb.append(" ").append(additionalInfo);
            }
            sb.append(">");
            LOG.debug(sb.toString());
        }
        return duration;
    }

}
