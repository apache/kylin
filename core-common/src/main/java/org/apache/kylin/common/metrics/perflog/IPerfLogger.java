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

public interface IPerfLogger {

    /**
     * Call this function when you start to measure time spent by a piece of code.
     *
     * @param callerName the logging object to be used.
     * @param method     method or ID that identifies this perf log element.
     */
    public void perfLogBegin(String callerName, String method);

    /**
     * Call this function in correspondence of perfLogBegin to mark the end of the measurement.
     *
     * @param callerName
     * @param method
     * @return long duration  the difference between now and startTime, or -1 if startTime is null
     */
    public long perfLogEnd(String callerName, String method);

    /**
     * Call this function in correspondence of perfLogBegin to mark the end of the measurement.
     *
     * @param callerName
     * @param method
     * @return long duration  the difference between now and startTime, or -1 if startTime is null
     */
    public long perfLogEnd(String callerName, String method, String additionalInfo);
}
