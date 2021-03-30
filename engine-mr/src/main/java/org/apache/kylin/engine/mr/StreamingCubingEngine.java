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

package org.apache.kylin.engine.mr;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.job.execution.DefaultChainedExecutable;

/**
 * Created by gwang3 on 2017/2/23.
 */
public class StreamingCubingEngine {

    /**
     * A factory which used to create building job which input data were columnar storage cache upload be receiver
     */
    public DefaultChainedExecutable createStreamingCubingJob(CubeSegment seg, String submitter) {
        return new StreamingCubingJobBuilder(seg, submitter).build();
    }
}
