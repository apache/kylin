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

public class CubeJobLockUtil {
    public enum LockType {
        CubeJobLockPath("/cube_job_lock/"), CubeJobEphemeralLockPath("/cube_job_ephemeral_lock/");
        private String name;

        LockType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public static String getLockPath(String cubeName, String jobId) {
        if (jobId == null) {
            return LockType.CubeJobLockPath.getName() + cubeName;
        } else {
            return LockType.CubeJobLockPath.getName() + cubeName + "/" + jobId;
        }
    }

    public static String getEphemeralLockPath(String cubeName) {
        return LockType.CubeJobEphemeralLockPath.getName() + cubeName;
    }
}
