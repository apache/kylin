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
package org.apache.kylin.storage.hdfs;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.util.concurrent.TimeUnit;


public class ResourceLock {

    private String resourcePath;

    private InterProcessMutex lock;

    protected ResourceLock(String resourcePath, InterProcessMutex lock) {
        this.resourcePath = resourcePath;
        this.lock = lock;
    }

    public void acquire(long time, TimeUnit unit) throws Exception {
        boolean success = lock.acquire(time, unit);
        if(!success){
            throw new IllegalStateException("Fail to get Zookeeper lock");
        }
    }

    public void acquire() throws Exception{
       lock.acquire();
    }

    protected void release() throws Exception {
        lock.release();
    }

    public String getResourcePath() {
        return resourcePath;
    }
}
