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

package org.apache.kylin.common.notify;

import org.apache.kylin.common.util.Pair;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public abstract class NotifyServiceBase implements Callable<Boolean> {

    public Map<String, List<String>> receivers;
    public String state;
    public Pair<String[], Map<String, Object>> content;

    public abstract boolean sendNotify();

    public void sendNotify(Map<String, List<String>> receivers, String state, Pair<String[], Map<String, Object>> content){
        this.receivers = receivers;
        this.state = state;
        this.content = content;
    }

    @Override
    public Boolean call() throws Exception {
        return sendNotify();
    }
}
