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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;


public class NotifyService {
    private static final Logger logger = LoggerFactory.getLogger(NotifyService.class);

    private NotifyServiceBase[] notifyServices;

    private ExecutorService pool = Executors.newCachedThreadPool();

    private List<Future> futureList = new ArrayList<>();

    public NotifyService(KylinConfig config) {
        notifyServices = new NotifyServiceBase[]{
                new MailService(config),
                new DingTalkService(config)
        };
    }

    public boolean sendNotify(Map<String, List<String>> receivers, String state, Pair<String[], Map<String, Object>> content) {
        boolean res = Boolean.TRUE;

        Consumer<NotifyServiceBase> action = new Consumer<NotifyServiceBase>() {

            @Override
            public void accept(NotifyServiceBase notifyServiceBase) {
                notifyServiceBase.sendNotify(receivers, state, content);
            }
        };
        Arrays.stream(notifyServices).forEach(action);

        for (Callable callable : notifyServices) {
            Future f = pool.submit(callable);
            futureList.add(f);
        }

        for (Future f : futureList) {
            try {
                if (f.get(1, TimeUnit.MINUTES).toString().equalsIgnoreCase("false")) {
                    res = false;
                }
            } catch (Exception e) {
                res = false;
                logger.error("push notification for email or dingtalk occurred exception");
            }
        }

        pool.shutdown();

        return res;
    }

}
