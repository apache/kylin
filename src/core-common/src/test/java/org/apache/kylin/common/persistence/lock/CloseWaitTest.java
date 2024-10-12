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

package org.apache.kylin.common.persistence.lock;

import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.InMemResourceStore;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.TransparentResourceStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@MetadataInfo(onlyProps = true)
@Disabled("Only run this manually")
class CloseWaitTest {

    private Server server = null;
    private static ResourceStore store = null;

    @BeforeEach
    public void setup() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.env.lock-timeout", "30");
        config.setProperty("kylin.env.dead-lock-check-interval", "10");

        if (server == null) {
            server = new Server(7077);
            server.setHandler(new TransactionHandler());
            server.start();
            InMemResourceStore inMemResourceStore = (InMemResourceStore) ResourceStore.getKylinMetaStore(config);
            store = new TransparentResourceStore(inMemResourceStore, config);
            TransactionDeadLockHandler.getInstance().start();
        }
    }

    @AfterEach
    public void cleanup() throws Exception {
        server.stop();
    }

    private static class TransactionHandler extends AbstractHandler {

        @Override
        public void handle(String s, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
                throws IOException, ServletException {
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().println("Mocked response");
            baseRequest.setHandled(true);
            if (s.equals("/api/suggest")) {
                try {
                    UnitOfWork.doInTransactionWithRetry(() -> {
                        MemoryLockUtils.doWithLock("/default/model_desc/mDrop", true, store, () -> null);
                        MemoryLockUtils.manuallyLockModule("default", ModuleLockEnum.MODEL, store);
                        System.out.println("Suggest transaction succeed");
                        return null;
                    }, "default");
                } catch (Exception e) {
                    if (Thread.currentThread().isInterrupted()) {
                        // do nothing.
                    }
                    System.out.println("Suggest transaction failed");
                }
            } else if (s.equals("/api/drop")) {
                try {
                    UnitOfWork.doInTransactionWithRetry(() -> {
                        MemoryLockUtils.doWithLock("/default/model_desc/mDrop", true, store, () -> null);
                        MemoryLockUtils.doWithLock("/default/model_desc/mDrop", false, store, () -> null);
                        System.out.println("Drop transaction succeed");
                        return null;
                    }, "default");
                } catch (Exception e) {
                    if (Thread.currentThread().isInterrupted()) {
                        // do nothing.
                    }
                    System.out.println("Drop transaction failed");
                }
            }
        }
    }

    @Disabled("Only run this manually")
    @Test
    void closeWaitTest() {
        AtomicBoolean shouldContinue = new AtomicBoolean(true);
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(5 * 1000)
                .setConnectionRequestTimeout(5 * 1000).setSocketTimeout(5 * 1000).build();
        Runnable suggest = () -> {
            while (shouldContinue.get()) {
                long start = System.currentTimeMillis();
                try (CloseableHttpClient client = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig)
                        .build()) {
                    HttpPost suggestRequest = new HttpPost("http://127.0.0.1:7077/api/suggest");

                    client.execute(suggestRequest);
                } catch (IOException e) {
                    System.out.println("suggestRequest failed.");
                } finally {
                    System.out.println("suggestRequest cost: " + (System.currentTimeMillis() - start) + "ms");
                }
            }
        };
        Runnable drop = () -> {
            while (shouldContinue.get()) {
                long start = System.currentTimeMillis();
                try (CloseableHttpClient client = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig)
                        .build()) {
                    HttpPost suggestRequest = new HttpPost("http://127.0.0.1:7077/api/drop");

                    client.execute(suggestRequest);
                } catch (IOException e) {
                    System.out.println("dropRequest failed.");
                } finally {
                    System.out.println("suggestRequest cost: " + (System.currentTimeMillis() - start) + "ms");
                }
            }
        };
        List<Thread> threads = new ArrayList<>();
        threads.add(new Thread(suggest));
        threads.add(new Thread(suggest));
        threads.add(new Thread(suggest));
        threads.add(new Thread(suggest));
        threads.add(new Thread(suggest));
        threads.add(new Thread(drop));

        threads.forEach(Thread::start);
        await().atMost(10000, TimeUnit.SECONDS).until(() -> false);
    }

}
