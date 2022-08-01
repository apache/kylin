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

package org.apache.kylin.junit;

import java.util.TimeZone;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeZoneTestRunner extends BlockJUnit4ClassRunner {
    private static final Logger logger = LoggerFactory.getLogger(KylinConfig.class);

    private static String[] timeZones = { "UTC" };

    public TimeZoneTestRunner(Class<?> clazz) throws Exception {
        super(clazz);
    }

    // Runs junit tests in a separate thread using the custom class loader
    @Override
    public void run(final RunNotifier notifier) {
        TimeZone aDefault = TimeZone.getDefault();
        for (String timeZone : timeZones) {
            TimeZone.setDefault(TimeZone.getTimeZone(timeZone));
            DateFormat.cleanCache();
            logger.info("Running {} with time zone {}", getTestClass().getJavaClass().toString(), timeZone);
            Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    TimeZoneTestRunner.super.run(notifier);
                }
            };
            Thread thread = new Thread(runnable);
            thread.start();
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException("current time zone is " + timeZone, e);
            } finally {
                DateFormat.cleanCache();
            }
        }
        TimeZone.setDefault(aDefault);
    }

}
