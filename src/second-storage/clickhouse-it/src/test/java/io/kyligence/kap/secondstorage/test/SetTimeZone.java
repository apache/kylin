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
package io.kyligence.kap.secondstorage.test;

import java.util.TimeZone;

import org.junit.rules.ExternalResource;

public class SetTimeZone extends ExternalResource {

    private final TimeZone oldTimeZone;
    private final TimeZone newTimeZone;

    public SetTimeZone(String timeZone) {
        this.oldTimeZone = TimeZone.getDefault();
        this.newTimeZone = TimeZone.getTimeZone(timeZone);
    }

    @Override
    protected void before() {
        TimeZone.setDefault(newTimeZone);
    }

    @Override
    protected void after() {
        TimeZone.setDefault(oldTimeZone);
    }
}
