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

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;

import lombok.AllArgsConstructor;
import lombok.val;

@AllArgsConstructor
class TimezoneExtension implements BeforeEachCallback, AfterEachCallback {

    private static final Namespace NAMESPACE = Namespace.create(TimezoneExtension.class);
    private static final String KEY = "DefaultTimeZone";
    private String timezone;

    @Override
    public void beforeEach(ExtensionContext context) {
        val store = context.getStore(NAMESPACE);
        setDefaultTimeZone(store, timezone);
    }

    private void setDefaultTimeZone(Store store, String zoneId) {
        TimeZone defaultTimeZone = createTimeZone(zoneId);
        // defer storing the current default time zone until the new time zone could be created from the configuration
        // (this prevents cases where misconfigured extensions store default time zone now and restore it later,
        // which leads to race conditions in our tests)
        storeDefaultTimeZone(store);
        TimeZone.setDefault(defaultTimeZone);
    }

    private static TimeZone createTimeZone(String timeZoneId) {
        TimeZone configuredTimeZone = TimeZone.getTimeZone(timeZoneId);
        // TimeZone::getTimeZone returns with GMT as fallback if the given ID cannot be understood
        if (configuredTimeZone.equals(TimeZone.getTimeZone("GMT")) && !timeZoneId.equals("GMT")) {
            throw new ExtensionConfigurationException(String.format(
                    "@DefaultTimeZone not configured correctly. " + "Could not find the specified time zone + '%s'. "
                            + "Please use correct identifiers, e.g. \"GMT\" for Greenwich Mean Time.",
                    timeZoneId));
        }
        return configuredTimeZone;
    }

    private void storeDefaultTimeZone(Store store) {
        store.put(KEY, TimeZone.getDefault());
    }

    @Override
    public void afterEach(ExtensionContext context) {
        resetDefaultTimeZone(context.getStore(NAMESPACE));
    }

    private void resetDefaultTimeZone(Store store) {
        TimeZone timeZone = store.get(KEY, TimeZone.class);
        // default time zone is null if the extension was misconfigured and execution failed in "before"
        if (timeZone != null)
            TimeZone.setDefault(timeZone);
    }

}
