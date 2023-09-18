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
package io.kyligence.kap.secondstorage.config;

import java.util.Locale;

public class ConfigOption<T> {
    private final String key;
    private final T defaultValue;
    private final Class<?> clazz;


    public ConfigOption(String key, Class<?> clazz) {
        this.key = key;
        this.clazz = clazz;
        this.defaultValue = null;
    }

    public ConfigOption(String key, T defaultValue, Class<?> clazz) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.clazz = clazz;
    }

    public String key() {
        return key;
    }

    public boolean hasDefaultValue() {
        return defaultValue != null;
    }

    Class<?> getClazz() {
        return clazz;
    }

    public T defaultValue() {
        return defaultValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && o.getClass() == ConfigOption.class) {
            ConfigOption<?> that = (ConfigOption<?>) o;
            return this.key.equals(that.key)
                    && (this.defaultValue == null
                    ? that.defaultValue == null
                    : (that.defaultValue != null
                    && this.defaultValue.equals(that.defaultValue)));
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return 31 * key.hashCode()
                + (defaultValue != null ? defaultValue.hashCode() : 0);
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT,
                "Key: '%s' , default: %s",
                key, defaultValue);
    }

}
