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

package org.apache.kylin.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Serializable;
import java.io.Writer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.InvalidPropertiesFormatException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;

/**
 * Modified from the etiennestuder/java-ordered-properties in https://github.com/etiennestuder/java-ordered-properties
 *
 * This class provides an alternative to the JDK's {@link Properties} class. It fixes the design flaw of using
 * inheritance over composition, while keeping up the same APIs as the original class. Keys and values are
 * guaranteed to be of type {@link String}.
 * <p/>
 * This class is not synchronized, contrary to the original implementation.
 * <p/>
 * As additional functionality, this class keeps its properties in a well-defined order. By default, the order
 * is the one in which the individual properties have been added, either through explicit API calls or through
 * reading them top-to-bottom from a properties file.
 * <p/>
 * Currently, this class does not support the concept of default properties, contrary to the original implementation.
 * <p/>
 * <strong>Note that this implementation is not synchronized.</strong> If multiple threads access ordered
 * properties concurrently, and at least one of the threads modifies the ordered properties structurally, it
 * <em>must</em> be synchronized externally. This is typically accomplished by synchronizing on some object
 * that naturally encapsulates the properties.
 * <p/>
 * Note that the actual (and quite complex) logic of parsing and storing properties from and to a stream
 * is delegated to the {@link Properties} class from the JDK.
 *
 * @see Properties
 */
public final class OrderedProperties implements Serializable {

    private static final long serialVersionUID = 1L;

    private transient Map<String, String> properties;

    public OrderedProperties() {
        this(new LinkedHashMap<String, String>());
    }

    public OrderedProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    /**
     * Creates a new instance that will have both the same property entries and
     * the same behavior as the given source.
     * <p/>
     * Note that the source instance and the copy instance will share the same
     * comparator instance if a custom ordering had been configured on the source.
     *
     * @param source the source to copy from
     * @return the copy
     */
    public static OrderedProperties copyOf(OrderedProperties source) {
        // create a copy that has the same behaviour
        OrderedPropertiesBuilder builder = new OrderedPropertiesBuilder();
        if (source.properties instanceof TreeMap) {
            builder.withOrdering(((TreeMap<String, String>) source.properties).comparator());
        }
        OrderedProperties result = builder.build();

        // copy the properties from the source to the target
        for (Map.Entry<String, String> entry : source.entrySet()) {
            result.setProperty(entry.getKey(), entry.getValue());
        }
        return result;
    }

    /**
     * copy a new instance that all k-v are trimed
     * @param source
     * @return
     */
    public static OrderedProperties copyAndTrim(@Nonnull OrderedProperties source) {
        Preconditions.checkNotNull(source, "source cannot be null");
        // create a copy that has the same behaviour
        OrderedPropertiesBuilder builder = new OrderedPropertiesBuilder();
        if (source.properties instanceof TreeMap) {
            builder.withOrdering(((TreeMap<String, String>) source.properties).comparator());
        }
        OrderedProperties result = builder.build();

        // copy the properties from the source to the target
        for (Map.Entry<String, String> entry : source.entrySet()) {
            result.setProperty(StringUtils.trim(entry.getKey()), StringUtils.trim(entry.getValue()));
        }
        return result;
    }

    /**
     * See {@link Properties#getProperty(String)}.
     */
    public String getProperty(String key) {
        return properties.get(key);
    }

    /**
     * See {@link Properties#getProperty(String, String)}.
     */
    public String getProperty(String key, String defaultValue) {
        String value = properties.get(key);
        return (value == null) ? defaultValue : value;
    }

    /**
     * See {@link Properties#setProperty(String, String)}.
     */
    public String setProperty(String key, String value) {
        return properties.put(key, value);
    }

    /**
     * Removes the property with the specified key, if it is present. Returns
     * the value of the property, or <tt>null</tt> if there was no property with
     * the specified key.
     *
     * @param key the key of the property to remove
     * @return the previous value of the property, or <tt>null</tt> if there was no property with the specified key
     */
    public String removeProperty(String key) {
        return properties.remove(key);
    }

    /**
     * Returns <tt>true</tt> if there is a property with the specified key.
     *
     * @param key the key whose presence is to be tested
     */
    public boolean containsProperty(String key) {
        return properties.containsKey(key);
    }

    /**
     * See {@link Properties#size()}.
     */
    public int size() {
        return properties.size();
    }

    /**
     * See {@link Properties#isEmpty()}.
     */
    public boolean isEmpty() {
        return properties.isEmpty();
    }

    /**
     * See {@link Properties#propertyNames()}.
     */
    public Enumeration<String> propertyNames() {
        return new Vector<String>(properties.keySet()).elements();
    }

    /**
     * See {@link Properties#stringPropertyNames()}.
     */
    public Set<String> stringPropertyNames() {
        return new LinkedHashSet<String>(properties.keySet());
    }

    /**
     * See {@link Properties#entrySet()}.
     */
    public Set<Map.Entry<String, String>> entrySet() {
        return new LinkedHashSet<Map.Entry<String, String>>(properties.entrySet());
    }

    public void putAll(OrderedProperties others) {
        for (Map.Entry<String, String> each : others.entrySet()) {
            properties.put(each.getKey(), each.getValue());
        }
    }

    /**
     * See {@link Properties#load(InputStream)}.
     */
    public void load(InputStream stream) throws IOException {
        CustomProperties customProperties = new CustomProperties(this.properties);
        customProperties.load(stream);
    }

    /**
     * See {@link Properties#load(Reader)}.
     */
    public void load(Reader reader) throws IOException {
        CustomProperties customProperties = new CustomProperties(this.properties);
        customProperties.load(reader);
    }

    /**
     * See {@link Properties#loadFromXML(InputStream)}.
     */
    @SuppressWarnings("DuplicateThrows")
    public void loadFromXML(InputStream stream) throws IOException, InvalidPropertiesFormatException {
        CustomProperties customProperties = new CustomProperties(this.properties);
        customProperties.loadFromXML(stream);
    }

    /**
     * See {@link Properties#store(OutputStream, String)}.
     */
    public void store(OutputStream stream, String comments) throws IOException {
        CustomProperties customProperties = new CustomProperties(this.properties);
        customProperties.store(stream, comments);
    }

    /**
     * See {@link Properties#store(Writer, String)}.
     */
    public void store(Writer writer, String comments) throws IOException {
        CustomProperties customProperties = new CustomProperties(this.properties);
        customProperties.store(writer, comments);
    }

    /**
     * See {@link Properties#storeToXML(OutputStream, String)}.
     */
    public void storeToXML(OutputStream stream, String comment) throws IOException {
        CustomProperties customProperties = new CustomProperties(this.properties);
        customProperties.storeToXML(stream, comment);
    }

    /**
     * See {@link Properties#storeToXML(OutputStream, String, String)}.
     */
    public void storeToXML(OutputStream stream, String comment, String encoding) throws IOException {
        CustomProperties customProperties = new CustomProperties(this.properties);
        customProperties.storeToXML(stream, comment, encoding);
    }

    /**
     * See {@link Properties#list(PrintStream)}.
     */
    public void list(PrintStream stream) {
        CustomProperties customProperties = new CustomProperties(this.properties);
        customProperties.list(stream);
    }

    /**
     * See {@link Properties#list(PrintWriter)}.
     */
    public void list(PrintWriter writer) {
        CustomProperties customProperties = new CustomProperties(this.properties);
        customProperties.list(writer);
    }

    /**
     * Convert this instance to a {@link Properties} instance.
     *
     * @return the {@link Properties} instance
     */
    public Properties toJdkProperties() {
        Properties jdkProperties = new Properties();
        for (Map.Entry<String, String> entry : this.entrySet()) {
            jdkProperties.put(entry.getKey(), entry.getValue());
        }
        return jdkProperties;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        OrderedProperties that = (OrderedProperties) other;
        return Arrays.equals(properties.entrySet().toArray(), that.properties.entrySet().toArray());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(properties.entrySet().toArray());
    }

    private void writeObject(ObjectOutputStream stream) throws IOException {
        stream.defaultWriteObject();
        stream.writeObject(properties);
    }

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        properties = (Map<String, String>) stream.readObject();
    }

    private void readObjectNoData() throws InvalidObjectException {
        throw new InvalidObjectException("Stream data required");
    }

    /**
     * See {@link Properties#toString()}.
     */
    @Override
    public String toString() {
        return properties.toString();
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Builder for {@link OrderedProperties} instances.
     */
    public static final class OrderedPropertiesBuilder {

        private Comparator<? super String> comparator;

        /**
         * Use a custom ordering of the keys.
         *
         * @param comparator the ordering to apply on the keys
         * @return the builder
         */
        public OrderedPropertiesBuilder withOrdering(Comparator<? super String> comparator) {
            this.comparator = comparator;
            return this;
        }

        /**
         * Builds a new {@link OrderedProperties} instance.
         *
         * @return the new instance
         */
        public OrderedProperties build() {
            Map<String, String> properties = (this.comparator != null) ? new TreeMap<>(comparator)
                    : new LinkedHashMap<>();
            return new OrderedProperties(properties);
        }

    }

    /**
     * Custom {@link Properties} that delegates reading, writing, and enumerating properties to the
     * backing {@link OrderedProperties} instance's properties.
     */
    private static final class CustomProperties extends Properties {

        private final Map<String, String> targetProperties;

        private CustomProperties(Map<String, String> targetProperties) {
            this.targetProperties = targetProperties;
        }

        @Override
        public Object get(Object key) {
            return targetProperties.get(key);
        }

        @Override
        public Object put(Object key, Object value) {
            return targetProperties.put((String) key, (String) value);
        }

        @Override
        public String getProperty(String key) {
            return targetProperties.get(key);
        }

        @Override
        public Enumeration<Object> keys() {
            return new Vector<Object>(targetProperties.keySet()).elements();
        }

        @SuppressWarnings("NullableProblems")
        @Override
        public Set<Object> keySet() {
            return new LinkedHashSet<Object>(targetProperties.keySet());
        }

    }

}
