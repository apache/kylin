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
package org.apache.kylin.rest.util;

import org.springframework.beans.BeanWrapperImpl;
import org.springframework.beans.BeansException;
import org.springframework.beans.support.MutableSortDefinition;
import org.springframework.beans.support.PropertyComparator;
import org.springframework.beans.support.SortDefinition;

public class NullsLastPropertyComparator<T> extends PropertyComparator<T> {

    private final SortDefinition sortBy;

    public NullsLastPropertyComparator(String property, boolean ignoreCase, boolean ascending) {
        super(property, ignoreCase, ascending);
        this.sortBy = new MutableSortDefinition(property, ignoreCase, ascending);
    }

    private Object getValue(Object obj) {
        try {
            BeanWrapperImpl beanWrapper = new BeanWrapperImpl(false);
            beanWrapper.setWrappedInstance(obj);
            return beanWrapper.getPropertyValue(this.sortBy.getProperty());
        } catch (BeansException var3) {
            this.logger.debug("PropertyComparator could not access property - treating as null for sorting", var3);
            return null;
        }
    }
    // null data is placed at the end of the list
    @Override
    public int compare(T a, T b) {
        Object v1 = this.getValue(a);
        Object v2 = this.getValue(b);

        if (v1 == null) {
            return (v2 == null) ? 0 : 1;
        } else if (v2 == null) {
            return -1;
        } else {
            return super.compare(a, b);
        }
    }
}