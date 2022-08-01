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

import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.beans.BeansException;
import org.springframework.beans.support.MutableSortDefinition;
import org.springframework.beans.support.SortDefinition;

/**
 * this class is used to performs a comparison of two beans
 * refer spring's PropertyComparator
 */
public class ModelTripleComparator implements Comparator<ModelTriple> {
    private static final Logger logger = LoggerFactory.getLogger(ModelTripleComparator.class);
    private final SortDefinition sortDefinition;
    private final BeanWrapperImpl beanWrapper = new BeanWrapperImpl(false);

    private int sortKey;

    public ModelTripleComparator(String property, boolean ascending, int sortKey) {
        this.sortDefinition = new MutableSortDefinition(property, false, ascending);
        this.sortKey = sortKey;
    }

    public int compare(ModelTriple o1, ModelTriple o2) {
        if (o1 == null || o2 == null) {
            return 0;
        }
        Object v1;
        Object v2;

        int result;
        try {
            if (sortKey == ModelTriple.SORT_KEY_DATAFLOW) {
                v1 = this.getPropertyValue(o1.getDataflow());
                v2 = this.getPropertyValue(o2.getDataflow());
            } else if (sortKey == ModelTriple.SORT_KEY_DATA_MODEL) {
                v1 = this.getPropertyValue(o1.getDataModel());
                v2 = this.getPropertyValue(o2.getDataModel());
            } else {
                v1 = o1.getCalcObject();
                v2 = o2.getCalcObject();
            }

            if (v1 != null) {
                result = v2 != null ? ((Comparable) v1).compareTo(v2) : -1;
            } else {
                result = v2 != null ? 1 : 0;
            }
        } catch (RuntimeException ex) {
            logger.warn("Could not sort objects [{}] and [{}]", o1, o2, ex);
            return 0;
        }

        return this.sortDefinition.isAscending() ? result : -result;
    }

    public Object getPropertyValue(Object obj) {
        try {
            this.beanWrapper.setWrappedInstance(obj);
            return this.beanWrapper.getPropertyValue(this.sortDefinition.getProperty());
        } catch (BeansException ex) {
            logger.info("PropertyComparator could not access property - treating as null for sorting", ex);
            return null;
        }
    }

}
