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

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.model.NDataModel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import lombok.val;

public class ModelTripleComparatorTest extends NLocalFileMetadataTestCase {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testModelTripleCompare() {
        val comparator = new ModelTripleComparator("queryHitCount", true, 1);
        val modelTripleOfNullValue = new ModelTriple(null, null);
        Assert.assertEquals(0, comparator.compare(null, modelTripleOfNullValue));
        Assert.assertEquals(0, comparator.compare(modelTripleOfNullValue, null));
        Assert.assertEquals(0, comparator.compare(null, null));
        Assert.assertEquals(0, comparator.compare(modelTripleOfNullValue, modelTripleOfNullValue));

        val dataflow1 = new NDataflow();
        val modelTripleOfNullDataModel1 = new ModelTriple(dataflow1, null);
        val modelTripleOfNullDataModel2 = new ModelTriple(new NDataflow(), null);

        dataflow1.setQueryHitCount(10);
        Assert.assertEquals(1, comparator.compare(modelTripleOfNullDataModel1, modelTripleOfNullDataModel2));

        val comparator1 = new ModelTripleComparator("lastModified", false, 2);
        val dataModel1 = new NDataModel();
        val dataModel2 = new NDataModel();
        val modelTripleOfNullDataflow1 = new ModelTriple(null, dataModel1);
        val modelTripleOfNullDataflow2 = new ModelTriple(null, dataModel2);
        Assert.assertEquals(0, comparator1.compare(modelTripleOfNullDataflow1, modelTripleOfNullDataflow2));

        val comparator2 = new ModelTripleComparator("calcObject", true, 3);
        modelTripleOfNullDataflow1.setCalcObject("t1");
        Assert.assertEquals(-1, comparator2.compare(modelTripleOfNullDataflow1, modelTripleOfNullDataflow2));

        dataModel1.setRecommendationsCount(1);
        dataModel2.setRecommendationsCount(2);
        val comparator3 = new ModelTripleComparator("recommendationsCount", false, 2);
        Assert.assertEquals(1, comparator3.compare(modelTripleOfNullDataflow1, modelTripleOfNullDataflow2));
    }

    @Test
    public void testGetPropertyValueException() {
        val comparator = new ModelTripleComparator("usage", true, 1);
        thrown.expect(Exception.class);
        comparator.getPropertyValue(null);
    }
}
