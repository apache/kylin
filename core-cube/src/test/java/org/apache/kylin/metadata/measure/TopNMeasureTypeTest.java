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
package org.apache.kylin.metadata.measure;

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.measure.topn.TopNMeasureType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by shishaofeng on 6/6/16.
 */
public class TopNMeasureTypeTest extends LocalFileMetadataTestCase {

    @Before
    public void setup() {
        this.createTestMetadata();

    }

    @After
    public void clear() {
        this.cleanupTestMetadata();
    }

    @Test
    public void test() {

        CubeDesc desc = CubeDescManager.getInstance(getTestConfig()).getCubeDesc("test_kylin_cube_without_slr_left_join_desc");

        MeasureDesc topSellerMeasure = null;

        for (MeasureDesc measureDesc : desc.getMeasures()) {
            if (measureDesc.getName().equals("TOP_SELLER")) {
                topSellerMeasure = measureDesc;
                break;
            }
        }
        TopNMeasureType measureType = (TopNMeasureType) MeasureTypeFactory.create(topSellerMeasure.getFunction().getExpression(), topSellerMeasure.getFunction().getReturnDataType());

        topSellerMeasure.getFunction().getConfiguration().clear();
        List<TblColRef> colsNeedDict = measureType.getColumnsNeedDictionary(topSellerMeasure.getFunction());

        assertTrue(colsNeedDict != null && colsNeedDict.size() == 1);

        TblColRef sellerColRef = topSellerMeasure.getFunction().getParameter().getColRefs().get(1);
        topSellerMeasure.getFunction().getConfiguration().put(TopNMeasureType.CONFIG_ENCODING_PREFIX + sellerColRef.getIdentity(), "int:6");
        colsNeedDict = measureType.getColumnsNeedDictionary(topSellerMeasure.getFunction());

        assertTrue(colsNeedDict.size() == 0);
    }
}
