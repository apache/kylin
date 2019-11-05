/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.engine.spark.utils;

import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.engine.spark.builder.CreateFlatTable;
import io.kyligence.kap.engine.spark.job.NSparkCubingUtil;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;

public class NUtilsTest extends NLocalWithSparkSessionTest {

    @Test
    public void testDotConversion() {
        String condition = "TEST_KYLIN_FACT.CAL_DT > 2017-09-12 AND TEST_KYLIN_FACT.PRICE < 9.9";
        String col = "TEST_KYLIN_FACT.CAL_DT";
        String withoutDot = NSparkCubingUtil.convertFromDot(col);
        Assert.assertEquals("TEST_KYLIN_FACT" + NSparkCubingUtil.SEPARATOR + "CAL_DT", withoutDot);
        String withDot = NSparkCubingUtil.convertToDot(withoutDot);
        Assert.assertEquals(col, withDot);

        NDataModel model = (NDataModel) NDataModelManager.getInstance(getTestConfig(), "default")
                .getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        String replaced = CreateFlatTable.replaceDot(condition, model);
        Assert.assertEquals("TEST_KYLIN_FACT" + NSparkCubingUtil.SEPARATOR + "CAL_DT > 2017-09-12 AND TEST_KYLIN_FACT"
                + NSparkCubingUtil.SEPARATOR + "PRICE < 9.9", replaced);
    }
}
