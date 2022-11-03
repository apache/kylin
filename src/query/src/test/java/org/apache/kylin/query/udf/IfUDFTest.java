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

package org.apache.kylin.query.udf;

import static org.junit.Assert.assertEquals;

import org.apache.kylin.query.udf.otherUdf.IfUDF;
import org.junit.Test;

public class IfUDFTest {

    @Test
    public void testIfUDF() throws Exception {
        IfUDF ifUDF = new IfUDF();
        int score1 = 100, score2 = 85, score3 = 65, score4 = -10;
        assertEquals(ifUDF.IF(score1 > 0, ifUDF.IF(score1 > 90, "优秀", ifUDF.IF(score1 > 70, "良好", "中等")), "非法分数"),
                "优秀");
        assertEquals(ifUDF.IF(score2 > 0, ifUDF.IF(score2 > 90, "优秀", ifUDF.IF(score2 > 70, "良好", "中等")), "非法分数"),
                "良好");
        assertEquals(ifUDF.IF(score3 > 0, ifUDF.IF(score3 > 90, "优秀", ifUDF.IF(score3 > 70, "良好", "中等")), "非法分数"),
                "中等");
        assertEquals(ifUDF.IF(score4 > 0, ifUDF.IF(score4 > 90, "优秀", ifUDF.IF(score4 > 70, "良好", "中等")), "非法分数"),
                "非法分数");
    }
}
