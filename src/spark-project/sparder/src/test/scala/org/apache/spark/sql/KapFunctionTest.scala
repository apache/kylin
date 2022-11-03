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
package org.apache.spark.sql

import java.time.ZoneId
import java.util.{Calendar, Locale, TimeZone}

import org.apache.spark.sql.catalyst.util.{DateTimeUtils, KapDateTimeUtils}
import org.apache.spark.sql.common.{SharedSparkSession, SparderBaseFunSuite}
import org.junit.Assert

class KapFunctionsTest extends SparderBaseFunSuite with SharedSparkSession {

  ignore("kapAddMonths") {
    Assert.assertEquals(KapDateTimeUtils.subtractMonths(1585411200000L, 1582819200000L), 1)
    Assert.assertEquals(KapDateTimeUtils.subtractMonths(1585497600000L, 1582905600000L), 0)
    Assert.assertEquals(KapDateTimeUtils.subtractMonths(1585584000000L, 1582905600000L), 1)
    Assert.assertEquals(KapDateTimeUtils.subtractMonths(1593446400000L, 1585584000000L), 3)
    var time = 1420048800000L
    // 4 year,include leap year
    for (i <- 0 to 35040) {
      for ( j <- -12 to 12) {
        val now = KapDateTimeUtils.millisToDaysLegacy(time, TimeZone.getTimeZone(ZoneId.systemDefault()))
        val d1 = DateTimeUtils.dateAddMonths(now, j)
        val d2 = KapDateTimeUtils.dateAddMonths(now, j)

        Assert.assertEquals(d1, d2)
      }
      // add hour
      time += 3600 * 1000L
    }
  }

}
