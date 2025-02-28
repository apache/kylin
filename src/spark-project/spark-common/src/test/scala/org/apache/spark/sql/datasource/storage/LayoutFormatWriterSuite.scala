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

package org.apache.spark.sql.datasource.storage

import org.apache.hadoop.fs.ContentSummary
import org.apache.kylin.common.{KapConfig, KylinConfig, KylinConfigBase}
import org.mockito.Mockito
import org.scalatest.funsuite.AnyFunSuite

class LayoutFormatWriterSuite extends AnyFunSuite {
  def getTestConfig: KylinConfig = {
    KylinConfig.createKylinConfig(new String())
  }

  test("test need reset row group") {
    val config = getTestConfig
    val kapConfig = KapConfig.wrap(config)
    val summary = Mockito.mock(classOf[ContentSummary])
    assert(!LayoutFormatWriter.needResetRowGroup(0, summary, kapConfig))
    config.setProperty("kylin.engine.reset-parquet-block-size", KylinConfigBase.TRUE)
    assert(!LayoutFormatWriter.needResetRowGroup(0, summary, kapConfig))
    config.setProperty("kylin.engine.parquet-row-count-per-mb", "1")
    Mockito.when(summary.getLength).thenReturn(1024 * 1024L)
    assert(LayoutFormatWriter.needResetRowGroup(2, summary, kapConfig))
  }
}
