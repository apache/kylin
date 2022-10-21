/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.common

import com.google.common.base.Preconditions
import org.apache.kylin.common.util.TempMetadataBuilder
import org.apache.kylin.metadata.model.NTableMetadataManager
import org.apache.kylin.metadata.project.NProjectManager
import org.apache.kylin.query.util.{QueryParams, QueryUtil}
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession}
import org.apache.spark.sql.execution.utils.SchemaProcessor
import org.scalatest.Suite

import java.util.Locale

trait SSSource extends SharedSparkSession with LocalMetadata {
  self: Suite =>

  val CSV_TABLE_DIR = "../" + TempMetadataBuilder.TEMP_TEST_METADATA + "/data/%s.csv"

  override def beforeAll() {
    super.beforeAll()
    val project = getProject
    import org.apache.kylin.metadata.project.NProjectManager
    val kylinConf = KylinConfig.getInstanceFromEnv
    val projectInstance =
      NProjectManager.getInstance(kylinConf).getProject(project)
    Preconditions.checkArgument(projectInstance != null)
    import scala.collection.JavaConverters._
    projectInstance.getTables.asScala
      .filter(!_.equals("DEFAULT.STREAMING_TABLE"))
      .foreach { table =>
        val tableDesc = NTableMetadataManager
          .getInstance(kylinConf, project)
          .getTableDesc(table)
        val columns = tableDesc.getColumns
        val schema = SchemaProcessor.buildSchemaWithRawTable(columns)
        var tableN = tableDesc.getName
        if (table.equals("DEFAULT.TEST_KYLIN_FACT")) {
          tableN = tableDesc.getName + "_table"
        }
        spark.catalog.createTable(
          tableName = tableN,
          source = "csv",
          schema = schema,
          options = Map("path" -> String.format(Locale.ROOT, CSV_TABLE_DIR, table)))
        if (table.equals("DEFAULT.TEST_KYLIN_FACT")) {
          spark.sql("create view " + tableDesc.getName + " as select * from " + tableN)
        }
      }
  }

  protected def getProject: String = "default"

  def cleanSql(originSql: String): String = {
    val sqlForSpark = originSql
      .replaceAll("edw\\.", "")
      .replaceAll("\"EDW\"\\.", "")
      .replaceAll("EDW\\.", "")
      .replaceAll("default\\.", "")
      .replaceAll("DEFAULT\\.", "")
      .replaceAll("\"DEFAULT\"\\.", "")
    val queryParams = new QueryParams("default", sqlForSpark, "DEFAULT", false)
    queryParams.setKylinConfig(NProjectManager.getProjectConfig("default"))
    QueryUtil.massagePushDownSql(queryParams)
  }
}
