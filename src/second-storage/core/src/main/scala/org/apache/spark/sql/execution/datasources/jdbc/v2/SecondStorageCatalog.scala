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
package org.apache.spark.sql.execution.datasources.jdbc.v2

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import io.kyligence.kap.secondstorage.ColumnMapping.secondStorageColumnToKapColumn
import io.kyligence.kap.secondstorage.NameUtil
import io.kyligence.kap.secondstorage.SecondStorage.tableFlowManager
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.utils.JavaOptionals.toRichOptional
import org.apache.kylin.metadata.cube.model.NDataflowManager
import org.apache.kylin.metadata.datatype.DataType
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRDD}
import org.apache.spark.sql.execution.datasources.v2.jdbc.ShardJDBCTableCatalog
import org.apache.spark.sql.types.{BooleanType, StructType}

class SecondStorageCatalog extends ShardJDBCTableCatalog {

  protected val cache: LoadingCache[Identifier, StructType] = CacheBuilder.newBuilder()
    .maximumSize(100L)
    .build(
      new CacheLoader[Identifier, StructType]() {
        override def load(ident: Identifier): StructType = {
          val config = KylinConfig.getInstanceFromEnv
          val project = NameUtil.recoverProject(ident.namespace()(0), config)
          val pair = NameUtil.recoverLayout(ident.name())
          val model_id = pair.getFirst
          val layout = pair.getSecond
          val dfMgr = NDataflowManager.getInstance(config, project)
          val orderedDims = dfMgr.getDataflow(model_id).getIndexPlan.getLayoutEntity(layout).getOrderedDimensions

          val schemaURL = tableFlowManager(config, project)
            .get(model_id)
            .flatMap(f => f.getEntity(layout))
            .toOption
            .map(_.getSchemaURL)
            .get

          val immutable = Map(
            JDBCOptions.JDBC_TABLE_NAME -> getTableName(ident),
            JDBCOptions.JDBC_URL -> schemaURL)
          val jdbcSchema = JDBCRDD.resolveTable(new JDBCOptions(immutable))

          // !!This is patch!!
          StructType(jdbcSchema.map(e => {
            orderedDims.get(secondStorageColumnToKapColumn(e.name).toInt).getType.getName match {
              case DataType.BOOLEAN => e.copy(dataType = BooleanType)
              case _ => e
            }
          })
          )
        }
      })

  override protected def resolveTable(ident: Identifier): Option[StructType] = {
    Option.apply(cache.get(ident))
  }
}
