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

package org.apache.kylin.engine.spark.utils

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.util.QueryExecutionListener

import java.net.URI

/**
 * This QueryExecutionListener will intercept QueryExecution when outputPath matched, so make sure outputPath was unique.
 */
class QueryExecutionInterceptListener(outputPath: String) extends QueryExecutionListener{

  var queryExecution : Option[QueryExecution] = None

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    qe.sparkPlan foreach {
      case plan: DataWritingCommandExec =>{
        //check if output path match
        if (plan.cmd.isInstanceOf[InsertIntoHadoopFsRelationCommand]
          && plan.cmd.asInstanceOf[InsertIntoHadoopFsRelationCommand].outputPath.toUri.equals(new URI(outputPath))) {
          queryExecution = Some(qe)
        }
      }
      case _ =>
    }
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {

  }
}
