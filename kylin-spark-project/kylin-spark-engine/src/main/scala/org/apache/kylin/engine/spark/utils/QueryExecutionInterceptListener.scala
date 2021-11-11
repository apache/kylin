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
