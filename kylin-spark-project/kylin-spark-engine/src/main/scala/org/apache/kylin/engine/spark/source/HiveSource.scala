package org.apache.kylin.engine.spark.source

import java.util
import java.util.Locale

import org.apache.kylin.engine.spark.NSparkCubingEngine.NSparkCubingSource
import org.apache.kylin.engine.spark.metadata.cube.source.ISource
import org.apache.kylin.engine.spark.metadata.TableDesc
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.util.SparkTypeUtil

class HiveSource extends ISource with Logging {
  /**
   * Return an adaptor that implements specified interface as requested by the build engine.
   * The IMRInput in particular, is required by the MR build engine.
   */
  override def adaptToBuildEngine[I](engineInterface: Class[I]): I = {
    new NSparkCubingSource() {
      override def getSourceData(table: TableDesc, ss: SparkSession, parameters: util.Map[String, String]): Dataset[Row] = {
        val colString = table.columns.map((tb) => tb.columnName).mkString(",")
        val sql = String.format(Locale.ROOT, "select %s from %s", colString, table.identity)
        val df = ss.sql(sql)
        val sparkSchema = df.schema
        logInfo("Source data sql is: " + sql)
        logInfo("Kylin schema " + table.toSchema.treeString)
        df.select(SparkTypeUtil.alignDataType(sparkSchema, table.toSchema): _*)
      }
    }.asInstanceOf[I]
  }
}
