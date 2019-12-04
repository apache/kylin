package org.apache.kylin.engine.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object SparkEnv extends Logging{
	@volatile
	private var spark: SparkSession = _

	def setSparkSession(sparkSession: SparkSession): Unit = {
		spark = sparkSession
	}
}
