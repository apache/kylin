package org.apache.spark.sql.common

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}

object SparderQueryTest {
	def castDataType(sparkResult: DataFrame, cubeResult: DataFrame): DataFrame = {
		val newNames = sparkResult.schema.names
				.zipWithIndex
				.map(name => name._1.replaceAll("\\.", "_") + "_" + name._2).toSeq
		val newDf = sparkResult.toDF(newNames: _*)
		val columns = newDf.schema.zip(cubeResult.schema).map {
			case (sparkField, kylinField) =>
				if (!sparkField.dataType.sameType(kylinField.dataType)) {
					col(sparkField.name).cast(kylinField.dataType)
				} else {
					col(sparkField.name)
				}
		}
		newDf.select(columns: _*)
	}

	// compare string .
	def prepareRow(row: Row): Row = {
		Row.fromSeq(row.toSeq.map {
			case null => null
			case d: java.math.BigDecimal => BigDecimal(d).setScale(2, BigDecimal.RoundingMode.HALF_UP)
			case db: Double if db.isNaN || db.isInfinite => None
			case db: Double => BigDecimal.apply(db).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString.toDouble
			// Convert array to Seq for easy equality check.
			case b: Array[_] => b.toSeq
			case r: Row => prepareRow(r)
			case o => o
		})
	}
}
