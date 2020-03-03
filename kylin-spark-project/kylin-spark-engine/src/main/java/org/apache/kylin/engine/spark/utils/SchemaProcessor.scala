package org.apache.kylin.engine.spark.utils

import org.apache.kylin.metadata.datatype.DataType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType}

object SchemaProcessor extends Logging {
	// scalastyle:off
	def toSparkType(dataTp: DataType, isSum: Boolean = false): org.apache.spark.sql.types.DataType = {
		dataTp.getName match {
			// org.apache.spark.sql.catalyst.expressions.aggregate.Sum#resultType
			case "decimal" =>
				if (isSum) {
					val i = dataTp.getPrecision + 10
					DecimalType(Math.min(DecimalType.MAX_PRECISION, i), dataTp.getScale)
				}
				else DecimalType(dataTp.getPrecision, dataTp.getScale)
			case "date" => DateType
			case "time" => DateType
			case "timestamp" => TimestampType
			case "datetime" => DateType
			case "tinyint" => if (isSum) LongType else ByteType
			case "smallint" => if (isSum) LongType else ShortType
			case "integer" => if (isSum) LongType else IntegerType
			case "int4" => if (isSum) LongType else IntegerType
			case "bigint" => LongType
			case "long8" => LongType
			case "float" => if (isSum) DoubleType else FloatType
			case "double" => DoubleType
			case tp if tp.startsWith("varchar") => StringType
			case tp if tp.startsWith("char") => StringType
			case "dim_dc" => LongType
			case "boolean" => BooleanType
			case tp if tp.startsWith("hllc") => BinaryType
			case tp if tp.startsWith("bitmap") => BinaryType
			case tp if tp.startsWith("extendedcolumn") => BinaryType
			case tp if tp.startsWith("percentile") => BinaryType
			case tp if tp.startsWith("raw") => BinaryType
			case _ => throw new IllegalArgumentException(dataTp.toString)
		}
	}
}
