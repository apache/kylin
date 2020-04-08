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
