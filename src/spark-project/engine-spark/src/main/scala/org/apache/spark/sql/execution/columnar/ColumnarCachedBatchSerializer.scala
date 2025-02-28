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

package org.apache.spark.sql.execution.columnar

import java.io.{IOException, InputStream}
import java.lang.reflect.Method
import java.nio.ByteBuffer
import java.time.ZoneId
import java.util

import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.parquet.column.{ColumnDescriptor, ParquetProperties}
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.apache.parquet.hadoop._
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io._
import org.apache.parquet.schema.Type
import org.apache.parquet.{HadoopReadOptions, ParquetReadOptions, VersionParser}
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, UnsafeRow}
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.columnar.{CachedBatch, CachedBatchSerializer}
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.execution.datasources.parquet._
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._
import scala.collection.SeqLike
import scala.collection.mutable.ArrayBuffer

class ColumnarCachedBatchSerializer extends CachedBatchSerializer {

  override def vectorTypes(attributes: Seq[Attribute], conf: SQLConf): Option[Seq[String]] =
    Option(Seq.fill(attributes.length)(
      if (!conf.offHeapColumnVectorEnabled) {
        classOf[OnHeapColumnVector].getName
      } else {
        classOf[OffHeapColumnVector].getName
      }
    ))

  override def buildFilter(predicates: Seq[Expression],
                           cachedAttributes: Seq[Attribute]): (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = {
    // Essentially a NOOP.
    (_: Int, b: Iterator[CachedBatch]) => b
  }

  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = {
    schema.map(_.dataType).forall(isTypeSupported)
  }

  override def supportsColumnarOutput(schema: StructType): Boolean = {
    schema.fields.map(_.dataType).forall(isTypeSupported)
  }

  override def convertInternalRowToCachedBatch(input: RDD[InternalRow], //
                                               schema: Seq[Attribute], //
                                               storageLevel: StorageLevel, //
                                               conf: SQLConf): RDD[CachedBatch] = {
    val broadCastedConf = SparkSession.active.sparkContext.broadcast(conf.getAllConfs)
    input.mapPartitions { batchIterator =>
      new InternalRowToCachedBatchIterator(batchIterator, schema, broadCastedConf)
    }
  }

  override def convertColumnarBatchToCachedBatch(input: RDD[ColumnarBatch], //
                                                 schema: Seq[Attribute], //
                                                 storageLevel: StorageLevel, //
                                                 conf: SQLConf): RDD[CachedBatch] = {
    val broadCastedConf = SparkSession.active.sparkContext.broadcast(conf.getAllConfs)
    input.mapPartitions { batchIterator =>
      new ColumnarBatchToCachedBatchIterator(batchIterator, schema, broadCastedConf)
    }
  }

  override def convertCachedBatchToInternalRow(input: RDD[CachedBatch], //
                                               cacheAttributes: Seq[Attribute], //
                                               selectedAttributes: Seq[Attribute], //
                                               conf: SQLConf): RDD[InternalRow] = {
    val broadCastedConf = SparkSession.active.sparkContext.broadcast(conf.getAllConfs)
    input.mapPartitions { batchIterator =>
      new CachedBatchToInternalRowIterator(batchIterator, cacheAttributes, selectedAttributes, broadCastedConf)
    }
  }

  override def convertCachedBatchToColumnarBatch(input: RDD[CachedBatch], //
                                                 cacheAttributes: Seq[Attribute], //
                                                 selectedAttributes: Seq[Attribute], //
                                                 conf: SQLConf): RDD[ColumnarBatch] = {
    val broadCastedConf = SparkSession.active.sparkContext.broadcast(conf.getAllConfs)
    input.mapPartitions { batchIterator =>
      new CachedBatchToColumnarBatchIterator(batchIterator, cacheAttributes, selectedAttributes, broadCastedConf)
    }
  }

  private def isTypeSupported(dataType: DataType): Boolean = {
    DataTypes.NullType == dataType || isTypeSupportedByColumnarWriter(dataType)
  }

  private def isTypeSupportedByColumnarWriter(dataType: DataType): Boolean = {
    // Columnar writer in Spark only supports AtomicTypes ATM
    dataType match {
      case TimestampType | StringType | BooleanType | DateType | BinaryType |
           DoubleType | FloatType | ByteType | IntegerType | LongType | ShortType => true
      case _: DecimalType => true
      case _ => false
    }
  }

  /**
   * This method checks if the datatype passed is officially supported by parquet.
   *
   * Please refer to https://github.com/apache/parquet-format/blob/master/LogicalTypes.md to see
   * the what types are supported by parquet.
   */
  private def isTypeSupportedByParquet(dataType: DataType): Boolean = {
    dataType match {
      case CalendarIntervalType | NullType => false
      case s: StructType => s.forall(field => isTypeSupportedByParquet(field.dataType))
      case ArrayType(elementType, _) => isTypeSupportedByParquet(elementType)
      case MapType(keyType, valueType, _) => isTypeSupportedByParquet(keyType) &&
        isTypeSupportedByParquet(valueType)
      case d: DecimalType if d.scale < 0 => false
      case _ => true
    }
  }

  // --------------------------- InternalRow or ColumnarBatch to CachedBatch --------------------------- //
  private abstract class ToCachedBatch(schema: Seq[Attribute],
                                       sharedConf: Broadcast[Map[String, String]]) extends Iterator[CachedBatch] {

    protected val conf: SQLConf = getSharedConf(sharedConf)

    private val bytesAllowedPerBatch = 256 * 1024 * 1024 // 256MB

    private val sparkSchema = schema.toStructType

    private val requestedSchema = sparkSchema

    private val hadoopConf: Configuration = getHadoopConf(sparkSchema, requestedSchema, conf)

    private val parquetOutputFileFormat = new ParquetOutputFileFormat()

    protected val codec: CompressionCodecName = //
      ParquetOptions.getParquetCompressionCodecName(conf.parquetCompressionCodec)

    protected def getInternalRowIterator: Iterator[InternalRow]

    override def hasNext: Boolean = getInternalRowIterator.hasNext

    // Estimate the size of a row.
    private val estimatedSize: Int = schema.map { attr =>
      attr.dataType.defaultSize
    }.sum

    override def next(): CachedBatch = {
      val rowIterator = getInternalRowIterator
      // Total row count.
      var rows = 0
      // At least one single block.
      autoClose(new ByteArrayOutputStream(ByteArrayOutputFile.BLOCK_SIZE)) { stream =>
        val outputFile: OutputFile = new ByteArrayOutputFile(stream)
        val recordWriter = SQLConf.withExistingConf(conf) {
          parquetOutputFileFormat.getRecordWriter(hadoopConf, outputFile, codec)
        }
        try {
          var totalSize = 0
          while (rowIterator.hasNext && totalSize < bytesAllowedPerBatch) {
            val row = rowIterator.next()
            totalSize += {
              row match {
                case r: UnsafeRow =>
                  r.getSizeInBytes
                case _ =>
                  estimatedSize
              }
            }
            recordWriter.write(null, row)
            rows += 1
          }
        } finally {
          // Passing null as context isn't used in this method
          recordWriter.close(null)
        }
        ParquetCachedBatch(rows, stream.toByteArray)
      }
    }

  }

  private class InternalRowToCachedBatchIterator(iter: Iterator[InternalRow],
                                                 schema: Seq[Attribute],
                                                 sharedConf: Broadcast[Map[String, String]]) //
    extends ToCachedBatch(schema, sharedConf) {

    protected override def getInternalRowIterator: Iterator[InternalRow] = {
      iter
    }
  }

  private class ColumnarBatchToCachedBatchIterator(iter: Iterator[ColumnarBatch],
                                                   schema: Seq[Attribute],
                                                   sharedConf: Broadcast[Map[String, String]]) //
    extends ToCachedBatch(schema, sharedConf) {

    protected override def getInternalRowIterator: Iterator[InternalRow] = internalRowIterator

    private lazy val internalRowIterator: Iterator[InternalRow] = new InnerIterator[ColumnarBatch, InternalRow] {

      override protected def srcIter: Iterator[ColumnarBatch] = iter

      override protected def nextInnerIterator: Iterator[InternalRow] = {
        assert(iter.hasNext, "No more columnar batch found.")
        iter.next().rowIterator().asScala
      }
    }
  }

  // --------------------------- InternalRow or ColumnarBatch to CachedBatch --------------------------- //

  private trait InnerIterator[F, T] extends Iterator[T] {

    override def hasNext: Boolean = {
      while ((innerIterator == null || !innerIterator.hasNext) && srcIter.hasNext) {
        innerIterator = nextInnerIterator
      }
      innerIterator != null && innerIterator.hasNext
    }

    override def next(): T = {
      assert(hasNext, "No more element found.")
      innerIterator.next()
    }

    protected def srcIter: Iterator[F]

    protected def nextInnerIterator: Iterator[T]

    private var innerIterator: Iterator[T] = if (srcIter.hasNext) {
      nextInnerIterator
    } else {
      Iterator.empty
    }
  }

  // --------------------------- CachedBatch to InternalRow or ColumnarBatch --------------------------- //

  private class FromCachedBatch(cacheAttributes: Seq[Attribute],
                                selectedAttributes: Seq[Attribute],
                                sharedConf: Broadcast[Map[String, String]]) {
    protected val conf: SQLConf = getSharedConf(sharedConf)

    protected val convertTz: Option[ZoneId] = None

    protected val datetimeRebaseMode: SQLConf.LegacyBehaviorPolicy.Value = //
      LegacyBehaviorPolicy.withName(conf.getConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE))


    protected val datetimeRebaseModeInRead: SQLConf.LegacyBehaviorPolicy.Value = //
      LegacyBehaviorPolicy.withName(conf.getConf(SQLConf.PARQUET_REBASE_MODE_IN_READ))


    protected val int96RebaseMode: SQLConf.LegacyBehaviorPolicy.Value = //
      LegacyBehaviorPolicy.withName(conf.getConf(SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE))

    protected val int96RebaseModeInRead: SQLConf.LegacyBehaviorPolicy.Value = //
      LegacyBehaviorPolicy.withName(conf.getConf(SQLConf.PARQUET_INT96_REBASE_MODE_IN_READ))

    private val sparkSchema = cacheAttributes.toStructType
    private val requestedSchema = selectedAttributes.toStructType
    protected val hadoopConf: Configuration = getHadoopConf(sparkSchema, requestedSchema, conf)
    protected val readOptions: ParquetReadOptions = HadoopReadOptions.builder(hadoopConf).build()

    /**
     * We are getting this method using reflection because its a package-private
     */
    protected val readBatchMethod: Method = {
      val reflected = classOf[VectorizedColumnReader].getDeclaredMethod("readBatch", Integer.TYPE,
        classOf[WritableColumnVector])
      reflected.setAccessible(true)
      reflected
    }

  }

  private class CachedBatchToInternalRowIterator(iter: Iterator[CachedBatch],
                                                 cacheAttributes: Seq[Attribute],
                                                 selectedAttributes: Seq[Attribute],
                                                 sharedConf: Broadcast[Map[String, String]]) //
    extends FromCachedBatch(cacheAttributes, selectedAttributes, sharedConf) with InnerIterator[CachedBatch, InternalRow] {


    override protected def srcIter: Iterator[CachedBatch] = iter

    override protected def nextInnerIterator: Iterator[InternalRow] = {

      assert(iter.hasNext, "No more cached batch found.")

      val parquetCachedBatch = iter.next().asInstanceOf[ParquetCachedBatch]
      val inputFile = new ByteArrayInputFile(parquetCachedBatch.buffer)


      autoClose(ParquetFileReader.open(inputFile, readOptions)) { fileReader =>
        val parquetSchema = fileReader.getFooter.getFileMetaData.getSchema
        // TODO Maybe we should read batch by batch.
        val unsafeRows = new ArrayBuffer[InternalRow]
        import org.apache.parquet.io.ColumnIOFactory
        var pages = fileReader.readNextRowGroup()
        while (pages != null) {
          val rows = pages.getRowCount
          val columnIO = new ColumnIOFactory().getColumnIO(parquetSchema)

          val recordReader = // Maybe we should only read the selected columns.
            columnIO.getRecordReader(pages, new ShadeParquetRecordMaterializer(parquetSchema,
              cacheAttributes.toStructType,
              new ParquetToSparkSchemaConverter(hadoopConf), convertTz, RebaseSpec(datetimeRebaseMode), RebaseSpec(int96RebaseMode)))
          for (_ <- 0 until rows.toInt) {
            val row = recordReader.read
            unsafeRows += row.copy()
          }
          pages = fileReader.readNextRowGroup()
        }

        val rowsIterator = unsafeRows.iterator
        val unsafeProjection = // Read all cached columns then project selected.
          GenerateUnsafeProjection.generate(selectedAttributes, cacheAttributes)
        rowsIterator.map(unsafeProjection)
      }
    }
  }

  private class CachedBatchToColumnarBatchIterator(iter: Iterator[CachedBatch],
                                                   cacheAttributes: Seq[Attribute],
                                                   selectedAttributes: Seq[Attribute],
                                                   sharedConf: Broadcast[Map[String, String]]) //
    extends FromCachedBatch(cacheAttributes, selectedAttributes, sharedConf) with InnerIterator[CachedBatch, ColumnarBatch] {

    override protected def srcIter: Iterator[CachedBatch] = iter

    override protected def nextInnerIterator: Iterator[ColumnarBatch] = {
      assert(iter.hasNext, "No more cached batch found.")
      val parquetCachedBatch = iter.next().asInstanceOf[ParquetCachedBatch]
      new ColumnarBatchIterator(parquetCachedBatch)
    }

    private class ColumnarBatchIterator(parquetCachedBatch: ParquetCachedBatch)
      extends Iterator[ColumnarBatch] with AutoCloseable {

      private val capacity = conf.parquetVectorizedReaderBatchSize
      private val offHeapColumnVectorEnabled = conf.offHeapColumnVectorEnabled

      private val columnVectors: Array[_ <: WritableColumnVector] = if (offHeapColumnVectorEnabled) {
        OffHeapColumnVector.allocateColumns(capacity, selectedAttributes.toStructType)
      } else {
        OnHeapColumnVector.allocateColumns(capacity, selectedAttributes.toStructType)
      }

      private val columnarBatch = new ColumnarBatch(columnVectors.asInstanceOf[Array[ColumnVector]])

      private var columnReaders: Array[VectorizedColumnReader] = _

      private var rowsReturned: Long = 0L
      private var totalCountLoadedSoFar: Long = 0

      private val fileReader = ParquetFileReader.open(new ByteArrayInputFile(parquetCachedBatch.buffer), readOptions)

      private val footerFileMetaData = fileReader.getFooter.getFileMetaData
      private val datetimeRebaseSpec = DataSourceUtils.datetimeRebaseSpec(
        footerFileMetaData.getKeyValueMetaData.get,
        datetimeRebaseModeInRead.toString)

      private val int96RebaseSpec = DataSourceUtils.int96RebaseSpec(
        footerFileMetaData.getKeyValueMetaData.get,
        int96RebaseModeInRead.toString)

      // Ensure file reader was closed.
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

      private val (totalRowCount, columnsRequested, cacheSchemaToReqSchemaMap, missingColumns,
      columnsInCache, typesInCache) = {
        val totalRowCount = fileReader.getRowGroups.asScala.map(_.getRowCount).sum
        // We are getting parquet schema and then converting it to catalyst schema,
        // because catalyst schema that we get from Spark doesn't have the exact schema
        // expected by the columnar parquet reader.
        val cacheParquetSchema = fileReader.getFooter.getFileMetaData.getSchema
        val cacheSparkSchema = new ParquetToSparkSchemaConverter(hadoopConf).convert(cacheParquetSchema)
        val reqSparkSchema = StructType(selectedAttributes.toStructType.map { field =>
          cacheSparkSchema.fields(cacheSparkSchema.fieldIndex(field.name))
        })
        val sparkToParquetSchemaConverter = new SparkToParquetSchemaConverter(hadoopConf)
        val reqParquetSchema = sparkToParquetSchemaConverter.convert(reqSparkSchema)
        val columnsRequested: util.List[ColumnDescriptor] = reqParquetSchema.getColumns

        val reqSparkSchemaInCacheOrder = StructType(cacheSparkSchema.filter(f =>
          reqSparkSchema.fields.exists(f0 => f0.name.equals(f.name))))

        // There could be a case especially in a distributed environment where the requestedSchema
        // and cacheSchema are not in the same order. We need to create a map so we can guarantee
        // that we writing to the correct columnVector.
        val cacheSchemaToReqSchemaMap: Map[Int, Int] = reqSparkSchemaInCacheOrder.indices.map {
          index => index -> reqSparkSchema.fields.indexOf(reqSparkSchemaInCacheOrder.fields(index))
        }.toMap

        val reqParquetSchemaInCacheOrder =
          sparkToParquetSchemaConverter.convert(reqSparkSchemaInCacheOrder)

        // reset spark schema calculated from parquet schema
        hadoopConf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, reqSparkSchema.json)
        hadoopConf.set(ParquetWriteSupport.SPARK_ROW_SCHEMA, reqSparkSchema.json)

        val columnsInCache: util.List[ColumnDescriptor] = reqParquetSchemaInCacheOrder.getColumns
        val typesInCache: util.List[Type] = reqParquetSchemaInCacheOrder.asGroupType.getFields
        val missingColumns = new Array[Boolean](reqParquetSchema.getFieldCount)

        // Initialize missingColumns to cover the case where requested column isn't present in the
        // cache, which should never happen but just in case it does.
        val paths: util.List[Array[String]] = reqParquetSchema.getPaths

        for (i <- 0 until reqParquetSchema.getFieldCount) {
          val t = reqParquetSchema.getFields.get(i)
          if (!t.isPrimitive || t.isRepetition(Type.Repetition.REPEATED)) {
            throw new UnsupportedOperationException("Complex types not supported.")
          }
          val colPath = paths.get(i)
          if (cacheParquetSchema.containsPath(colPath)) {
            val fd = cacheParquetSchema.getColumnDescription(colPath)
            if (!fd.equals(columnsRequested.get(i))) {
              throw new UnsupportedOperationException("Schema evolution not supported.")
            }
            missingColumns(i) = false
          } else {
            if (columnsRequested.get(i).getMaxDefinitionLevel == 0) {
              // Column is missing in data but the required data is non-nullable.
              // This file is invalid.
              throw new IOException(s"Required column is missing in data file: ${colPath.mkString("[", ",", "]")}")
            }
            missingColumns(i) = true
          }
        }

        // Initialize missing columns with nulls.
        for (i <- missingColumns.indices) {
          if (missingColumns(i)) {
            columnVectors(i).putNulls(0, capacity)
            columnVectors(i).setIsConstant()
          }
        }

        (totalRowCount, columnsRequested, cacheSchemaToReqSchemaMap, missingColumns,
          columnsInCache, typesInCache)
      }


      override def hasNext: Boolean = rowsReturned < totalRowCount

      override def next(): ColumnarBatch = {
        assert(nextBatch, "No more columnar batch found.")
        columnarBatch
      }

      override def close(): Unit = {
        try {
          fileReader.close()
        } finally {
          columnarBatch.close()
        }
      }

      @throws(classOf[IOException])
      private def nextBatch: Boolean = {
        columnVectors.foreach(_.reset())
        columnarBatch.setNumRows(0)
        if (rowsReturned >= totalRowCount) return false
        checkEndOfRowGroup()
        val num = Math.min(capacity.toLong, totalCountLoadedSoFar - rowsReturned).toInt
        for (i <- columnReaders.indices) {
          if (columnReaders(i) != null) {
            readBatchMethod.invoke(columnReaders(i), num.asInstanceOf[AnyRef],
              columnVectors(cacheSchemaToReqSchemaMap(i)).asInstanceOf[AnyRef])
          }
        }
        rowsReturned += num
        columnarBatch.setNumRows(num)
        true
      }

      @throws(classOf[IOException])
      private def checkEndOfRowGroup(): Unit = {
        if (rowsReturned != totalCountLoadedSoFar) return
        val pages = fileReader.readNextRowGroup
        if (pages == null) {
          throw new IOException(s"Expecting more rows but reached last block. Read $rowsReturned out of $totalRowCount")
        }

        val versionParser = VersionParser.parse(fileReader.getFileMetaData().getCreatedBy())

        columnReaders = new Array[VectorizedColumnReader](columnsRequested.size)
        for (i <- 0 until columnsRequested.size) {
          if (!missingColumns(i)) {
            columnReaders(i) =
              new VectorizedColumnReader(columnsInCache.get(i),
                true,
                pages,
                convertTz.orNull,
                datetimeRebaseSpec.mode.toString,
                datetimeRebaseSpec.timeZone.toString,
                int96RebaseSpec.mode.toString,
                int96RebaseSpec.timeZone.toString,
                versionParser)
          }
        }
        totalCountLoadedSoFar += pages.getRowCount
      }

    }

  }

  // --------------------------- CachedBatch to InternalRow or ColumnarBatch --------------------------- //


  private case class ParquetCachedBatch(numRows: Int,
                                        buffer: Array[Byte]) extends CachedBatch {
    override def sizeInBytes: Long = buffer.length
  }

  private def getSharedConf(sharedConf: Broadcast[Map[String, String]]): SQLConf = {
    val conf = new SQLConf()
    sharedConf.value.foreach { case (k, v) => conf.setConfString(k, v) }
    conf
  }

  private def getHadoopConf(sparkSchema: StructType,
                            requestedSchema: StructType,
                            sqlConf: SQLConf): Configuration = {

    val hadoopConf = new Configuration(false)
    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS,
      classOf[ParquetReadSupport].getName)

    hadoopConf.set(ParquetWriteSupport.SPARK_ROW_SCHEMA,
      sparkSchema.json)

    hadoopConf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      requestedSchema.json)

    hadoopConf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key,
      sqlConf.sessionLocalTimeZone)

    hadoopConf.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING.key,
      sqlConf.isParquetBinaryAsString)
    hadoopConf.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      sqlConf.isParquetINT96AsTimestamp)

    // datetimeRebaseMode
    hadoopConf.set(SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key,
      sqlConf.getConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE))

    // int96RebaseMode
    hadoopConf.set(SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE.key,
      sqlConf.getConf(SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE))

    hadoopConf.set(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key,
      sqlConf.parquetOutputTimestampType.toString)

    hadoopConf.setBoolean(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key,
      sqlConf.writeLegacyParquetFormat)

    hadoopConf.setIfUnset(ParquetOutputFormat.WRITER_VERSION,
      ParquetProperties.WriterVersion.PARQUET_1_0.toString)

    ParquetWriteSupport.setSchema(requestedSchema, hadoopConf)

    hadoopConf
  }


  private[columnar] class ByteArrayInputFile(buff: Array[Byte]) extends InputFile with Serializable {

    override def getLength: Long = buff.length

    override def newStream(): SeekableInputStream = {
      val byteBuffer = ByteBuffer.wrap(buff)
      new DelegatingSeekableInputStream(new ByteBufferInputStream(byteBuffer)) {
        override def getPos: Long = byteBuffer.position()

        override def seek(newPos: Long): Unit = {
          if (newPos > Int.MaxValue || newPos < Int.MinValue) {
            throw new UnsupportedOperationException(s"Seek value is out of supported range $newPos.")
          }
          byteBuffer.position(newPos.toInt)
        }
      }
    }
  }

  private object ByteArrayOutputFile extends Serializable {
    val BLOCK_SIZE: Int = 32 * 1024 * 1024 // 32MB
  }

  private class ByteArrayOutputFile(stream: ByteArrayOutputStream) extends OutputFile with Serializable {
    override def create(blockSizeHint: Long): PositionOutputStream = {
      new DelegatingPositionOutputStream(stream) {
        var pos = 0

        override def getPos: Long = pos

        override def write(b: Int): Unit = {
          super.write(b)
          pos += Integer.BYTES
        }

        override def write(b: Array[Byte]): Unit = {
          super.write(b)
          pos += b.length
        }

        override def write(b: Array[Byte], off: Int, len: Int): Unit = {
          super.write(b, off, len)
          pos += len
        }
      }
    }

    override def createOrOverwrite(blockSizeHint: Long): PositionOutputStream =
      throw new UnsupportedOperationException("Don't need to overwrite.")

    override def supportsBlockSize(): Boolean = true

    override def defaultBlockSize(): Long = ByteArrayOutputFile.BLOCK_SIZE
  }

  /**
   * Copied from Spark org.apache.spark.util.ByteBufferInputStream
   */
  private class ByteBufferInputStream(private var buffer: ByteBuffer)
    extends InputStream with Serializable {

    override def read(): Int = {
      if (buffer == null || buffer.remaining() == 0) {
        cleanUp()
        -1
      } else {
        buffer.get() & 0xFF
      }
    }

    override def read(dest: Array[Byte]): Int = {
      read(dest, 0, dest.length)
    }

    override def read(dest: Array[Byte], offset: Int, length: Int): Int = {
      if (buffer == null || buffer.remaining() == 0) {
        cleanUp()
        -1
      } else {
        val amountToGet = math.min(buffer.remaining(), length)
        buffer.get(dest, offset, amountToGet)
        amountToGet
      }
    }

    override def skip(bytes: Long): Long = {
      if (buffer != null) {
        val amountToSkip = math.min(bytes, buffer.remaining).toInt
        buffer.position(buffer.position() + amountToSkip)
        if (buffer.remaining() == 0) {
          cleanUp()
        }
        amountToSkip
      } else {
        0L
      }
    }

    /**
     * Clean up the buffer, and potentially dispose of it using StorageUtils.dispose().
     */
    private def cleanUp(): Unit = {
      if (buffer != null) {
        buffer = null
      }
    }
  }

  private def autoClose[T <: AutoCloseable, V](r: T)(block: T => V): V = {
    try {
      block(r)
    } finally {
      r.safeClose()
    }
  }

  import scala.language.implicitConversions

  implicit class AutoCloseableColumn[A <: AutoCloseable](autoCloseable: AutoCloseable) extends Serializable {

    private[columnar] def safeClose(e: Throwable = null): Unit = {
      if (autoCloseable != null) {
        if (e != null) {
          try {
            autoCloseable.close()
          } catch {
            case suppressed: Throwable => e.addSuppressed(suppressed)
          }
        } else {
          autoCloseable.close()
        }
      }
    }

  }

  implicit class AutoCloseableSeq[A <: AutoCloseable](val in: SeqLike[A, _]) extends Serializable {

    def safeClose(error: Throwable = null): Unit = if (in != null) {
      var closeException: Throwable = null
      in.foreach { element =>
        if (element != null) {
          try {
            element.close()
          } catch {
            case e: Throwable if error != null => error.addSuppressed(e)
            case e: Throwable if closeException == null => closeException = e
            case e: Throwable => closeException.addSuppressed(e)
          }
        }
      }
      if (closeException != null) {
        throw closeException
      }
    }
  }

  implicit class AutoCloseableArray[A <: AutoCloseable](val in: Array[A]) extends Serializable {
    def safeClose(e: Throwable = null): Unit = if (in != null) {
      in.toSeq.safeClose(e)
    }
  }


  private class ParquetOutputFileFormat extends Serializable {

    def getRecordWriter(conf: Configuration, output: OutputFile, codec: CompressionCodecName): RecordWriter[Void, InternalRow] = {
      import ParquetOutputFormat._


      val blockSize = getLongBlockSize(conf)
      val maxPaddingSize =
        conf.getInt(MAX_PADDING_BYTES, ParquetWriter.MAX_PADDING_SIZE_DEFAULT)
      val validating = getValidation(conf)

      val writeSupport = new ParquetWriteSupport().asInstanceOf[WriteSupport[InternalRow]]
      val init = writeSupport.init(conf)
      // TODO parquetOptions
      val writer = new ParquetFileWriter(output, init.getSchema,
        Mode.CREATE, blockSize, maxPaddingSize)
      writer.start()

      val writerVersion =
        ParquetProperties.WriterVersion.fromString(conf.get(ParquetOutputFormat.WRITER_VERSION,
          ParquetProperties.WriterVersion.PARQUET_1_0.toString))

      val codecFactory = new CodecFactory(conf, getPageSize(conf))

      new ParquetRecordWriter[InternalRow](writer, writeSupport, init.getSchema,
        init.getExtraMetaData, blockSize, getPageSize(conf),
        codecFactory.getCompressor(codec), getDictionaryPageSize(conf),
        getEnableDictionary(conf), validating, writerVersion,
        ParquetOutputFileFormat.getMemoryManager(conf))
    }
  }

  private object ParquetOutputFileFormat extends Serializable {

    @transient
    private var memoryManager: MemoryManager = _
    private val DEFAULT_MEMORY_POOL_RATIO: Float = 0.95f
    private val DEFAULT_MIN_MEMORY_ALLOCATION: Long = 4 * 1024 * 1024 // 4MB

    def getMemoryManager(conf: Configuration): MemoryManager = {
      synchronized {
        if (memoryManager == null) {
          import ParquetOutputFormat._
          val maxLoad = conf.getFloat(MEMORY_POOL_RATIO, DEFAULT_MEMORY_POOL_RATIO)
          val minAllocation = conf.getLong(MIN_MEMORY_ALLOCATION, DEFAULT_MIN_MEMORY_ALLOCATION)
          memoryManager = new MemoryManager(maxLoad, minAllocation)
        }
      }
      memoryManager
    }
  }

  private object ParquetOptions extends Serializable {

    // The parquet compression short names
    private val shortParquetCompressionCodecNames = Map(
      "none" -> CompressionCodecName.UNCOMPRESSED,
      "uncompressed" -> CompressionCodecName.UNCOMPRESSED,
      "snappy" -> CompressionCodecName.SNAPPY,
      "gzip" -> CompressionCodecName.GZIP,
      "lzo" -> CompressionCodecName.LZO,
      "lz4" -> CompressionCodecName.LZ4,
      "brotli" -> CompressionCodecName.BROTLI,
      "zstd" -> CompressionCodecName.ZSTD)

    def getParquetCompressionCodecName(name: String): CompressionCodecName = {
      shortParquetCompressionCodecNames(name)
    }
  }

}
