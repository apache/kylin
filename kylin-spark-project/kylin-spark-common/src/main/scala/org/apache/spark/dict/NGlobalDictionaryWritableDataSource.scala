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

package org.apache.spark.dict

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, WriteSupport}
import org.apache.spark.sql.types.{BinaryType, IntegerType, StructField, StructType}
import org.apache.spark.util.SerializableConfiguration

import java.util.Optional
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable.ListBuffer


class NGlobalDictionaryWritableDataSource extends DataSourceV2
  with WriteSupport {
  override def createWriter(writeUUID: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = {
    val conf = SparkContext.getActive.get.hadoopConfiguration
    Optional.of(new NGlobalDictDataSourceWriter(options.get("path").get(), new SerializableConfiguration(conf)))
  }

}

class NGlobalDictDataSourceWriter(path: String, conf: SerializableConfiguration) extends DataSourceWriter
  with Serializable {
  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    new NGlobalDictDataWriterFactory(path, conf)
  }


  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val outputDir = new Path(path)
    val fs = outputDir.getFileSystem(conf.value)
    messages.foreach { msg =>
      msg match {
        case DictBucketWriterCommitMessage(_, dictFiles) =>{
          dictFiles.foreach {
            file => {
              val f = new Path(file)
              fs.rename(f, new Path(outputDir, f.getName))
            }
          }
        }
      }
    }

    fs.delete(new Path(outputDir, "_temporary"), true)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {

  }
}

class NGlobalDictDataWriterFactory(path: String, conf: SerializableConfiguration) extends DataWriterFactory[InternalRow] {
  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    val workingDir = new Path(path);
    val fs = workingDir.getFileSystem(conf.value)
    val taskAttemptDir = new Path(path, new Path("_temporary" +
      Path.SEPARATOR + partitionId + Path.SEPARATOR + taskId))
    fs.mkdirs(taskAttemptDir)
    new NGlobalDictBucketDataWriter(fs, taskAttemptDir, partitionId)
  }
}

class NGlobalDictBucketDataWriter(fs: FileSystem, taskAttemptDir: Path, partitionId: Int) extends DataWriter[InternalRow] {

  private var curFileNo = -1
  private var curOutputStream : FSDataOutputStream = null
  private var curFile : String = null
  var dictFiles = ListBuffer[String]()
  override def write(record: InternalRow): Unit = {
    val fileNo = record.getInt(0)
    if(!curFileNo.equals(fileNo)){
      completeCurFile()
      val file = new Path(taskAttemptDir, new Path(NGlobalDictOutputFileName.FILE_PREFIX(fileNo) + partitionId))
      curOutputStream = fs.create(file)
      curFile = file.toUri.toString
      curFileNo = fileNo
    }

    curOutputStream.write(record.getBinary(1));
  }

  def completeCurFile(): Unit ={
    if(curOutputStream != null){
      curOutputStream.close()
      dictFiles += curFile
      curFile = null
      curOutputStream = null
    }
  }

  override def commit(): WriterCommitMessage = {
    completeCurFile()
    DictBucketWriterCommitMessage(partitionId, dictFiles)
  }

  override def abort(): Unit = {
    completeCurFile()
  }
}
case class DictBucketWriterCommitMessage(partition: Int, dictFiles: Seq[String]) extends WriterCommitMessage

object NGlobalDictOutputFileName {
  val FILE_PREFIX = Array("CURR_","PREV_")
  val FILE_NUMBER_OF_CURR : Int = 0
  val FILE_NUMBER_OF_PREV : Int = 1
}

object DictHelper extends Logging{

  def convertToRowIterator(bucketDict : NBucketDictionary): Iterator[Row] = {
    val curr = bucketDict.newCurrDictDataProduceIterator().asScala
    val pre = bucketDict.newPreDictDataProduceIterator().asScala
    new DictHelper.DictDataRow(NGlobalDictOutputFileName.FILE_NUMBER_OF_CURR, curr) ++
      new DictHelper.DictDataRow(NGlobalDictOutputFileName.FILE_NUMBER_OF_PREV, pre)
  }

  def rowEncoder: ExpressionEncoder[Row] = {
    val schema = StructType(
      StructField("file_number", IntegerType, nullable = false) ::
        StructField("file_data", BinaryType, nullable = false) :: Nil)
    RowEncoder(schema)
  }

  class DictDataRow(fileNumber : Int, dictData : Iterator[Array[Byte]]) extends Iterator[Row] {
    override def hasNext: Boolean = dictData.hasNext

    override def next(): Row = {
      Row.apply(fileNumber , dictData.next())
    }
  }

}
