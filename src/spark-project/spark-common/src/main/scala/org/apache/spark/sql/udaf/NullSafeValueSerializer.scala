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

package org.apache.spark.sql.udaf

import org.apache.spark.unsafe.types.UTF8String

import java.io.{DataInput, DataOutput}
import java.nio.charset.StandardCharsets

@SerialVersionUID(1)
sealed trait NullSafeValueSerializer {
  final def serialize(output: DataOutput, value: Any): Unit = {
    if (value == null) {
      output.writeInt(0)
    } else {
      serialize0(output, value.asInstanceOf[Any])
    }
  }

  @inline protected def serialize0(output: DataOutput, value: Any): Unit

  def deserialize(input: DataInput): Any = {
    val length = input.readInt()
    if (length == 0) {
      null
    } else {
      deSerialize0(input, length)
    }
  }

  @inline protected def deSerialize0(input: DataInput, length: Int): Any
}

@SerialVersionUID(1)
class BooleanSerializer extends NullSafeValueSerializer {
  override protected def serialize0(output: DataOutput, value: Any): Unit = {
    output.writeInt(1)
    output.writeBoolean(value.asInstanceOf[Boolean])
  }

  override protected def deSerialize0(input: DataInput, length: Int): Any = {
    input.readBoolean()
  }
}

@SerialVersionUID(1)
class ByteSerializer extends NullSafeValueSerializer {
  override def serialize0(output: DataOutput, value: Any): Unit = {
    output.writeInt(1)
    output.writeByte(value.asInstanceOf[Byte])
  }

  override def deSerialize0(input: DataInput, length: Int): Any = {
    input.readByte()
  }
}

@SerialVersionUID(1)
class ShortSerializer extends NullSafeValueSerializer {
  override def serialize0(output: DataOutput, value: Any): Unit = {
    output.writeInt(2)
    output.writeShort(value.asInstanceOf[Short])
  }

  override def deSerialize0(input: DataInput, length: Int): Any = {
    input.readShort()
  }
}

@SerialVersionUID(1)
class IntegerSerializer extends NullSafeValueSerializer {
  override def serialize0(output: DataOutput, value: Any): Unit = {
    output.writeInt(4)
    output.writeInt(value.asInstanceOf[Int])
  }

  override def deSerialize0(input: DataInput, length: Int): Any = {
    input.readInt()
  }
}

@SerialVersionUID(1)
class FloatSerializer extends NullSafeValueSerializer {
  override def serialize0(output: DataOutput, value: Any): Unit = {
    output.writeInt(4)
    output.writeFloat(value.asInstanceOf[Float])
  }

  override def deSerialize0(input: DataInput, length: Int): Any = {
    input.readFloat()
  }
}

@SerialVersionUID(1)
class DoubleSerializer extends NullSafeValueSerializer {
  override def serialize0(output: DataOutput, value: Any): Unit = {
    output.writeInt(8)
    output.writeDouble(value.asInstanceOf[Double])
  }

  override def deSerialize0(input: DataInput, length: Int): Any = {
    input.readDouble()
  }
}

@SerialVersionUID(1)
class LongSerializer extends NullSafeValueSerializer {
  override def serialize0(output: DataOutput, value: Any): Unit = {
    output.writeInt(8)
    output.writeLong(value.asInstanceOf[Long])
  }

  override def deSerialize0(input: DataInput, length: Int): Any = {
    input.readLong()
  }
}

@SerialVersionUID(1)
class StringSerializer extends NullSafeValueSerializer {
  override def serialize0(output: DataOutput, value: Any): Unit = {
    val bytes = value.toString.getBytes(StandardCharsets.UTF_8)
    output.writeInt(bytes.length)
    output.write(bytes)
  }

  override def deSerialize0(input: DataInput, length: Int): Any = {
    val bytes = new Array[Byte](length)
    input.readFully(bytes)
    UTF8String.fromBytes(bytes)
  }
}

@SerialVersionUID(1)
class DecimalSerializer extends NullSafeValueSerializer {
  override def serialize0(output: DataOutput, value: Any): Unit = {
    val decimal = value.asInstanceOf[BigDecimal]
    DecimalCodecUtil.encode(decimal, output)
  }

  override def deSerialize0(input: DataInput, length: Int): Any = {
    DecimalCodecUtil.decode(input)
  }
}

@SerialVersionUID(1)
class JavaBigDecimalSerializer extends NullSafeValueSerializer {
  override def serialize0(output: DataOutput, value: Any): Unit = {
    val decimal = BigDecimal.apply(value.asInstanceOf[java.math.BigDecimal])
    DecimalCodecUtil.encode(decimal, output);
  }

  override def deSerialize0(input: DataInput, length: Int): Any = {
    val decimal = DecimalCodecUtil.decode(input)
    decimal.asInstanceOf[BigDecimal].bigDecimal
  }
}

object DecimalCodecUtil {
  def encode(decimal: BigDecimal, output: DataOutput): Unit = {
    val bytes = decimal.toString().getBytes(StandardCharsets.UTF_8)
    output.writeInt(1 + bytes.length)
    output.writeByte(decimal.scale)
    output.writeInt(bytes.length)
    output.write(bytes)
  }

  def decode(input: DataInput): Any = {
    val scale = input.readByte()
    val length = input.readInt()
    val bytes = new Array[Byte](length)
    input.readFully(bytes)
    val decimal = BigDecimal.apply(new String(bytes, StandardCharsets.UTF_8))
    decimal.setScale(scale)
  }
}
