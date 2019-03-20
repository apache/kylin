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
package org.apache.kylin.source.kafka.hadoop;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

/**
 * Unit Tests for {@link KafkaInputRecordReader}.
 *
 * @author Michael Hausegger
 * @date 25.02.2019
 * @see KafkaInputRecordReader
 */
public class KafkaInputRecordReaderTest {

    @Test
    public void testNextKeyValue()  throws Throwable  {
        KafkaInputRecordReader kafkaInputRecordReader = new KafkaInputRecordReader();
        assertFalse(kafkaInputRecordReader.nextKeyValue());
        assertFalse(kafkaInputRecordReader.nextKeyValue());
    }

    @Test
    public void testGetProgress()  throws Throwable  {
        KafkaInputRecordReader kafkaInputRecordReader = new KafkaInputRecordReader();
        assertEquals(1.0F, kafkaInputRecordReader.getProgress(), 0.01F);
    }

    @Test
    public void testGetCurrentKey()  throws Throwable  {
        KafkaInputRecordReader kafkaInputRecordReader = new KafkaInputRecordReader();
        kafkaInputRecordReader.nextKeyValue();
        assertEquals(0L, kafkaInputRecordReader.getCurrentKey().get());
    }

    @Test
    public void testGetCurrentValue()  throws Throwable  {
        KafkaInputRecordReader kafkaInputRecordReader = new KafkaInputRecordReader();
        kafkaInputRecordReader.nextKeyValue();
        assertEquals(0, kafkaInputRecordReader.getCurrentValue().getBytes().length);
    }
}