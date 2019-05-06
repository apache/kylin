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
package org.apache.kylin.stream.source.kafka;

import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for class {@link KafkaSource}.
 *
 * @see KafkaSource
 *
 */
public class KafkaSourceTest{

  @Test
  public void testGetStreamingMessageParserClass() throws ClassNotFoundException {
      ConcurrentHashMap<String, String> concurrentHashMap = new ConcurrentHashMap<>();
      Class<?> clasz = KafkaSource.getStreamingMessageParserClass(concurrentHashMap);

      assertTrue(concurrentHashMap.isEmpty());
      assertFalse(clasz.isEnum());
      assertFalse(clasz.isSynthetic());
      assertEquals("class org.apache.kylin.stream.source.kafka.TimedJsonStreamParser", clasz.toString());
      assertFalse(clasz.isPrimitive());
      assertFalse(clasz.isAnnotation());
      assertFalse(clasz.isInterface());
      assertFalse(clasz.isArray());
  }

  @Test(expected = ClassNotFoundException.class)
  public void testGetStreamingMessageParserClassThrowsClassNotFoundException() throws ClassNotFoundException {
      ConcurrentHashMap<String, String> concurrentHashMap = new ConcurrentHashMap<>();
      concurrentHashMap.put("message.parser", "ev");
      KafkaSource.getStreamingMessageParserClass(concurrentHashMap);
  }

}