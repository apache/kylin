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

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for class {@link KafkaTopicAssignment}.
 *
 * @see KafkaTopicAssignment
 */
public class KafkaTopicAssignmentTest {

    @Test
    public void testEqualsReturningTrueForSameObject() {
        List<TopicPartition> linkedList = new LinkedList<>();
        KafkaTopicAssignment kafkaTopicAssignment = new KafkaTopicAssignment(0, linkedList);

        assertTrue(kafkaTopicAssignment.equals(kafkaTopicAssignment));
    }

    @Test
    public void testEqualsAndEqualsReturningTrueOne() {
        List<TopicPartition> linkedList = new LinkedList<>();
        KafkaTopicAssignment kafkaTopicAssignment = new KafkaTopicAssignment(0, linkedList);
        KafkaTopicAssignment kafkaTopicAssignmentTwo = new KafkaTopicAssignment(0, linkedList);

        assertTrue(kafkaTopicAssignment.equals(kafkaTopicAssignmentTwo));
        assertTrue(kafkaTopicAssignmentTwo.equals(kafkaTopicAssignment));
    }

    @Test
    public void testEqualsAndEqualsReturningFalseOne() {
        KafkaTopicAssignment kafkaTopicAssignment = new KafkaTopicAssignment(null, null);
        KafkaTopicAssignment kafkaTopicAssignmentTwo = new KafkaTopicAssignment(new Integer(0), null);

        assertFalse(kafkaTopicAssignment.equals(kafkaTopicAssignmentTwo));
        assertFalse(kafkaTopicAssignmentTwo.equals(kafkaTopicAssignment));
    }

    @Test
    public void testEqualsAndEqualsReturningFalseThree() {
        List<TopicPartition> linkedList = new LinkedList<>();
        Integer integer = new Integer((-21));
        KafkaTopicAssignment kafkaTopicAssignment = new KafkaTopicAssignment(integer, null);
        KafkaTopicAssignment kafkaTopicAssignmentTwo = new KafkaTopicAssignment(integer, linkedList);

        assertFalse(kafkaTopicAssignment.equals(kafkaTopicAssignmentTwo));
    }

    @Test
    public void testEqualsAndEqualsReturningFalseFour() {
        KafkaTopicAssignment kafkaTopicAssignment = new KafkaTopicAssignment(null, null);
        Integer integer = new Integer(0);
        KafkaTopicAssignment kafkaTopicAssignmentTwo = new KafkaTopicAssignment(integer, null);

        assertFalse(kafkaTopicAssignment.equals(kafkaTopicAssignmentTwo));
    }

    @Test
    public void testEqualsAndEqualsReturningTrueTwo() {
        List<TopicPartition> linkedList = new LinkedList<>();
        KafkaTopicAssignment kafkaTopicAssignment = new KafkaTopicAssignment(null, linkedList);
        KafkaTopicAssignment kafkaTopicAssignmentTwo = new KafkaTopicAssignment(null, linkedList);

        assertTrue(kafkaTopicAssignment.equals(kafkaTopicAssignmentTwo));
    }

    @Test
    public void testEqualsWithNull() {
        List<TopicPartition> linkedList = new LinkedList<TopicPartition>();
        KafkaTopicAssignment kafkaTopicAssignment = new KafkaTopicAssignment(null, linkedList);

        assertFalse(kafkaTopicAssignment.equals(null));
    }

    @Test
    public void testEqualsAndEqualsReturningFalseFive() {
        List<TopicPartition> linkedList = new LinkedList<>();
        KafkaTopicAssignment kafkaTopicAssignment = new KafkaTopicAssignment(null, linkedList);
        TopicPartition topicPartition = new TopicPartition("sW=V$uM^HI^g", 3221);

        assertFalse(kafkaTopicAssignment.equals(topicPartition));
    }

}