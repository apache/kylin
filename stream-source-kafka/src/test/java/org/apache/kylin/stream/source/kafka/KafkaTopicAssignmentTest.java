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
 *
 */
public class KafkaTopicAssignmentTest{

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
  public void testEqualsAndEqualsReturningFalseTwo() {
      Integer integer = new Integer((-1308));
      List<TopicPartition> linkedList = new LinkedList<>();
      List<TopicPartition> linkedListTwo = new LinkedList<>(linkedList);
      KafkaTopicAssignment kafkaTopicAssignment = new KafkaTopicAssignment(integer, linkedListTwo);
      KafkaTopicAssignment kafkaTopicAssignmentTwo = new KafkaTopicAssignment(integer, linkedList);

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