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