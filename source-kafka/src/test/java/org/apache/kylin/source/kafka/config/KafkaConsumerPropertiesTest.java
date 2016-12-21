package org.apache.kylin.source.kafka.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

public class KafkaConsumerPropertiesTest extends LocalFileMetadataTestCase {
    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testLoadKafkaProperties() {
        KafkaConsumerProperties kafkaConsumerProperties = KafkaConsumerProperties.getInstanceFromEnv();
        assertFalse(kafkaConsumerProperties.getProperties().containsKey("acks"));
        assertTrue(kafkaConsumerProperties.getProperties().containsKey("session.timeout.ms"));
        assertEquals("30000", kafkaConsumerProperties.getProperties().getProperty("session.timeout.ms"));
    }

    @Test
    public void testLoadKafkaPropertiesAsHadoopJobConf() throws IOException, ParserConfigurationException, SAXException {
        KafkaConsumerProperties kafkaConsumerProperties = KafkaConsumerProperties.getInstanceFromEnv();
        Configuration conf = new Configuration(false);
        conf.addResource(new FileInputStream(new File(kafkaConsumerProperties.getKafkaConsumerHadoopJobConf())), KafkaConsumerProperties.KAFKA_CONSUMER_FILE);
        assertEquals("30000", conf.get("session.timeout.ms"));

        Properties prop = KafkaConsumerProperties.getProperties(conf);
        assertEquals("30000", prop.getProperty("session.timeout.ms"));
    }
}
