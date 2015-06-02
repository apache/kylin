package org.apache.kylin.streaming;

/**
 */
public interface MicroStreamBatchConsumer {

    void consume(MicroStreamBatch microStreamBatch) throws Exception;

    void stop();

}
