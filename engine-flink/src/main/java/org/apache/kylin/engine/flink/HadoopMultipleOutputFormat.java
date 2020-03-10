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

package org.apache.kylin.engine.flink;

import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormatBase;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.task.ReduceContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl.DummyReporter;
import org.apache.hadoop.util.Progress;

import java.io.IOException;

public class HadoopMultipleOutputFormat<K, V> extends HadoopOutputFormatBase<K, V, Tuple2<String, Tuple3<K, V, String>>> {

    private static final long serialVersionUID = 1L;

    protected static final Object OPEN_MULTIPLE_MUTEX = new Object();
    protected static final Object CLOSE_MULTIPLE_MUTEX = new Object();

    protected MultipleOutputs writer;

    public HadoopMultipleOutputFormat(OutputFormat<K, V> mapreduceOutputFormat, Job job) {
        super(mapreduceOutputFormat, job);
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.open(taskNumber, numTasks);

        synchronized (OPEN_MULTIPLE_MUTEX) {
            try {
                TaskInputOutputContext taskInputOutputContext = new ReduceContextImpl(configuration,
                        context.getTaskAttemptID(), new InputIterator(), new GenericCounter(), new GenericCounter(),
                        recordWriter, outputCommitter, new DummyReporter(), null,
                        BytesWritable.class, BytesWritable.class);
                this.writer = new MultipleOutputs(taskInputOutputContext);
            } catch (InterruptedException e) {
                throw new IOException("Could not create MultipleOutputs.", e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        synchronized (CLOSE_MULTIPLE_MUTEX) {
            try {
                this.writer.close();
            } catch (InterruptedException e) {
                throw new IOException("Could not close MultipleOutputs.", e);
            }
        }
    }

    @Override
    public void writeRecord(Tuple2<String, Tuple3<K, V, String>> record) throws IOException {
        try {
            this.writer.write(record.f0, record.f1.f0, record.f1.f1, record.f1.f2);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    static class InputIterator implements RawKeyValueIterator {
        @Override
        public DataInputBuffer getKey() throws IOException {
            return null;
        }

        @Override
        public DataInputBuffer getValue() throws IOException {
            return null;
        }

        @Override
        public Progress getProgress() {
            return null;
        }

        @Override
        public boolean next() throws IOException {
            return false;
        }

        @Override
        public void close() throws IOException {
        }
    }
}
