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

package org.apache.kylin.storage.hbase.steps;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.security.Credentials;
import org.apache.kylin.engine.mr.common.BatchConstants;

/**
 * @author yangli9
 *
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class MockupMapContext implements MapContext {

    private Configuration hconf;

    private Object[] outKV;

    public static Context create(final Configuration hconf, String metadataUrl, String cubeName, final Object[] outKV) {

        hconf.set(BatchConstants.CFG_CUBE_NAME, cubeName);

        return new WrappedMapper().getMapContext(new MockupMapContext(hconf, outKV));
    }

    public MockupMapContext(Configuration hconf, Object[] outKV) {
        this.hconf = hconf;
        this.outKV = outKV;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        throw new NotImplementedException();
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
        throw new NotImplementedException();
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
        throw new NotImplementedException();
    }

    @Override
    public void write(Object key, Object value) throws IOException, InterruptedException {
        System.out.println("Write -- k:" + key + ", v:" + value);
        if (outKV != null) {
            outKV[0] = key;
            outKV[1] = value;
        }
    }

    @Override
    public OutputCommitter getOutputCommitter() {
        throw new NotImplementedException();
    }

    @Override
    public TaskAttemptID getTaskAttemptID() {
        throw new NotImplementedException();
    }

    @Override
    public String getStatus() {
        throw new NotImplementedException();
    }

    @Override
    public void setStatus(String msg) {
        throw new NotImplementedException();
    }

    public float getProgress() {
        throw new NotImplementedException();
    }

    @Override
    public Counter getCounter(Enum<?> counterName) {
        throw new NotImplementedException();
    }

    @Override
    public Counter getCounter(String groupName, String counterName) {
        throw new NotImplementedException();
    }

    @Override
    public Configuration getConfiguration() {
        return hconf;
    }

    @Override
    public Credentials getCredentials() {
        throw new NotImplementedException();
    }

    @Override
    public JobID getJobID() {
        throw new NotImplementedException();
    }

    @Override
    public int getNumReduceTasks() {
        throw new NotImplementedException();
    }

    @Override
    public Path getWorkingDirectory() throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public Class<?> getOutputKeyClass() {
        throw new NotImplementedException();
    }

    @Override
    public Class<?> getOutputValueClass() {
        throw new NotImplementedException();
    }

    @Override
    public Class<?> getMapOutputKeyClass() {
        throw new NotImplementedException();
    }

    @Override
    public Class<?> getMapOutputValueClass() {
        throw new NotImplementedException();
    }

    @Override
    public String getJobName() {
        throw new NotImplementedException();
    }

    @Override
    public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
        throw new NotImplementedException();
    }

    @Override
    public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
        throw new NotImplementedException();
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
        throw new NotImplementedException();
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
        throw new NotImplementedException();
    }

    @Override
    public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
        throw new NotImplementedException();
    }

    @Override
    public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
        throw new NotImplementedException();
    }

    @Override
    public RawComparator<?> getSortComparator() {
        throw new NotImplementedException();
    }

    @Override
    public String getJar() {
        throw new NotImplementedException();
    }

    @Override
    public RawComparator<?> getGroupingComparator() {
        throw new NotImplementedException();
    }

    @Override
    public boolean getJobSetupCleanupNeeded() {
        throw new NotImplementedException();
    }

    public boolean getTaskCleanupNeeded() {
        throw new NotImplementedException();
    }

    @Override
    public boolean getProfileEnabled() {
        throw new NotImplementedException();
    }

    @Override
    public String getProfileParams() {
        throw new NotImplementedException();
    }

    public IntegerRanges getProfileTaskRange(boolean isMap) {
        throw new NotImplementedException();
    }

    @Override
    public String getUser() {
        throw new NotImplementedException();
    }

    @Override
    public boolean getSymlink() {
        throw new NotImplementedException();
    }

    @Override
    public Path[] getArchiveClassPaths() {
        throw new NotImplementedException();
    }

    @Override
    public URI[] getCacheArchives() throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public URI[] getCacheFiles() throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public Path[] getLocalCacheArchives() throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public Path[] getLocalCacheFiles() throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public Path[] getFileClassPaths() {
        throw new NotImplementedException();
    }

    @Override
    public String[] getArchiveTimestamps() {
        throw new NotImplementedException();
    }

    @Override
    public String[] getFileTimestamps() {
        throw new NotImplementedException();
    }

    @Override
    public int getMaxMapAttempts() {
        throw new NotImplementedException();
    }

    @Override
    public int getMaxReduceAttempts() {
        throw new NotImplementedException();
    }

    @Override
    public void progress() {
        throw new NotImplementedException();
    }

    @Override
    public InputSplit getInputSplit() {
        throw new NotImplementedException();
    }

    @Override
    public RawComparator<?> getCombinerKeyGroupingComparator() {
        throw new NotImplementedException();
    }

    public boolean userClassesTakesPrecedence() {
        throw new NotImplementedException();
    }
}
