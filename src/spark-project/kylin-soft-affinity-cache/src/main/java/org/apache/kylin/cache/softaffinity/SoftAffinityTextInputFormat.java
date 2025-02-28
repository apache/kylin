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
package org.apache.kylin.cache.softaffinity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.softaffinity.SoftAffinityManager;

/**
 * A TextInputFormat that reports preferred location based on soft affinity
 */
public class SoftAffinityTextInputFormat extends org.apache.hadoop.mapred.TextInputFormat {

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        FileSplit[] splits = (FileSplit[]) super.getSplits(job, numSplits);

        // enable soft affinity, for possible leverage of local cache
        return SoftAffinityManager.usingSoftAffinity()
                ? Arrays.stream(splits).map(FileSplitWithCachedLocation::new).toArray(InputSplit[]::new)
                : splits;
    }

    public static class FileSplitWithCachedLocation extends FileSplit {
        org.apache.hadoop.mapreduce.lib.input.FileSplit selfFS;
        String[] locations;

        @SuppressWarnings("unused") // no-arg constructor for Writable serialization
        public FileSplitWithCachedLocation() {
            selfFS = new org.apache.hadoop.mapreduce.lib.input.FileSplit();
            locations = new String[0];
        }

        public FileSplitWithCachedLocation(FileSplit s) {
            try {
                Field f = FieldUtils.getDeclaredField(FileSplit.class, "fs", true);
                this.selfFS = (org.apache.hadoop.mapreduce.lib.input.FileSplit) f.get(s);
            } catch (Exception e) {
                throw new KylinRuntimeException(e);
            }

            Path localityBase = s.getPath().getParent();
            this.locations = SoftAffinityManager.askExecutors(localityBase.toString());
        }

        @Override
        public String[] getLocations() {
            return locations;
        }

        @Override
        public SplitLocationInfo[] getLocationInfo() {
            return Arrays.stream(locations).map(l -> new SplitLocationInfo(l, false)).toArray(SplitLocationInfo[]::new);
        }

        @Override
        public Path getPath() {
            return selfFS.getPath();
        }

        @Override
        public long getStart() {
            return selfFS.getStart();
        }

        @Override
        public long getLength() {
            return selfFS.getLength();
        }

        @Override
        public String toString() {
            return selfFS.toString();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            selfFS.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            selfFS.readFields(in);
        }
    }
}
