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

package org.apache.kylin.engine.mr;

import java.util.Collection;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kylin.metadata.model.TableDesc;

/**
 * Any ISource that wishes to serve as input of MapReduce build engine must adapt to this interface.
 */
public interface IMRInput extends IInput {

    /** Return an InputFormat that reads from specified table. */
    public IMRTableInputFormat getTableInputFormat(TableDesc table, String uuid);

    /**
     * Utility that configures mapper to read from a table.
     */
    public interface IMRTableInputFormat {

        /** Configure the InputFormat of given job. */
        public void configureJob(Job job);

        /** Parse a mapper input object into column values. */
        public Collection<String[]> parseMapperInput(Object mapperInput);

        /** Get the signature for the input split*/
        public String getInputSplitSignature(InputSplit inputSplit);
    }

    /**
     * Participate the batch cubing flow as the input side. Responsible for creating
     * intermediate flat table (Phase 1) and clean up any leftover (Phase 4).
     * 
     * - Phase 1: Create Flat Table
     * - Phase 2: Build Dictionary (with FlatTableInputFormat)
     * - Phase 3: Build Cube (with FlatTableInputFormat)
     * - Phase 4: Update Metadata & Cleanup
     */
    public interface IMRBatchCubingInputSide extends IBatchCubingInputSide {

        /** Return an InputFormat that reads from the intermediate flat table */
        public IMRTableInputFormat getFlatTableInputFormat();
    }

    public interface IMRBatchMergeInputSide extends IBatchMergeInputSide {

    }
}
