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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class KylinMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    private static final Logger logger = LoggerFactory.getLogger(KylinMapper.class);

    protected int mapCounter = 0;

    protected void bindCurrentConfiguration(Configuration conf) {
        logger.info("The conf for current mapper will be " + System.identityHashCode(conf));
        HadoopUtil.setCurrentConfiguration(conf);
    }

    @Override
    final public void map(KEYIN key, VALUEIN value, Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context) throws IOException, InterruptedException {
        try {
            if (mapCounter++ % BatchConstants.NORMAL_RECORD_LOG_THRESHOLD == 0) {
                logger.info("Accepting Mapper Key with ordinal: " + mapCounter);
            }
            doMap(key, value, context);
        } catch (IOException ex) { // KYLIN-2170
            logger.error("", ex);
            throw ex;
        } catch (InterruptedException ex) { // KYLIN-2170
            logger.error("", ex);
            throw ex;
        } catch (RuntimeException ex) { // KYLIN-2170
            logger.error("", ex);
            throw ex;
        } catch (Error ex) { // KYLIN-2170
            logger.error("", ex);
            throw ex;
        }
    }

    protected void doMap(KEYIN key, VALUEIN value, Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }

    @Override
    final protected void cleanup(Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context) throws IOException, InterruptedException {
        try {
            doCleanup(context);
        } catch (IOException ex) { // KYLIN-2170
            logger.error("", ex);
            throw ex;
        } catch (InterruptedException ex) { // KYLIN-2170
            logger.error("", ex);
            throw ex;
        } catch (RuntimeException ex) { // KYLIN-2170
            logger.error("", ex);
            throw ex;
        } catch (Error ex) { // KYLIN-2170
            logger.error("", ex);
            throw ex;
        }
    }

    protected void doCleanup(Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context) throws IOException, InterruptedException {
    }
}
