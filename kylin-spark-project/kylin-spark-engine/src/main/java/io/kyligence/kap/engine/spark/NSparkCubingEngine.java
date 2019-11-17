/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.engine.spark;

import java.util.Map;

import org.apache.kylin.common.util.ImplementationSwitch;
import org.apache.kylin.job.engine.NCubingEngine;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import io.kyligence.kap.common.obf.IKeep;

public class NSparkCubingEngine implements NCubingEngine, IKeep {

    private static ThreadLocal<ImplementationSwitch<NSparkCubingStorage>> storages = new ThreadLocal<>();

    @Override
    public Class<?> getSourceInterface() {
        return NSparkCubingSource.class;
    }

    @Override
    public Class<?> getStorageInterface() {
        return NSparkCubingStorage.class;
    }

    public interface NSparkCubingSource {
        /**
         * Get Dataset<Row>
         *
         * @param table, source table
         * @param ss
         * @return the Dataset<Row>, its schema consists of table column's name, for example, [column1,column2,column3]
         */
        Dataset<Row> getSourceData(TableDesc table, SparkSession ss, Map<String, String> parameters);
    }

    public interface NSparkCubingStorage {

        void saveTo(String path, Dataset<Row> data, SparkSession ss);

        Dataset<Row> getFrom(String path, SparkSession ss);
    }
}
