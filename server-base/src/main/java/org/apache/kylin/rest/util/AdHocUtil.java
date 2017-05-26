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

package org.apache.kylin.rest.util;

import java.sql.SQLException;
import java.util.List;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.query.routing.NoRealizationFoundException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.storage.adhoc.AdHocRunnerBase;
import org.apache.kylin.storage.adhoc.IAdhocConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdHocUtil {
    private static final Logger logger = LoggerFactory.getLogger(AdHocUtil.class);

    public static boolean doAdHocQuery(String sql, List<List<String>> results, List<SelectedColumnMeta> columnMetas, SQLException sqlException) throws Exception {
        boolean isExpectedCause = (ExceptionUtils.getRootCause(sqlException).getClass().equals(NoRealizationFoundException.class));
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        Boolean isAdHoc = false;

        if (isExpectedCause && kylinConfig.isAdhocEnabled()) {
            Class runnerClass = Class.forName(kylinConfig.getAdHocRunnerClassName());
            Class converterClass = Class.forName(kylinConfig.getAdHocConverterClassName());
            Object runnerObj = runnerClass.newInstance();
            Object converterObj = converterClass.newInstance();

            if (!(runnerObj instanceof AdHocRunnerBase)) {
                throw new InternalErrorException("Ad-hoc runner class should be sub-class of AdHocRunnerBase");
            }

            if (!(converterObj instanceof IAdhocConverter)) {
                throw new InternalErrorException("Ad-hoc converter class should implement of IAdhocConverter");
            }

            AdHocRunnerBase runner = (AdHocRunnerBase) runnerObj;
            IAdhocConverter converter = (IAdhocConverter) converterObj;
            runner.setConfig(kylinConfig);

            logger.debug("Ad-hoc query enabled for Kylin");

            runner.init();

            try {
                String adhocSql = converter.convert(sql);
                if (!sql.equals(adhocSql)) {
                    logger.info("the original query is converted to {} before delegating to ", adhocSql);
                }

                runner.executeQuery(adhocSql, results, columnMetas);
                isAdHoc = true;
            } catch (Exception exception) {
                throw exception;
            }
        } else {
            throw sqlException;
        }

        return isAdHoc;
    }
}
