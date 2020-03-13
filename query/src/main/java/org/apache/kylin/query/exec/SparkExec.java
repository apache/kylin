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

package org.apache.kylin.query.exec;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.kylin.common.QueryContextFacade;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.query.relnode.OLAPRel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkExec {

    private static final Logger logger = LoggerFactory.getLogger(SparkExec.class);

    public static Enumerable<Object[]> collectToEnumerable(DataContext dataContext) {
        if (BackdoorToggles.getPrepareOnly()) {
            return Linq4j.emptyEnumerable();
        }

        OLAPRel olapRel = (OLAPRel) QueryContextFacade.current().getOlapRel();
        RelDataType rowType = (RelDataType) QueryContextFacade.current().getResultType();
        try {
            Enumerable<Object[]> computer = QueryEngineFactory.compute(dataContext, olapRel, rowType);
            return computer;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Enumerable<Object> collectToScalarEnumerable(DataContext dataContext) {
        if (BackdoorToggles.getPrepareOnly()) {
            return Linq4j.emptyEnumerable();
        }

        OLAPRel olapRel = (OLAPRel) QueryContextFacade.current().getOlapRel();
        RelDataType rowType = (RelDataType) QueryContextFacade.current().getResultType();
        try {
            Enumerable<Object> objects = QueryEngineFactory.computeSCALA(dataContext, olapRel, rowType);
            return objects;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Enumerable<Object[]> asyncResult(DataContext dataContext) {
        if (BackdoorToggles.getPrepareOnly()) {
            return Linq4j.emptyEnumerable();
        }
        OLAPRel olapRel = (OLAPRel) QueryContextFacade.current().getOlapRel();
        RelDataType rowType = (RelDataType) QueryContextFacade.current().getResultType();
        return QueryEngineFactory.computeAsync(dataContext, olapRel, rowType);
    }

}
