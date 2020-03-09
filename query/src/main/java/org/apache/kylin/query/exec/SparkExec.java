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

package org.apache.kylin.query.exec;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.kylin.common.QueryContext;
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
