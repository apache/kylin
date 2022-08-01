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
package org.apache.kylin.tool.bisync.tableau.datasource.connection.relation;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public class Expression {

    @JacksonXmlProperty(localName = "op", isAttribute = true)
    private String op;

    @JacksonXmlProperty(localName = "expression")
    @JacksonXmlElementWrapper(useWrapping = false)
    private List<Expression> expressionList;

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public List<Expression> getExpressionList() {
        return expressionList;
    }

    public void setExpressionList(List<Expression> expressionList) {
        this.expressionList = expressionList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Expression)) {
            return false;
        }
        Expression that = (Expression) o;
        return Objects.equals(getOp(), that.getOp()) && expressionChildListEquals(that.getExpressionList());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getOp(), getExpressionList());
    }

    private boolean expressionChildListEquals(List<Expression> thatExpressionList) {
        if (getExpressionList() == thatExpressionList) {
            return true;
        }
        if (getExpressionList() != null && thatExpressionList != null
                && getExpressionList().size() == thatExpressionList.size()) {
            boolean flag = true;
            for (int i = 0; i < getExpressionList().size() && flag; i++) {
                flag = Objects.equals(getExpressionList().get(i), thatExpressionList.get(i));
            }
            return flag;
        }
        return false;
    }
}
