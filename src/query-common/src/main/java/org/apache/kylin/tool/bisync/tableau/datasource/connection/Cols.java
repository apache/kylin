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
package org.apache.kylin.tool.bisync.tableau.datasource.connection;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public class Cols {

    @JacksonXmlProperty(localName = "map", isAttribute = true)
    @JacksonXmlElementWrapper(useWrapping = false)
    private List<Col> cols;

    public List<Col> getCols() {
        return cols;
    }

    public void setCols(List<Col> cols) {
        this.cols = cols;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Cols)) {
            return false;
        }
        Cols cols1 = (Cols) o;
        return colListEquals(cols1.getCols());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getCols());
    }

    private boolean colListEquals(List<Col> thatColList) {
        if (getCols() == thatColList) {
            return true;
        }
        if (getCols() != null && thatColList != null && getCols().size() == thatColList.size()) {
            Comparator<Col> colsComparator = (o1, o2) -> o1.getKey().compareTo(o2.getKey());
            Collections.sort(getCols(), colsComparator);
            Collections.sort(thatColList, colsComparator);

            boolean flag = true;
            for (int i = 0; i < getCols().size() && flag; i++) {
                flag = Objects.equals(getCols().get(i), thatColList.get(i));
            }
            return flag;
        }
        return false;
    }
}
