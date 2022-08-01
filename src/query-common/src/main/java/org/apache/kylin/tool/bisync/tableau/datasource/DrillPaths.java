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
package org.apache.kylin.tool.bisync.tableau.datasource;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public class DrillPaths {

    @JacksonXmlProperty(localName = "drill-path")
    @JacksonXmlElementWrapper(useWrapping = false)
    private List<DrillPath> drillPathList;

    public List<DrillPath> getDrillPathList() {
        return drillPathList;
    }

    public void setDrillPathList(List<DrillPath> drillPathList) {
        this.drillPathList = drillPathList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof DrillPaths))
            return false;
        DrillPaths that = (DrillPaths) o;
        return drillPathListEquals(that.getDrillPathList());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getDrillPathList());
    }

    private boolean drillPathListEquals(List<DrillPath> thatDrillPathList) {
        if (getDrillPathList() == thatDrillPathList) {
            return true;
        }
        if (getDrillPathList() != null && thatDrillPathList != null
                && getDrillPathList().size() == thatDrillPathList.size()) {
            Comparator<DrillPath> drillPathComparator = (o1, o2) -> o1.getName().compareTo(o2.getName());
            Collections.sort(getDrillPathList(), drillPathComparator);
            Collections.sort(thatDrillPathList, drillPathComparator);

            boolean flag = true;
            for (int i = 0; i < getDrillPathList().size() && flag; i++) {
                flag = Objects.equals(getDrillPathList().get(i), thatDrillPathList.get(i));
            }
            return flag;
        }
        return false;
    }
}
