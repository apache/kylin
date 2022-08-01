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

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.apache.kylin.tool.bisync.tableau.datasource.column.Column;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

@JacksonXmlRootElement(localName = "datasource")
public class TableauDatasource implements Serializable {

    @JacksonXmlProperty(localName = "formatted-name", isAttribute = true)
    private String formattedName;

    @JacksonXmlProperty(localName = "inline", isAttribute = true)
    private String inline;

    @JacksonXmlProperty(localName = "source-platform", isAttribute = true)
    private String sourcePlatform;

    @JacksonXmlProperty(localName = "version", isAttribute = true)
    private String version;

    @JacksonXmlProperty(localName = "connection")
    private TableauConnection tableauConnection;

    @JacksonXmlProperty(localName = "aliases")
    private Aliases aliases;

    @JacksonXmlProperty(localName = "column")
    @JacksonXmlElementWrapper(useWrapping = false)
    private List<Column> columns;

    @JacksonXmlProperty(localName = "drill-paths")
    private DrillPaths drillPaths;
    @JacksonXmlProperty(localName = "layout")
    private Layout layout;
    @JacksonXmlProperty(localName = "semantic-values")
    private SemanticValueList semanticValues;

    public String getFormattedName() {
        return formattedName;
    }

    public void setFormattedName(String formattedName) {
        this.formattedName = formattedName;
    }

    public String getInline() {
        return inline;
    }

    public void setInline(String inline) {
        this.inline = inline;
    }

    public String getSourcePlatform() {
        return sourcePlatform;
    }

    public void setSourcePlatform(String sourcePlatform) {
        this.sourcePlatform = sourcePlatform;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public TableauConnection getTableauConnection() {
        return tableauConnection;
    }

    public void setTableauConnection(TableauConnection tableauConnection) {
        this.tableauConnection = tableauConnection;
    }

    public Aliases getAliases() {
        return aliases;
    }

    public void setAliases(Aliases aliases) {
        this.aliases = aliases;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public Layout getLayout() {
        return layout;
    }

    public void setLayout(Layout layout) {
        this.layout = layout;
    }

    public SemanticValueList getSemanticValues() {
        return semanticValues;
    }

    public void setSemanticValues(SemanticValueList semanticValues) {
        this.semanticValues = semanticValues;
    }

    public DrillPaths getDrillPaths() {
        return drillPaths;
    }

    public void setDrillPaths(DrillPaths drillPaths) {
        this.drillPaths = drillPaths;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TableauDatasource)) {
            return false;
        }
        TableauDatasource that = (TableauDatasource) o;
        return Objects.equals(getTableauConnection(), that.getTableauConnection())
                && columnListEquals(that.getColumns()) && Objects.equals(getDrillPaths(), that.getDrillPaths());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTableauConnection(), getColumns(), getDrillPaths());
    }

    private boolean columnListEquals(List<Column> thatColumnList) {
        if (getColumns() == thatColumnList) {
            return true;
        }
        if (getColumns() != null && thatColumnList != null && getColumns().size() == thatColumnList.size()) {
            Comparator<Column> columnComparator = (o1, o2) -> o1.getName().compareTo(o2.getName());
            Collections.sort(getColumns(), columnComparator);
            Collections.sort(thatColumnList, columnComparator);

            boolean flag = true;
            for (int i = 0; i < getColumns().size() && flag; i++) {
                flag = Objects.equals(getColumns().get(i), thatColumnList.get(i));
            }
            return flag;
        }
        return false;
    }

}
