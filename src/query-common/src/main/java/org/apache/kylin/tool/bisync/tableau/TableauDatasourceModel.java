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

package org.apache.kylin.tool.bisync.tableau;

import java.io.IOException;
import java.io.OutputStream;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.kylin.common.exception.CommonErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.tool.bisync.BISyncModel;
import org.apache.kylin.tool.bisync.tableau.datasource.TableauDatasource;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;

public class TableauDatasourceModel implements BISyncModel {

    private final TableauDatasource tableauDatasource;

    public TableauDatasourceModel(TableauDatasource tableauDatasource) {
        this.tableauDatasource = tableauDatasource;
    }

    public static void dumpModelAsXML(TableauDatasource BISyncModel, OutputStream outputStream)
            throws XMLStreamException, IOException {
        XmlMapper xmlMapper = new XmlMapper();
        XMLStreamWriter writer = xmlMapper.getFactory().getXMLOutputFactory().createXMLStreamWriter(outputStream);
        xmlMapper.enable(ToXmlGenerator.Feature.WRITE_XML_DECLARATION);
        xmlMapper.enable(SerializationFeature.INDENT_OUTPUT);
        xmlMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        xmlMapper.getFactory().getXMLOutputFactory().setProperty("javax.xml.stream.isRepairingNamespaces", false);
        xmlMapper.writeValue(writer, BISyncModel);
    }

    @Override
    public void dump(OutputStream outputStream) {
        try {
            dumpModelAsXML(tableauDatasource, outputStream);
        } catch (XMLStreamException | IOException e) {
            throw new KylinException(CommonErrorCode.UNKNOWN_ERROR_CODE, e);
        }
    }
}
