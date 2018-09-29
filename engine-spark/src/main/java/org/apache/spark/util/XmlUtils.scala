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

package org.apache.spark.util

import java.io.{BufferedInputStream, File, FileInputStream, InputStream}
import java.util.Properties

import javax.xml.parsers.{DocumentBuilder, DocumentBuilderFactory}
import org.apache.hadoop.util.StringInterner
import org.slf4j.LoggerFactory
import org.w3c.dom.{Document, Element, Text}

import scala.collection.immutable.Range

object XmlUtils {

  private val logger = LoggerFactory.getLogger(XmlUtils.getClass)

  def loadProp(path: String): Properties = {
    val docBuilderFactory = DocumentBuilderFactory.newInstance
    docBuilderFactory.setIgnoringComments(true)

    //  allow includes in the xml file
    docBuilderFactory.setNamespaceAware(true)
    try docBuilderFactory.setXIncludeAware(true)
    catch {
      case e: UnsupportedOperationException =>
    }
    val builder = docBuilderFactory.newDocumentBuilder
    var doc: Document = null
    var root: Element = null
    val properties = new Properties()
    val file = new File(path).getAbsoluteFile
    if (file.exists) {
      doc = parse(builder, new BufferedInputStream(new FileInputStream(file)), path)
    } else {
      throw new RuntimeException("File not found in path: " + path)
    }
    root = doc.getDocumentElement
    if (!("configuration" == root.getTagName)) {
      logger.error("bad conf file: top-level element not <configuration>")
    }

    val props = root.getChildNodes
    for (i <- Range(0, props.getLength)) {
      val propNode = props.item(i)

      propNode match {
        case prop: Element =>
          if ("configuration".equals(prop.getTagName())) {}
          if (!"property".equals(prop.getTagName())) {
            logger.warn("bad conf file: element not <property>")
          }

          val fields = prop.getChildNodes
          var attr: String = null
          var value: String = null
          var finalParameter: Boolean = false
          val source: java.util.LinkedList[String] =
            new java.util.LinkedList[String]
          for (j <- Range(0, fields.getLength)) {
            val fieldNode = fields.item(j)
            fieldNode match {
              case field: Element =>
                if ("name" == field.getTagName && field.hasChildNodes) {
                  attr =
                    StringInterner.weakIntern(field.getFirstChild.asInstanceOf[Text].getData.trim)
                }
                if ("value" == field.getTagName && field.hasChildNodes) {
                  value = StringInterner.weakIntern(field.getFirstChild.asInstanceOf[Text].getData)
                }
              case _ =>
            }
            if (attr != null && value != null) {
              properties.setProperty(attr, value)
            }
          }
        case _ =>
      }

    }
    properties
  }

  def parse(builder: DocumentBuilder, is: InputStream, systemId: String): Document = {
    if (is == null) {
      return null
    }
    try {
      builder.parse(is, systemId)
    } finally {
      is.close()
    }
  }
}
