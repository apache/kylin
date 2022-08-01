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
package org.apache.kylin.tool.bisync.model;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import org.apache.kylin.metadata.model.JoinTableDesc;

public class JoinTreeNode {

    private JoinTableDesc value;

    private List<JoinTreeNode> childNodes;

    public JoinTableDesc getValue() {
        return value;
    }

    public void setValue(JoinTableDesc value) {
        this.value = value;
    }

    public List<JoinTreeNode> getChildNodes() {
        return childNodes;
    }

    public void setChildNodes(List<JoinTreeNode> childNodes) {
        this.childNodes = childNodes;
    }

    /**
     * serialize a tree node to list by level-first
     */
    public List<JoinTableDesc> iteratorAsList() {
        if (this.value == null) {
            return null;
        }

        Deque<JoinTreeNode> nodeDeque = new LinkedList<>();
        List<JoinTableDesc> elements = new LinkedList<>();
        nodeDeque.push(this);
        breadthSerialize(nodeDeque, elements);
        return elements;

    }

    private void breadthSerialize(Deque<JoinTreeNode> nodeDeque, List<JoinTableDesc> elements) {
        if (nodeDeque.isEmpty()) {
            return;
        }

        JoinTreeNode node = nodeDeque.removeFirst();
        elements.add(node.getValue());
        if (node.getChildNodes() != null) {
            for (JoinTreeNode childNode : node.getChildNodes()) {
                nodeDeque.addLast(childNode);
            }
        }
        breadthSerialize(nodeDeque, elements);

    }
}
