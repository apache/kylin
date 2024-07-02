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

package org.apache.kylin.engine.spark.builder;

import java.util.Set;

public interface TreeNode<T extends TreeNode> {
    T getRootNode();

    Set<T> getParents(boolean recursive);

    Set<T> getChildren(boolean recursive);

    int getDepth();

    void addAsChild(T childNode, boolean recursive);

    void removeChild(T childNode, boolean recursive);

    boolean isChildOf(T treeNode);

    boolean isParentOf(T treeNode);

    String getSimpleString();

    String getDetailString();

    default void getTreePrint(int depth, StringBuilder builder, String prefix, boolean simple, int indent) {
        int countIndent = 0;
        while (depth * indent > countIndent) {
            builder.append(' ');
            countIndent++;
        }
        if (prefix != null) {
            builder.append(prefix);
        }
        if (simple) {
            builder.append(this.getSimpleString());
        } else {
            builder.append(this.getDetailString());
        }
        builder.append('\n');
        if (getChildren(false) != null)
            getChildren(false).stream()
                    .forEach(child -> child.getTreePrint(depth + 1, builder, prefix, simple, indent));
    }
}
