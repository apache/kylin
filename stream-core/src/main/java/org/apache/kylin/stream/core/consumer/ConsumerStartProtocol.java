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

package org.apache.kylin.stream.core.consumer;

public class ConsumerStartProtocol {
    private ConsumerStartMode startMode;
    private String startPosition;
    private String endPosition;

    public ConsumerStartProtocol(ConsumerStartMode startMode) {
        this.startMode = startMode;
    }

    public ConsumerStartProtocol(String startPosition) {
        this.startMode = ConsumerStartMode.SPECIFIC_POSITION;
        this.startPosition = startPosition;
    }

    public ConsumerStartProtocol() {
    }

    public ConsumerStartMode getStartMode() {
        return startMode;
    }

    public void setStartMode(ConsumerStartMode startMode) {
        this.startMode = startMode;
    }

    public String getStartPosition() {
        return startPosition;
    }

    public void setStartPosition(String startPosition) {
        this.startPosition = startPosition;
    }

    public String getEndPosition() {
        return endPosition;
    }

    public void setEndPosition(String endPosition) {
        this.endPosition = endPosition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConsumerStartProtocol that = (ConsumerStartProtocol) o;

        if (startMode != that.startMode) return false;
        if (startPosition != null ? !startPosition.equals(that.startPosition) : that.startPosition != null)
            return false;
        return endPosition != null ? endPosition.equals(that.endPosition) : that.endPosition == null;

    }

    @Override
    public int hashCode() {
        int result = startMode != null ? startMode.hashCode() : 0;
        result = 31 * result + (startPosition != null ? startPosition.hashCode() : 0);
        result = 31 * result + (endPosition != null ? endPosition.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ConsumerStartProtocol{" +
                "startMode=" + startMode +
                ", startPosition='" + startPosition + '\'' +
                ", endPosition='" + endPosition + '\'' +
                '}';
    }
}
