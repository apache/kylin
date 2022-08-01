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

package org.apache.kylin.common.util;

public enum ByteUnit {
    BYTE(1L), KiB(1024L), MiB((long) Math.pow(1024.0D, 2.0D)), GiB((long) Math.pow(1024.0D, 3.0D)), TiB(
            (long) Math.pow(1024.0D, 4.0D)), PiB((long) Math.pow(1024.0D, 5.0D));

    private final long multiplier;

    private ByteUnit(long multiplier) {
        this.multiplier = multiplier;
    }

    public long convertFrom(long d, ByteUnit u) {
        return u.convertTo(d, this);
    }

    public long convertTo(long d, ByteUnit u) {
        if (this.multiplier > u.multiplier) {
            long ratio = this.multiplier / u.multiplier;
            if (9223372036854775807L / ratio < d) {
                throw new IllegalArgumentException("Conversion of " + d + " exceeds Long.MAX_VALUE in " + this.name()
                        + ". Try a larger unit (e.g. MiB instead of KiB)");
            } else {
                return d * ratio;
            }
        } else {
            return d / (u.multiplier / this.multiplier);
        }
    }

    public double toBytes(long d) {
        if (d < 0L) {
            throw new IllegalArgumentException("Negative size value. Size must be positive: " + d);
        } else {
            return (double) (d * this.multiplier);
        }
    }

    public long toKiB(long d) {
        return this.convertTo(d, KiB);
    }

    public long toMiB(long d) {
        return this.convertTo(d, MiB);
    }

    public long toGiB(long d) {
        return this.convertTo(d, GiB);
    }

    public long toTiB(long d) {
        return this.convertTo(d, TiB);
    }

    public long toPiB(long d) {
        return this.convertTo(d, PiB);
    }
}
