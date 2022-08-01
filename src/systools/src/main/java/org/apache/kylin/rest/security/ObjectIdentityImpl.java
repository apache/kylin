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

package org.apache.kylin.rest.security;

import java.io.Serializable;

import org.apache.kylin.common.persistence.AclEntity;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.util.Assert;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Mimic org.springframework.security.acls.domain.ObjectIdentityImpl
 * Make it Jackson friendly.
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class ObjectIdentityImpl implements ObjectIdentity {
    // ~ Instance fields
    // ================================================================================================

    @JsonProperty("type")
    private String type;
    @JsonProperty("id")
    private String identifier;

    // ~ Constructors
    // ===================================================================================================

    // for Jackson
    public ObjectIdentityImpl() {
    }

    public ObjectIdentityImpl(ObjectIdentity oid) {
        this(oid.getType(), String.valueOf(oid.getIdentifier()));
    }

    public ObjectIdentityImpl(String type, String identifier) {
        Assert.hasText(type, "Type required");
        Assert.notNull(identifier, "identifier required");

        this.identifier = identifier;
        this.type = type;
    }

    public ObjectIdentityImpl(AclEntity ae) {
        Assert.notNull(ae, "ACL entity required");
        this.type = ae.getClass().getName();
        this.identifier = ae.getId();
    }

    // ~ Methods
    // ========================================================================================================

    /**
     * Important so caching operates properly.
     * <p>
     * Considers an object of the same class equal if it has the same
     * <code>classname</code> and <code>id</code> properties.
     * <p>
     * Numeric identities (Integer and Long values) are considered equal if they are
     * numerically equal. Other serializable types are evaluated using a simple equality.
     *
     * @param arg0 object to compare
     *
     * @return <code>true</code> if the presented object matches this object
     */
    public boolean equals(Object arg0) {
        if (arg0 == null || !(arg0 instanceof ObjectIdentity)) {
            return false;
        }

        ObjectIdentity other = (ObjectIdentity) arg0;

        if (!identifier.equals(other.getIdentifier())) {
            return false;
        }

        return type.equals(other.getType());
    }

    public Serializable getIdentifier() {
        return identifier;
    }

    public String getId() {
        return identifier;
    }

    public String getType() {
        return type;
    }

    /**
     * Important so caching operates properly.
     *
     * @return the hash
     */
    public int hashCode() {
        int code = 31;
        code ^= this.type.hashCode();
        code ^= this.identifier.hashCode();

        return code;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getName()).append("[");
        sb.append("Type: ").append(this.type);
        sb.append("; Identifier: ").append(this.identifier).append("]");

        return sb.toString();
    }
}
