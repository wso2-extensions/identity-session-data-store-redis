/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.identity.session.store.redis.model;

import org.wso2.carbon.identity.session.store.redis.util.RedisConstants;
import org.wso2.carbon.identity.session.store.redis.util.RedisValueUtils;

/**
 * Reference to a federated authentication session mapping, held as a member of an index of them.
 */
public class FederatedSessionRef {

    private static final int MEMBER_COMPONENTS = 3;
    private static final int IDP_MEMBER_COMPONENTS = 2;

    private final int tenantId;
    private final int idpId;
    private final String idpSessionId;

    /**
     * Creates a reference to a federated authentication session mapping.
     *
     * @param tenantId     Tenant identifier.
     * @param idpId        Identity provider identifier.
     * @param idpSessionId Identity provider session index.
     */
    public FederatedSessionRef(int tenantId, int idpId, String idpSessionId) {

        this.tenantId = tenantId;
        this.idpId = idpId;
        this.idpSessionId = idpSessionId;
    }

    /**
     * Builds a reference from a member of the session to mapping index, which carries the tenant, the
     * identity provider and the session index of the mapping.
     *
     * @param member Index member, of the form {@code tenantId:idpId:idpSessionId}.
     * @return the reference, or {@code null} when the member is malformed.
     */
    public static FederatedSessionRef buildFromMember(String member) {

        if (member == null) {
            return null;
        }
        String[] components = member.split(RedisConstants.KEY_SEPARATOR, MEMBER_COMPONENTS);
        if (components.length < MEMBER_COMPONENTS) {
            return null;
        }
        return build(components[0], components[1], components[2]);
    }

    /**
     * Builds a reference from a member of the identity provider session index, which carries only the
     * tenant and the identity provider. The session index is the one the member was found under, since
     * the member itself does not carry it.
     *
     * @param member       Index member, of the form {@code tenantId:idpId}.
     * @param idpSessionId Identity provider session index the member was found under.
     * @return the reference, or {@code null} when the member is malformed.
     */
    public static FederatedSessionRef buildFromIdpMember(String member, String idpSessionId) {

        if (member == null) {
            return null;
        }
        String[] components = member.split(RedisConstants.KEY_SEPARATOR, IDP_MEMBER_COMPONENTS);
        if (components.length < IDP_MEMBER_COMPONENTS) {
            return null;
        }
        return build(components[0], components[1], idpSessionId);
    }

    private static FederatedSessionRef build(String tenant, String idp, String idpSessionId) {

        Integer tenantId = RedisValueUtils.toInteger(tenant);
        Integer idpId = RedisValueUtils.toInteger(idp);
        if (tenantId == null || idpId == null) {
            return null;
        }
        return new FederatedSessionRef(tenantId, idpId, idpSessionId);
    }

    /**
     * The tenant of the mapping.
     *
     * @return the tenant identifier.
     */
    public int getTenantId() {

        return tenantId;
    }

    /**
     * The identity provider of the mapping.
     *
     * @return the identity provider identifier.
     */
    public int getIdpId() {

        return idpId;
    }

    /**
     * The identity provider session index of the mapping.
     *
     * @return the session index.
     */
    public String getIdpSessionId() {

        return idpSessionId;
    }
}
