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

import org.wso2.carbon.identity.session.store.redis.util.RedisValueUtils;

import java.util.Collections;
import java.util.List;

/**
 * What a deleted session held, which is everything the caller needs to clean up after it: the user and
 * the tenant whose indexes referred to it, and the federated mappings that belonged to it. All of it is
 * read in the call that deletes the session, since afterwards there is nothing left to read it from.
 */
public class SessionRemovalResult {

    private static final int USER_INDEX = 0;
    private static final int TENANT_INDEX = 1;
    private static final int FEDERATED_MEMBERS_INDEX = 2;

    private final String userId;
    private final Integer tenantId;
    private final List<String> federatedMembers;

    private SessionRemovalResult(String userId, Integer tenantId, List<String> federatedMembers) {

        this.userId = userId;
        this.tenantId = tenantId;
        this.federatedMembers = federatedMembers;
    }

    /**
     * Decodes the reply of the delete script.
     *
     * @param reply Reply of the script, as user, tenant and federated mappings.
     * @return the decoded reply, holding nothing when there is none.
     */
    public static SessionRemovalResult build(List<?> reply) {

        if (reply == null || reply.isEmpty()) {
            return new SessionRemovalResult(null, null, Collections.emptyList());
        }
        String userId = RedisValueUtils.toUserId(reply.get(USER_INDEX));
        Integer tenantId = reply.size() > TENANT_INDEX
                ? RedisValueUtils.toInteger(RedisValueUtils.toString(reply.get(TENANT_INDEX))) : null;
        List<String> members = reply.size() > FEDERATED_MEMBERS_INDEX
                ? RedisValueUtils.toMembers(reply.get(FEDERATED_MEMBERS_INDEX)) : Collections.emptyList();
        return new SessionRemovalResult(userId, tenantId, members);
    }

    /**
     * The user whose sessions held the deleted one.
     *
     * @return the user identifier, or {@code null} when the session held none.
     */
    public String getUserId() {

        return userId;
    }

    /**
     * The tenant whose sessions held the deleted one.
     *
     * @return the tenant identifier, or {@code null} when the entry held no session record.
     */
    public Integer getTenantId() {

        return tenantId;
    }

    /**
     * The federated mappings the deleted session held, as members of the form
     * {@code <tenantId>:<idpId>:<idpSessionId>}.
     *
     * @return the federated mapping members of the session.
     */
    public List<String> getFederatedMembers() {

        return Collections.unmodifiableList(federatedMembers);
    }
}
