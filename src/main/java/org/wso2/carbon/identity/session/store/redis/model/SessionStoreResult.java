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

import java.util.Collections;
import java.util.List;

/**
 * The reply of a session store: what it did, and what the session held before it. The store reads the
 * user and the federated mappings of the session in the same call, so refreshing everything the session
 * is referred to by costs no read of its own.
 */
public class SessionStoreResult {

    /**
     * What the store did to the session. Only whether it was applied is reported, through
     * {@link #isStored()}.
     */
    private enum Status {

        /** A newer store had already been applied, so this one was not. */
        STALE,

        /** The session was not there, so it was written for the first time. */
        CREATED,

        /** The session was already there and was replaced with a later expiry. */
        EXTENDED
    }

    private static final int STATUS_INDEX = 0;
    private static final int USER_INDEX = 1;
    private static final int FEDERATED_MEMBERS_INDEX = 2;

    private final Status status;
    private final String userId;
    private final List<String> federatedMembers;

    private SessionStoreResult(Status status, String userId, List<String> federatedMembers) {

        this.status = status;
        this.userId = userId;
        this.federatedMembers = federatedMembers;
    }

    /**
     * Decodes the reply of the store script.
     *
     * @param reply Reply of the script, as status, user and federated mappings.
     * @return the decoded reply, reporting {@link Status#STALE} when there is none.
     */
    public static SessionStoreResult build(List<?> reply) {

        if (reply == null || reply.isEmpty()) {
            return new SessionStoreResult(Status.STALE, null, Collections.emptyList());
        }
        Status status = toStatus(RedisValueUtils.toInteger(
                RedisValueUtils.toString(reply.get(STATUS_INDEX))));
        String userId = reply.size() > USER_INDEX ? RedisValueUtils.toUserId(reply.get(USER_INDEX)) : null;
        List<String> members = reply.size() > FEDERATED_MEMBERS_INDEX
                ? RedisValueUtils.toMembers(reply.get(FEDERATED_MEMBERS_INDEX)) : Collections.emptyList();
        return new SessionStoreResult(status, userId, members);
    }

    /**
     * The status a code reports. A code this version does not know is read as a rejection, since taking
     * one for a store would update the indexes for a session that may not be there.
     */
    private static Status toStatus(Integer code) {

        if (code != null) {
            if (code == RedisConstants.STORE_STATUS_CREATED) {
                return Status.CREATED;
            }
            if (code == RedisConstants.STORE_STATUS_EXTENDED) {
                return Status.EXTENDED;
            }
        }
        return Status.STALE;
    }

    /**
     * Whether the session was written, so that what refers to it can be updated.
     *
     * @return true when the store was applied.
     */
    public boolean isStored() {

        return status != Status.STALE;
    }

    /**
     * The user the session holds, which a session holds only one of.
     *
     * @return the user identifier, or {@code null} when the session holds none.
     */
    public String getUserId() {

        return userId;
    }

    /**
     * The federated mappings the session holds, as members of the form
     * {@code <tenantId>:<idpId>:<idpSessionId>}. Empty for a session stored for the first time, which
     * cannot yet have any.
     *
     * @return the federated mapping members of the session.
     */
    public List<String> getFederatedMembers() {

        return Collections.unmodifiableList(federatedMembers);
    }
}
