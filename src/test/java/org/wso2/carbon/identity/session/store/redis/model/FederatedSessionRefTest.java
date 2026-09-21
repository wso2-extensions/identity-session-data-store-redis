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

import org.junit.jupiter.api.Test;
import org.wso2.carbon.identity.session.store.redis.util.RedisKeyUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests that both kinds of federated index member are parsed, as the two indexes hold members of
 * different shapes.
 */
public class FederatedSessionRefTest {

    @Test
    void testMemberOfTheSessionIndexIsParsed() {

        FederatedSessionRef reference = FederatedSessionRef.buildFromMember(
                RedisKeyUtils.getFederatedMember(1, 7, "idp-sid-1"));

        assertNotNull(reference);
        assertEquals(1, reference.getTenantId());
        assertEquals(7, reference.getIdpId());
        assertEquals("idp-sid-1", reference.getIdpSessionId());
    }

    @Test
    void testMemberOfTheIdpSessionIndexIsParsed() {

        // This member carries only the tenant and the identity provider, since the session index is
        // already part of the key it is stored under. Parsing it as a full mapping member yields nothing,
        // which used to make every federated session look absent.
        String member = RedisKeyUtils.getFederatedIdpMember(1, 7);

        assertNull(FederatedSessionRef.buildFromMember(member));

        FederatedSessionRef reference = FederatedSessionRef.buildFromIdpMember(member, "idp-sid-1");
        assertNotNull(reference);
        assertEquals(1, reference.getTenantId());
        assertEquals(7, reference.getIdpId());
        assertEquals("idp-sid-1", reference.getIdpSessionId());
    }

    @Test
    void testIdpMemberOfAnUnspecifiedTenantAndIdp() {

        FederatedSessionRef reference = FederatedSessionRef.buildFromIdpMember(
                RedisKeyUtils.getFederatedIdpMember(-1234, -1234), "idp-sid-1");

        assertNotNull(reference);
        assertEquals(-1234, reference.getTenantId());
        assertEquals(-1234, reference.getIdpId());
    }

    @Test
    void testMalformedMembersAreRejected() {

        assertNull(FederatedSessionRef.buildFromMember(null));
        assertNull(FederatedSessionRef.buildFromMember("1:7"));
        assertNull(FederatedSessionRef.buildFromMember("one:7:idp-sid-1"));
        assertNull(FederatedSessionRef.buildFromIdpMember(null, "idp-sid-1"));
        assertNull(FederatedSessionRef.buildFromIdpMember("1", "idp-sid-1"));
        assertNull(FederatedSessionRef.buildFromIdpMember("1:seven", "idp-sid-1"));
    }
}
