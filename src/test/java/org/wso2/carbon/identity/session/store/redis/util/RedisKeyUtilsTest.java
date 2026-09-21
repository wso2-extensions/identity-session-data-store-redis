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

package org.wso2.carbon.identity.session.store.redis.util;

import org.junit.jupiter.api.Test;
import org.wso2.carbon.identity.session.store.redis.model.FederatedSessionRef;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the key scheme, which is the storage format of the session store.
 */
public class RedisKeyUtilsTest {

    private static final String PREFIX = "idn";
    private static final String SESSION_ID = "9f2ce41";
    private static final String TYPE = RedisConstants.TYPE_SESSION_CONTEXT_CACHE;

    private final RedisKeyUtils keyUtils = new RedisKeyUtils(PREFIX);

    @Test
    void testSessionKey() {

        assertEquals("idn:s:" + TYPE + ":" + SESSION_ID, keyUtils.getSessionKey(TYPE, SESSION_ID));
    }

    @Test
    void testSessionKeyIncludesType() {

        // The same identifier is stored under more than one type, so the type must be part of the key.
        assertNotEquals(keyUtils.getSessionKey("OAuthSessionDataCache", SESSION_ID),
                keyUtils.getSessionKey("AuthenticationContextCache", SESSION_ID));
    }

    @Test
    void testKeysHaveNoClusterHashTag() {

        assertTrue(keyUtils.getSessionKey(TYPE, SESSION_ID).indexOf('{') < 0);
        assertTrue(keyUtils.getUserKey("a1b2").indexOf('{') < 0);
        assertTrue(keyUtils.getTenantKey(1).indexOf('{') < 0);
    }

    @Test
    void testIndexKeys() {

        assertEquals("idn:u:a1b2-c3d4", keyUtils.getUserKey("a1b2-c3d4"));
        assertEquals("idn:tid:1", keyUtils.getTenantKey(1));
        assertEquals("idn:tid:-1", keyUtils.getTenantKey(-1));
        assertEquals("idn:fed:idp:idp-sid-1", keyUtils.getFederatedByIdpSessionKey("idp-sid-1"));
    }

    @Test
    void testFederatedFieldRecordsTheMappingOnTheSession() {

        // The session carries its federated mappings as fields, so they expire with it and no per session
        // index is needed. The member is read back with the same separator it was written with.
        assertEquals("f:3:7:idp-sid-1", RedisKeyUtils.getPrefixedFederatedField(3, 7, "idp-sid-1"));
        assertEquals("3:7:idp-sid-1", RedisKeyUtils.stripFederatedFieldPrefix("f:3:7:idp-sid-1"));
        assertNull(RedisKeyUtils.stripFederatedFieldPrefix(RedisKeyUtils.getMetadataField("IP")));
        assertNull(RedisKeyUtils.stripFederatedFieldPrefix(null));
    }

    @Test
    void testFederatedKeyIncludesTenantAndIdp() {

        assertEquals("idn:fed:1:7:idp-sid-1", keyUtils.getFederatedKey(1, 7, "idp-sid-1"));
        assertNotEquals(keyUtils.getFederatedKey(1, 7, "idp-sid-1"),
                keyUtils.getFederatedKey(2, 7, "idp-sid-1"));
        assertNotEquals(keyUtils.getFederatedKey(1, 7, "idp-sid-1"),
                keyUtils.getFederatedKey(1, 8, "idp-sid-1"));
    }

    @Test
    void testFederatedMemberKeepsSessionIndexWhole() {

        String member = RedisKeyUtils.getFederatedMember(1, 7, "a:b:c");
        assertEquals("1:7:a:b:c", member);

        FederatedSessionRef reference = FederatedSessionRef.buildFromMember(member);
        assertEquals(1, reference.getTenantId());
        assertEquals(7, reference.getIdpId());
        assertEquals("a:b:c", reference.getIdpSessionId());
    }

    @Test
    void testMetadataFieldKeepsPropertyTypeVerbatim() {

        assertEquals("m:Last Access Time", RedisKeyUtils.getMetadataField("Last Access Time"));
        assertEquals("m:X-Custom-Claim", RedisKeyUtils.getMetadataField("X-Custom-Claim"));
    }

    @Test
    void testAppField() {

        assertEquals("a:3" + RedisConstants.FIELD_VALUE_SEPARATOR + "oauth2"
                        + RedisConstants.FIELD_VALUE_SEPARATOR + "alice",
                RedisKeyUtils.getAppField(3, "oauth2", "alice"));
    }

    @Test
    void testAppFieldDistinguishesInboundProtocol() {

        // Two application records differing only in the protocol are two records, so they must not be
        // stored as one field.
        assertNotEquals(RedisKeyUtils.getAppField(3, "oauth2", "alice"),
                RedisKeyUtils.getAppField(3, "samlsso", "alice"));
    }

    @Test
    void testAppFieldKeepsSubjectWhole() {

        String subject = "carbon.super:alice" + RedisConstants.FIELD_VALUE_SEPARATOR + "extra";
        String field = RedisKeyUtils.getAppField(3, "oauth2", subject);
        String[] components = field.substring(RedisConstants.FIELD_PREFIX_APP.length())
                .split(RedisConstants.FIELD_VALUE_SEPARATOR, RedisConstants.APP_FIELD_COMPONENTS);

        assertEquals("3", components[0]);
        assertEquals("oauth2", components[1]);
        assertEquals(subject, components[2]);
    }
}
