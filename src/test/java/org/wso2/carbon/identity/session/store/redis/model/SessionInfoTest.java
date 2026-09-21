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
import org.wso2.carbon.identity.application.authentication.framework.util.SessionMgtConstants;
import org.wso2.carbon.identity.session.store.redis.util.RedisConstants;
import org.wso2.carbon.identity.session.store.redis.util.RedisKeyUtils;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the decoding of a session hash read from Redis, covering the fields a malformed or partially
 * written hash leaves absent.
 */
public class SessionInfoTest {

    private static final String SESSION_ID = "sid-1";
    private static final long TIME_CREATED = 1754899200123456789L;

    @Test
    void testHashIsDecoded() {

        Map<String, byte[]> hash = hash();
        hash.put(RedisConstants.FIELD_TENANT_ID, toBytes("3"));
        hash.put(RedisConstants.FIELD_USER_ID, toBytes("user-1"));
        hash.put(RedisKeyUtils.getMetadataField(SessionMgtConstants.IP_ADDRESS), toBytes("10.0.0.5"));
        hash.put(RedisKeyUtils.getAppField(3, "oidc", "alice"), toBytes(""));

        SessionInfo session = new SessionInfo(SESSION_ID, hash);

        assertTrue(session.isStored());
        assertEquals(SESSION_ID, session.getSessionId());
        assertEquals(3, session.getTenantId());
        assertEquals("user-1", session.getUserId());
        assertEquals("10.0.0.5", session.getMetadata().get(SessionMgtConstants.IP_ADDRESS));
        assertEquals(1, session.getApplications().size());
        assertEquals("3", session.getApplications().get(0).getAppId());
        assertEquals("alice", session.getApplications().get(0).getSubject());
    }

    @Test
    void testHashWithoutTheCreationTimeIsNotStored() {

        // Fields can be written before the session itself, which is not a session until it has a time.
        Map<String, byte[]> hash = new HashMap<>();
        hash.put(RedisConstants.FIELD_USER_ID, toBytes("user-1"));

        SessionInfo session = new SessionInfo(SESSION_ID, hash);

        assertFalse(session.isStored());
    }

    @Test
    void testEmptyHashIsNotStored() {

        SessionInfo session = new SessionInfo(SESSION_ID, new HashMap<>());

        assertFalse(session.isStored());
        assertNull(session.getTenantId());
        assertNull(session.getUserId());
        assertTrue(session.getMetadata().isEmpty());
        assertTrue(session.getApplications().isEmpty());
    }

    @Test
    void testAbsentTenantIsNull() {

        assertNull(new SessionInfo(SESSION_ID, hash()).getTenantId());
    }

    @Test
    void testNonNumericTenantIsNull() {

        Map<String, byte[]> hash = hash();
        hash.put(RedisConstants.FIELD_TENANT_ID, toBytes("carbon.super"));

        assertNull(new SessionInfo(SESSION_ID, hash).getTenantId());
    }

    @Test
    void testAHashWithAnUnreadableCreationTimeIsStillASession() {

        Map<String, byte[]> hash = new HashMap<>();
        hash.put(RedisConstants.FIELD_TIME_CREATED, toBytes("not-a-time"));

        SessionInfo session = new SessionInfo(SESSION_ID, hash);

        // The field is present, so the entry is a session, whether or not its time decodes.
        assertTrue(session.isStored());
    }

    @Test
    void testAnAbsentUserIdIsNull() {

        assertNull(new SessionInfo(SESSION_ID, hash()).getUserId());
    }

    @Test
    void testAnEmptyUserIdValueIsNull() {

        Map<String, byte[]> hash = hash();
        hash.put(RedisConstants.FIELD_USER_ID, toBytes(""));

        assertNull(new SessionInfo(SESSION_ID, hash).getUserId());
    }

    @Test
    void testMetadataIsKeyedByPropertyWithoutItsPrefix() {

        Map<String, byte[]> hash = hash();
        hash.put(RedisKeyUtils.getMetadataField(SessionMgtConstants.USER_AGENT), toBytes("Mozilla/5.0"));
        hash.put(RedisKeyUtils.getMetadataField(SessionMgtConstants.LOGIN_TIME), toBytes("1000"));
        hash.put(RedisKeyUtils.getMetadataField(SessionMgtConstants.LAST_ACCESS_TIME), toBytes("2000"));

        SessionInfo session = new SessionInfo(SESSION_ID, hash);

        assertEquals("Mozilla/5.0", session.getMetadata().get(SessionMgtConstants.USER_AGENT));
        assertEquals("1000", session.getMetadata().get(SessionMgtConstants.LOGIN_TIME));
        assertEquals("2000", session.getMetadata().get(SessionMgtConstants.LAST_ACCESS_TIME));
    }

    @Test
    void testMalformedApplicationFieldIsSkipped() {

        Map<String, byte[]> hash = hash();
        // An application field needs three components; one with fewer is not an application record.
        hash.put(RedisConstants.FIELD_PREFIX_APP + join("3", "oidc"), toBytes(""));
        hash.put(RedisConstants.FIELD_PREFIX_APP + "3", toBytes(""));

        assertTrue(new SessionInfo(SESSION_ID, hash).getApplications().isEmpty());
    }

    @Test
    void testSubjectKeepsAnySeparatorItContains() {

        Map<String, byte[]> hash = hash();
        hash.put(RedisKeyUtils.getAppField(3, "oidc", join("alice", "extra")), toBytes(""));

        SessionInfo session = new SessionInfo(SESSION_ID, hash);

        // The subject is the last component, so it stays whole however many separators it holds.
        assertEquals(1, session.getApplications().size());
        assertEquals(join("alice", "extra"), session.getApplications().get(0).getSubject());
    }

    @Test
    void testTwoApplicationsDifferingOnlyInTheInboundProtocolAreBothKept() {

        Map<String, byte[]> hash = hash();
        hash.put(RedisKeyUtils.getAppField(3, "oidc", "alice"), toBytes(""));
        hash.put(RedisKeyUtils.getAppField(3, "samlsso", "alice"), toBytes(""));

        assertEquals(2, new SessionInfo(SESSION_ID, hash).getApplications().size());
    }

    @Test
    void testReturnedCollectionsAreReadOnly() {

        SessionInfo session = new SessionInfo(SESSION_ID, hash());

        assertThrows(UnsupportedOperationException.class, () -> session.getMetadata().put("a", "b"));
        assertThrows(UnsupportedOperationException.class, () -> session.getApplications().clear());
    }

    private static Map<String, byte[]> hash() {

        Map<String, byte[]> hash = new HashMap<>();
        hash.put(RedisConstants.FIELD_TIME_CREATED, toBytes(String.valueOf(TIME_CREATED)));
        return hash;
    }

    private static String join(String... components) {

        return String.join(RedisConstants.FIELD_VALUE_SEPARATOR, components);
    }

    private static byte[] toBytes(String value) {

        return value.getBytes(StandardCharsets.UTF_8);
    }
}
