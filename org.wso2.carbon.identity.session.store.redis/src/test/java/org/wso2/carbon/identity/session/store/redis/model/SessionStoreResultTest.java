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
import org.wso2.carbon.identity.session.store.redis.util.RedisConstants;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the replies of the store and delete scripts, which carry the status of a store and everything
 * that refers to the session, since neither can be read again afterwards.
 */
public class SessionStoreResultTest {

    @Test
    void testAFirstStoreAndAnExtensionAreBothStored() {

        assertTrue(SessionStoreResult.build(
                reply(RedisConstants.STORE_STATUS_CREATED, "", List.of())).isStored());
        assertTrue(SessionStoreResult.build(
                reply(RedisConstants.STORE_STATUS_EXTENDED, "", List.of())).isStored());
    }

    @Test
    void testAStaleStoreIsNotStored() {

        SessionStoreResult result = SessionStoreResult.build(
                reply(RedisConstants.STORE_STATUS_STALE, "", List.of()));

        assertFalse(result.isStored(), "the indexes must not be updated for a store that was rejected");
    }

    @Test
    void testAStoreCarriesTheUserAndTheFederatedMappings() {

        SessionStoreResult result = SessionStoreResult.build(reply(
                RedisConstants.STORE_STATUS_EXTENDED, "user-1", List.of("3:7:idp-session-1", "3:8:other")));

        assertTrue(result.isStored());
        assertEquals("user-1", result.getUserId());
        assertEquals(List.of("3:7:idp-session-1", "3:8:other"), result.getFederatedMembers());
    }

    @Test
    void testAnEmptyUserIsReportedAsNone() {

        // The script answers with an empty string, since a Lua table cannot hold a nil.
        assertNull(SessionStoreResult.build(
                reply(RedisConstants.STORE_STATUS_EXTENDED, "", List.of())).getUserId());
    }

    @Test
    void testAnUnreadableReplyIsNotTakenForAStore() {

        // Updating the indexes for a reply that cannot be read would refer to a session that may not be
        // there, so anything unrecognised is treated as a rejection.
        assertFalse(SessionStoreResult.build(null).isStored());
        assertFalse(SessionStoreResult.build(List.of()).isStored());
        assertFalse(SessionStoreResult.build(reply(99, "user-1", List.of())).isStored());
        assertFalse(SessionStoreResult.build(List.of(bytes("not a number"))).isStored());
    }

    @Test
    void testAShortReplyReportsNothingRatherThanFailing() {

        SessionStoreResult result = SessionStoreResult.build(
                List.of(bytes(String.valueOf(RedisConstants.STORE_STATUS_EXTENDED))));

        assertTrue(result.isStored());
        assertNull(result.getUserId());
        assertTrue(result.getFederatedMembers().isEmpty());
    }

    @Test
    void testADeletionCarriesEveryIndexToClean() {

        SessionRemovalResult result = SessionRemovalResult.build(List.of(
                bytes("user-1"), bytes("3"), List.of(bytes("3:7:idp-session-1"))));

        assertEquals("user-1", result.getUserId());
        assertEquals(3, result.getTenantId());
        assertEquals(List.of("3:7:idp-session-1"), result.getFederatedMembers());
    }

    @Test
    void testADeletionOfAnEntryHoldingNothing() {

        SessionRemovalResult result = SessionRemovalResult.build(
                List.of(bytes(""), bytes(""), List.of()));

        assertNull(result.getUserId());
        assertNull(result.getTenantId(), "an entry that held no session record has no tenant");
        assertTrue(result.getFederatedMembers().isEmpty());
    }

    @Test
    void testADeletionWithNoReply() {

        SessionRemovalResult result = SessionRemovalResult.build(null);

        assertNull(result.getUserId());
        assertNull(result.getTenantId());
        assertTrue(result.getFederatedMembers().isEmpty());
    }

    @Test
    void testTheReportedMembersAreNotModifiable() {

        SessionStoreResult result = SessionStoreResult.build(reply(
                RedisConstants.STORE_STATUS_EXTENDED, "user-1", List.of("3:7:idp-session-1")));

        assertThrows(UnsupportedOperationException.class,
                () -> result.getFederatedMembers().add("3:7:another"));
    }

    private static List<Object> reply(int status, String userId, List<String> members) {

        List<Object> encoded = new ArrayList<>(members.size());
        for (String member : members) {
            encoded.add(bytes(member));
        }
        return List.of(bytes(String.valueOf(status)), bytes(userId), encoded);
    }

    private static byte[] bytes(String value) {

        return value.getBytes(StandardCharsets.UTF_8);
    }
}
