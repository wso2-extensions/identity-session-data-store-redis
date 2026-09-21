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

package org.wso2.carbon.identity.session.store.redis.dao;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.wso2.carbon.identity.application.authentication.framework.context.AuthHistory;
import org.wso2.carbon.identity.session.store.redis.config.RedisStoreConfig;
import org.wso2.carbon.identity.session.store.redis.exception.RedisSessionStoreException;
import org.wso2.carbon.identity.session.store.redis.model.FederatedSessionRef;
import org.wso2.carbon.identity.session.store.redis.util.RedisConstants;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Runs the federated authentication session mappings against a Redis server, in particular the case
 * where a mapping has expired while the index pointing at it has not.
 * <p>
 * Skipped when no server is reachable. See {@code RedisSessionDataStoreIntegrationTest} for how to
 * start one.
 */
public class RedisFederatedSessionStoreTest {

    private static final String DEFAULT_URI = "redis://localhost:6380";
    private static final RedisCodec<String, byte[]> CODEC =
            RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE);

    private static final String PREFIX = "ftest";
    private static final String SESSION_ID = "session-1";
    private static final String IDP_SESSION_ID = "idp-session-1";
    private static final int TENANT_ID = 3;
    private static final int IDP_ID = 7;
    private static final long EXTENSION = 600000L;
    private static final long SESSION_EXPIRY = 300000L;

    private static RedisClient client;
    private static StatefulRedisConnection<String, byte[]> connection;
    private static RedisCommands<String, byte[]> redis;

    private RedisSessionContext context;
    private RedisFederatedSessionStore federatedSessionStore;

    @BeforeAll
    static void connect() {

        String uri = System.getProperty("redis.test.uri", DEFAULT_URI);
        try {
            client = RedisClient.create(RedisURI.create(uri));
            client.setOptions(ClientOptions.builder().autoReconnect(false).build());
            connection = client.connect(CODEC);
            connection.setTimeout(Duration.ofSeconds(2));
            redis = connection.sync();
            redis.ping();
        } catch (RuntimeException e) {
            close();
            assumeTrue(false, "No Redis server at " + uri + ".");
        }
    }

    @AfterAll
    static void close() {

        if (connection != null) {
            connection.close();
            connection = null;
        }
        if (client != null) {
            client.shutdown();
            client = null;
        }
    }

    @BeforeEach
    void clean() {

        List<String> keys = redis.keys(PREFIX + ":*");
        if (!keys.isEmpty()) {
            redis.del(keys.toArray(new String[0]));
        }
        String uri = System.getProperty("redis.test.uri", DEFAULT_URI);
        RedisURI parsed = RedisURI.create(uri);
        context = new RedisSessionContext(new RedisStoreConfig.Builder()
                .hosts(parsed.getHost() + ":" + parsed.getPort())
                .keyPrefix(PREFIX)
                .commandTimeout(2000)
                .build());
        federatedSessionStore = new RedisFederatedSessionStore(context);
    }

    /**
     * Writes a session hash the way the store script does, since a federated mapping is now recorded on
     * the session and is dropped when there is none.
     */
    private void storeSession(String sessionId) {

        redis.hset(sessionKey(sessionId), RedisConstants.FIELD_TIME_CREATED, bytes("1"));
        redis.pexpire(sessionKey(sessionId), SESSION_EXPIRY);
    }

    @Test
    void testMappingAndItsIndexesCarryAnExpiry() throws Exception {

        storeSession(SESSION_ID);

        federatedSessionStore.store(SESSION_ID, authHistory(), TENANT_ID, IDP_ID);

        assertTrue(redis.pttl(mappingKey()) > 0, "the mapping must carry an expiry");
        assertTrue(redis.pttl(idpSessionIndexKey()) > 0);
        assertEquals(SESSION_ID, string(redis.hget(mappingKey(), "sid")));
        assertTrue(federatedSessionStore.hasMapping(TENANT_ID, IDP_ID, IDP_SESSION_ID));
    }

    @Test
    void testTheSessionRecordsTheMappingAndNeedsNoIndexOfItsOwn() throws Exception {

        storeSession(SESSION_ID);

        federatedSessionStore.store(SESSION_ID, authHistory(), TENANT_ID, IDP_ID);

        // The reference lives on the session, so it expires with the session and needs no expiry of its
        // own, which is the whole reason the per session index was dropped.
        assertTrue(redis.hexists(sessionKey(SESSION_ID), federatedField(IDP_SESSION_ID)));
        assertTrue(redis.keys(PREFIX + ":fed:s:*").isEmpty(), "no per session index is written");
    }

    @Test
    void testAMappingIsDroppedWhenTheSessionIsNotThere() throws Exception {

        // The framework stores the session before the mapping, so no session means the session is gone,
        // and a mapping written for it would be unreachable from the session for the rest of its life.
        federatedSessionStore.store(SESSION_ID, authHistory(), TENANT_ID, IDP_ID);

        assertEquals(0L, redis.exists(mappingKey()), "the mapping must be dropped");
        assertEquals(0L, redis.exists(idpSessionIndexKey()), "the index must not be written either");
        assertFalse(federatedSessionStore.hasMapping(TENANT_ID, IDP_ID, IDP_SESSION_ID));
    }

    @Test
    void testActiveMappingsFindTheMapping() throws Exception {

        storeSession(SESSION_ID);
        federatedSessionStore.store(SESSION_ID, authHistory(), TENANT_ID, IDP_ID);

        List<FederatedSessionRef> references = federatedSessionStore.getActiveFedSessionMappings(IDP_SESSION_ID);

        assertEquals(1, references.size());
        assertEquals(TENANT_ID, references.get(0).getTenantId());
        assertEquals(IDP_ID, references.get(0).getIdpId());
        assertEquals(IDP_SESSION_ID, references.get(0).getIdpSessionId());
    }

    @Test
    void testAnExpiredMappingIsNotReportedAsLiveAndIsPruned() throws Exception {

        storeSession(SESSION_ID);
        federatedSessionStore.store(SESSION_ID, authHistory(), TENANT_ID, IDP_ID);
        // The index of an identity provider session expires at the latest expiry of its members, so a
        // mapping can be gone while its member survives. Membership alone must not answer the question.
        redis.del(mappingKey());

        assertTrue(federatedSessionStore.getActiveFedSessionMappings(IDP_SESSION_ID).isEmpty());
        assertFalse(federatedSessionStore.hasMapping(TENANT_ID, IDP_ID, IDP_SESSION_ID));
        assertEquals(0L, redis.exists(idpSessionIndexKey()), "the stale member must be pruned");
    }

    @Test
    void testRefreshExpiryRaisesTheMappingAndItsIndexes() throws Exception {

        // A mapping is given the expiry of the session when it is created, and an extension of the
        // session does not go through that path, so the mapping has to be raised to follow it.
        storeSession(SESSION_ID);
        federatedSessionStore.store(SESSION_ID, authHistory(), TENANT_ID, IDP_ID);
        long before = redis.pttl(mappingKey());

        federatedSessionStore.refreshExpiry(members(IDP_SESSION_ID), before + EXTENSION);

        assertTrue(redis.pttl(mappingKey()) > before, "the mapping must follow the session");
        assertTrue(redis.pttl(idpSessionIndexKey()) > before,
                "the identity provider index must outlive the mapping it points at");
    }

    @Test
    void testRefreshExpiryNeverLowersTheExpiry() throws Exception {

        // Stores from two nodes are not ordered, so an earlier expiry can arrive last, and applying it
        // would drop a mapping while the session it belongs to is still live.
        storeSession(SESSION_ID);
        federatedSessionStore.store(SESSION_ID, authHistory(), TENANT_ID, IDP_ID);
        long before = redis.pttl(mappingKey());

        federatedSessionStore.refreshExpiry(members(IDP_SESSION_ID), 1000L);

        assertTrue(redis.pttl(mappingKey()) > before - 2000, "a shorter expiry must not be applied");
        assertTrue(redis.pttl(idpSessionIndexKey()) > before - 2000);
    }

    @Test
    void testRefreshExpiryOfASessionThatFederatesNothingIsANoOperation() throws Exception {

        federatedSessionStore.refreshExpiry(List.of(), EXTENSION);

        assertTrue(redis.keys(PREFIX + ":fed:*").isEmpty(), "nothing must be created by a refresh");
    }

    @Test
    void testRefreshExpiryDoesNotResurrectAMappingThatIsGone() throws Exception {

        // A mapping expires on its own, so an extension can arrive for one that is already gone.
        federatedSessionStore.refreshExpiry(members(IDP_SESSION_ID), EXTENSION);

        assertEquals(0L, redis.exists(mappingKey()));
        assertEquals(0L, redis.exists(idpSessionIndexKey()));
    }

    @Test
    void testRemovingBySessionRemovesTheMappingAndItsIndex() throws Exception {

        storeSession(SESSION_ID);
        federatedSessionStore.store(SESSION_ID, authHistory(), TENANT_ID, IDP_ID);

        federatedSessionStore.removeBySession(SESSION_ID);

        assertEquals(0L, redis.exists(mappingKey()));
        assertEquals(0L, redis.exists(idpSessionIndexKey()));
        assertFalse(redis.hexists(sessionKey(SESSION_ID), federatedField(IDP_SESSION_ID)),
                "the reference on the session must go with the mapping");
    }

    @Test
    void testRemovingReportedMappingsNeedsNoSession() throws Exception {

        // The deletion of a session reports the mappings it held, so the cleanup that follows it has
        // nothing left to read them from and must work from the reported members alone.
        storeSession(SESSION_ID);
        federatedSessionStore.store(SESSION_ID, authHistory(), TENANT_ID, IDP_ID);
        redis.del(sessionKey(SESSION_ID));

        federatedSessionStore.removeMappings(members(IDP_SESSION_ID));

        assertEquals(0L, redis.exists(mappingKey()));
        assertEquals(0L, redis.exists(idpSessionIndexKey()));
    }

    @Test
    void testRemovingBySessionAndIdentityProviderKeepsTheOtherMapping() throws Exception {

        storeSession(SESSION_ID);
        federatedSessionStore.store(SESSION_ID, authHistory(), TENANT_ID, IDP_ID);
        AuthHistory other = new AuthHistory("SAMLSSOAuthenticator", "azure");
        other.setIdpSessionIndex("idp-session-2");
        federatedSessionStore.store(SESSION_ID, other, TENANT_ID, IDP_ID + 1);

        federatedSessionStore.removeBySession(SESSION_ID, IDP_ID);

        assertEquals(0L, redis.exists(mappingKey()));
        assertTrue(federatedSessionStore.hasMapping(TENANT_ID, IDP_ID + 1, "idp-session-2"));
        assertFalse(redis.hexists(sessionKey(SESSION_ID), federatedField(IDP_SESSION_ID)));
        assertTrue(redis.hexists(sessionKey(SESSION_ID), "f:" + TENANT_ID + ":" + (IDP_ID + 1)
                + ":idp-session-2"), "the mapping of the other identity provider must stay");
    }

    @Test
    void testAMappingMovedToAnotherSessionIsNotRemovedWithThePreviousOne() throws Exception {

        storeSession(SESSION_ID);
        storeSession("session-2");
        federatedSessionStore.store(SESSION_ID, authHistory(), TENANT_ID, IDP_ID);
        federatedSessionStore.detachPreviousSession("session-2", IDP_SESSION_ID, TENANT_ID, IDP_ID);
        federatedSessionStore.store("session-2", authHistory(), TENANT_ID, IDP_ID);

        federatedSessionStore.removeBySession(SESSION_ID);

        assertTrue(federatedSessionStore.hasMapping(TENANT_ID, IDP_ID, IDP_SESSION_ID));
        assertFalse(redis.hexists(sessionKey(SESSION_ID), federatedField(IDP_SESSION_ID)),
                "the previous session must stop referring to the mapping");
        assertTrue(redis.hexists(sessionKey("session-2"), federatedField(IDP_SESSION_ID)));
    }

    @Test
    void testRemovingSeveralSessionsRemovesEveryMappingAndIndex() throws Exception {

        List<String> members = new ArrayList<>();
        for (int index = 0; index < 4; index++) {
            AuthHistory authHistory = new AuthHistory("OpenIDConnectAuthenticator", "google");
            authHistory.setIdpSessionIndex(IDP_SESSION_ID + "-" + index);
            storeSession(SESSION_ID + "-" + index);
            federatedSessionStore.store(SESSION_ID + "-" + index, authHistory, TENANT_ID, IDP_ID);
            members.add(TENANT_ID + ":" + IDP_ID + ":" + IDP_SESSION_ID + "-" + index);
        }

        federatedSessionStore.removeMappings(members);

        assertTrue(redis.keys(PREFIX + ":fed:" + TENANT_ID + ":*").isEmpty(), "every mapping must be removed");
        assertTrue(redis.keys(PREFIX + ":fed:idp:*").isEmpty(), "every identity provider index must be removed");
    }

    @Test
    void testRemovingNoMappingIsANoOperation() throws Exception {

        storeSession(SESSION_ID);
        federatedSessionStore.store(SESSION_ID, authHistory(), TENANT_ID, IDP_ID);

        federatedSessionStore.removeMappings(List.of());

        assertTrue(federatedSessionStore.hasMapping(TENANT_ID, IDP_ID, IDP_SESSION_ID));
    }

    @Test
    void testASessionWithoutMappingsHasNone() throws RedisSessionStoreException {

        assertTrue(federatedSessionStore.getActiveFedSessionMappings(IDP_SESSION_ID).isEmpty());
        assertFalse(federatedSessionStore.hasMapping(TENANT_ID, IDP_ID, IDP_SESSION_ID));
        federatedSessionStore.removeBySession(SESSION_ID);
        federatedSessionStore.removeBySession(SESSION_ID, IDP_ID);
    }

    private static AuthHistory authHistory() {

        AuthHistory authHistory = new AuthHistory("OpenIDConnectAuthenticator", "google");
        authHistory.setIdpSessionIndex(IDP_SESSION_ID);
        authHistory.setRequestType("oidc");
        return authHistory;
    }

    private static String mappingKey() {

        return PREFIX + ":fed:" + TENANT_ID + ":" + IDP_ID + ":" + IDP_SESSION_ID;
    }

    private static List<String> members(String idpSessionId) {

        return List.of(TENANT_ID + ":" + IDP_ID + ":" + idpSessionId);
    }

    private static String sessionKey(String sessionId) {

        return PREFIX + ":s:AppAuthFrameworkSessionContextCache:" + sessionId;
    }

    private static String federatedField(String idpSessionId) {

        return "f:" + TENANT_ID + ":" + IDP_ID + ":" + idpSessionId;
    }

    private static String idpSessionIndexKey() {

        return PREFIX + ":fed:idp:" + IDP_SESSION_ID;
    }

    private static byte[] bytes(String value) {

        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String string(byte[] value) {

        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }
}
