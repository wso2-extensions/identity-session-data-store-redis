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
import org.wso2.carbon.identity.application.authentication.framework.store.JavaSessionSerializer;
import org.wso2.carbon.identity.application.authentication.framework.store.SessionSerializer;
import org.wso2.carbon.identity.application.authentication.framework.util.FrameworkUtils;
import org.wso2.carbon.identity.core.cache.CacheEntry;
import org.wso2.carbon.identity.session.store.redis.config.RedisStoreConfig;
import org.wso2.carbon.identity.session.store.redis.internal.RedisSessionStoreDataHolder;
import org.wso2.carbon.identity.session.store.redis.util.RedisConstants;
import org.wso2.carbon.identity.session.store.redis.util.RedisKeyUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Runs the store against a Redis server, so that what it does to the keyspace is asserted through the
 * store rather than only through its scripts.
 * <p>
 * Skipped when no server is reachable. The server can be given with
 * {@code -Dredis.test.uri=redis://host:port} and defaults to port 6380, so a test run cannot reach a
 * deployed server. One can be started with {@code docker run -d --rm -p 6380:6379 redis:7-alpine}.
 */
public class RedisSessionDataStoreIntegrationTest {

    private static final String DEFAULT_URI = "redis://localhost:6380";
    private static final RedisCodec<String, byte[]> CODEC =
            RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE);

    private static final String PREFIX = "itest";
    private static final String TYPE = RedisConstants.TYPE_SESSION_CONTEXT_CACHE;
    private static final String OTHER_TYPE = "AppAuthFrameworkOperationDataStore";
    private static final int TENANT_ID = 3;
    private static final String SESSION_ID = "session-1";
    private static final String USER_ID = "user-1";
    private static final String IDP_SESSION_ID = "idp-session-1";
    private static final int IDP_ID = 7;

    private static RedisClient client;
    private static StatefulRedisConnection<String, byte[]> connection;
    private static RedisCommands<String, byte[]> redis;

    private RedisSessionContext context;
    private RedisSessionDataStore store;

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
            assumeTrue(false, "No Redis server at " + uri + ". Start one with "
                    + "'docker run -d --rm -p 6380:6379 redis:7-alpine'.");
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
        context = new RedisSessionContext(config().build());
        store = new RedisSessionDataStore(context);
    }

    @Test
    void testSessionRoundTrip() {

        store.storeSessionData(SESSION_ID, TYPE, entry("payload"), TENANT_ID);

        assertEquals("payload", value(store.getSessionData(SESSION_ID, TYPE)));
        assertNotNull(store.getSessionContextData(SESSION_ID, TYPE));
        assertTrue(store.getSessionContextData(SESSION_ID, TYPE).getNanoTime() > 0);
        assertTrue(redis.pttl(sessionKey(SESSION_ID)) > 0, "the session must carry an expiry");
    }

    @Test
    void testAStoreDoesNotIndexASessionThatCarriesNoUser() {

        // The sessions of a tenant are the ones the active session count reports, and a session is
        // counted only once it carries a user, so the member is created by the DAO rather than here.
        store.storeSessionData(SESSION_ID, TYPE, entry("payload"), TENANT_ID);

        assertEquals(0L, redis.exists(tenantKey(TENANT_ID)),
                "a store must not add a session that has no user to the sessions of its tenant");
    }

    @Test
    void testExtendingASessionRefreshesTheTenantIndex() {

        store.storeSessionData(SESSION_ID, TYPE, entry("payload"), TENANT_ID);
        // The session is added to the sessions of its tenant once, by the DAO, and its score is the
        // expiry the session had at that moment.
        double initial = System.currentTimeMillis() + 1000;
        redis.zadd(tenantKey(TENANT_ID), initial, bytes(SESSION_ID));

        // Extending a session re-stores it with a later expiry, and does not go through the DAO.
        long before = System.currentTimeMillis();
        store.storeSessionData(SESSION_ID, TYPE, entry("payload"), TENANT_ID);

        Double score = redis.zscore(tenantKey(TENANT_ID), bytes(SESSION_ID));
        assertNotNull(score, "an extended session must stay in the sessions of its tenant");
        assertTrue(score > initial, "the score must follow the expiry of the session: " + score);

        long ttl = redis.pttl(tenantKey(TENANT_ID));
        assertTrue(ttl > 1000, "the tenant index must expire rather than live forever: " + ttl);
        assertTrue(ttl <= score - before + 1000,
                "the tenant index must not outlive its last session: " + ttl);
    }

    @Test
    void testASessionScoredAsExpiredIsRescuedByAnExtensionOfTheTenantIndex() {

        store.storeSessionData(SESSION_ID, TYPE, entry("payload"), TENANT_ID);
        // A session extended past the expiry it was scored by is no longer counted as active, which the
        // extension has to undo.
        redis.zadd(tenantKey(TENANT_ID), (double) (System.currentTimeMillis() - 1000), bytes(SESSION_ID));

        store.storeSessionData(SESSION_ID, TYPE, entry("payload"), TENANT_ID);

        Double score = redis.zscore(tenantKey(TENANT_ID), bytes(SESSION_ID));
        assertNotNull(score, "the session must not be trimmed while it is still stored");
        assertTrue(score > System.currentTimeMillis(), "the session must read as live again: " + score);
    }

    @Test
    void testExtendingASessionRefreshesTheUserIndex() {

        store.storeSessionData(SESSION_ID, TYPE, entry("payload"), TENANT_ID);
        // The mapping between the user and the session is written once, by the DAO, and its score is the
        // expiry the session had at that moment.
        redis.hset(sessionKey(SESSION_ID), RedisConstants.FIELD_USER_ID, bytes(USER_ID));
        double initial = System.currentTimeMillis() + 1000;
        redis.zadd(userKey(USER_ID), initial, bytes(SESSION_ID));

        // Extending a session re-stores it with a later expiry, and does not go through the DAO.
        store.storeSessionData(SESSION_ID, TYPE, entry("payload"), TENANT_ID);

        Double score = redis.zscore(userKey(USER_ID), bytes(SESSION_ID));
        assertNotNull(score, "an extended session must stay in the sessions of its user");
        assertTrue(score > initial, "the score must follow the expiry of the session: " + score);
        assertTrue(redis.pttl(userKey(USER_ID)) > 1000,
                "the user index must live as long as the session it was extended for");
    }

    @Test
    void testASessionScoredAsExpiredIsRescuedByAnExtension() {

        store.storeSessionData(SESSION_ID, TYPE, entry("payload"), TENANT_ID);
        redis.hset(sessionKey(SESSION_ID), RedisConstants.FIELD_USER_ID, bytes(USER_ID));
        // A session extended past the expiry its mapping was scored by is invisible to the by user id
        // reads, which range over the unexpired members only.
        redis.zadd(userKey(USER_ID), (double) (System.currentTimeMillis() - 1000), bytes(SESSION_ID));

        store.storeSessionData(SESSION_ID, TYPE, entry("payload"), TENANT_ID);

        Double score = redis.zscore(userKey(USER_ID), bytes(SESSION_ID));
        assertNotNull(score, "the session must not be trimmed while it is still stored");
        assertTrue(score > System.currentTimeMillis(), "the session must read as live again: " + score);
    }

    @Test
    void testExtendingASessionRefreshesItsFederatedMappings() {

        // The mapping is written once, with the expiry the session had at that moment, and an extension
        // of the session does not go through the DAO, so without this it would expire under a live session.
        store.storeSessionData(SESSION_ID, TYPE, entry("payload"), TENANT_ID);
        federate(SESSION_ID, IDP_SESSION_ID, 2000);

        store.storeSessionData(SESSION_ID, TYPE, entry("payload"), TENANT_ID);

        assertTrue(redis.pttl(mappingKey(IDP_SESSION_ID)) > 2000,
                "the mapping must follow the expiry of the session it belongs to");
        assertTrue(redis.pttl(idpIndexKey(IDP_SESSION_ID)) > 2000,
                "the index must outlive the mapping it points at");
    }

    @Test
    void testRemovalClearsTheFederatedMappingsOfTheSession() {

        // The session carries its mappings as fields, so the deletion reports them and the cleanup that
        // follows needs no index of its own to find them.
        store.storeSessionData(SESSION_ID, TYPE, entry("payload"), TENANT_ID);
        federate(SESSION_ID, IDP_SESSION_ID, 60000);

        store.clearSessionData(SESSION_ID, TYPE);

        assertEquals(0L, redis.exists(mappingKey(IDP_SESSION_ID)));
        assertEquals(0L, redis.exists(idpIndexKey(IDP_SESSION_ID)), "an emptied index is dropped");
    }

    @Test
    void testBatchRemovalClearsTheFederatedMappingsOfEverySession() {

        for (int index = 0; index < 3; index++) {
            store.storeSessionData(SESSION_ID + "-" + index, TYPE, entry("payload"), TENANT_ID);
            federate(SESSION_ID + "-" + index, IDP_SESSION_ID + "-" + index, 60000);
        }

        store.clearSessionDataBatch(List.of(SESSION_ID + "-0", SESSION_ID + "-1", SESSION_ID + "-2"), TYPE);

        assertTrue(redis.keys(PREFIX + ":fed:*").isEmpty(), "every mapping and index must be removed");
    }

    @Test
    void testAStoreDoesNotReturnASessionToTheUserIndex() {

        store.storeSessionData(SESSION_ID, TYPE, entry("payload"), TENANT_ID);
        // The user is still on the session, but the mapping is gone, which is what a logout leaves behind
        // for as long as a reordered store can still arrive.
        redis.hset(sessionKey(SESSION_ID), RedisConstants.FIELD_USER_ID, bytes(USER_ID));

        store.storeSessionData(SESSION_ID, TYPE, entry("payload"), TENANT_ID);

        assertEquals(0L, redis.exists(userKey(USER_ID)),
                "a store must not create a mapping between a user and a session");
    }

    @Test
    void testAStaleStoreDoesNotReturnASessionToTheTenantIndex() {

        long now = FrameworkUtils.getCurrentStandardNano();
        store.persistSessionData(SESSION_ID, TYPE, entry("payload"), now, TENANT_ID);
        store.clearSessionData(SESSION_ID, TYPE);

        // An older store arriving after the removal is rejected by the store script, so the index must
        // not be updated either.
        store.persistSessionData(SESSION_ID, TYPE, entry("payload"), now - 1000, TENANT_ID);

        assertEquals("payload", value(store.getSessionData(SESSION_ID, TYPE)));
        assertEquals(0L, redis.exists(tenantKey(TENANT_ID)),
                "a store must not add a session to the sessions of its tenant");
    }

    @Test
    void testRemovalClearsTheUserAndTenantIndexes() {

        store.storeSessionData(SESSION_ID, TYPE, entry("payload"), TENANT_ID);
        // The user of a session and both index members are written by the DAO; here they are written
        // directly, since the removal path of the store reads them to find the indexes it has to clean.
        redis.hset(sessionKey(SESSION_ID), RedisConstants.FIELD_USER_ID, bytes(USER_ID));
        redis.zadd(userKey(USER_ID), (double) (System.currentTimeMillis() + 60000), bytes(SESSION_ID));
        redis.zadd(tenantKey(TENANT_ID), (double) (System.currentTimeMillis() + 60000), bytes(SESSION_ID));

        store.clearSessionData(SESSION_ID, TYPE);

        assertNull(store.getSessionData(SESSION_ID, TYPE));
        assertEquals(0L, redis.exists(sessionKey(SESSION_ID)));
        assertEquals(0L, redis.exists(userKey(USER_ID)), "an emptied user index is dropped");
        assertEquals(0L, redis.exists(tenantKey(TENANT_ID)), "an emptied tenant index is dropped");
    }

    @Test
    void testBatchRemovalOfSessionsClearsEveryIndex() {

        for (int index = 0; index < 5; index++) {
            String sessionId = SESSION_ID + "-" + index;
            store.storeSessionData(sessionId, TYPE, entry("payload"), TENANT_ID);
            redis.hset(sessionKey(sessionId), RedisConstants.FIELD_USER_ID, bytes(USER_ID));
            redis.zadd(userKey(USER_ID), (double) (System.currentTimeMillis() + 60000), bytes(sessionId));
            redis.zadd(tenantKey(TENANT_ID), (double) (System.currentTimeMillis() + 60000),
                    bytes(sessionId));
        }

        store.clearSessionDataBatch(List.of(SESSION_ID + "-0", SESSION_ID + "-1", SESSION_ID + "-2",
                SESSION_ID + "-3", SESSION_ID + "-4"), TYPE);

        assertEquals(0L, redis.exists(tenantKey(TENANT_ID)));
        assertEquals(0L, redis.exists(userKey(USER_ID)));
        for (int index = 0; index < 5; index++) {
            assertEquals(0L, redis.exists(sessionKey(SESSION_ID + "-" + index)));
        }
    }

    @Test
    void testBatchRemovalReportsTheSessionsItCouldNotRemove() {

        for (int index = 0; index < 3; index++) {
            String sessionId = SESSION_ID + "-" + index;
            store.storeSessionData(sessionId, TYPE, entry("payload"), TENANT_ID);
            redis.hset(sessionKey(sessionId), RedisConstants.FIELD_USER_ID, bytes(USER_ID));
            redis.zadd(userKey(USER_ID), (double) (System.currentTimeMillis() + 60000), bytes(sessionId));
            redis.zadd(tenantKey(TENANT_ID), (double) (System.currentTimeMillis() + 60000),
                    bytes(sessionId));
        }
        // A string where the session hash is expected, so the deletion of it is rejected by the server
        // while the deletions around it succeed.
        String rejected = SESSION_ID + "-1";
        redis.del(sessionKey(rejected));
        redis.set(sessionKey(rejected), bytes("not-a-hash"));

        store.clearSessionDataBatch(List.of(SESSION_ID + "-0", rejected, SESSION_ID + "-2"), TYPE);

        assertEquals(0L, redis.exists(sessionKey(SESSION_ID + "-0")), "the batch is removed past a failure");
        assertEquals(0L, redis.exists(sessionKey(SESSION_ID + "-2")), "the batch is removed past a failure");
        assertEquals(1L, redis.exists(sessionKey(rejected)), "a rejected deletion leaves the key");
        // The key is still there, so its indexes are kept rather than orphaning it.
        assertNotNull(redis.zscore(userKey(USER_ID), bytes(rejected)));
        assertNotNull(redis.zscore(tenantKey(TENANT_ID), bytes(rejected)));
        assertNull(redis.zscore(userKey(USER_ID), bytes(SESSION_ID + "-0")));
        assertNull(redis.zscore(tenantKey(TENANT_ID), bytes(SESSION_ID + "-2")));
    }

    @Test
    void testBatchRemovalOfRecordsThatAreNotSessions() {

        store.storeSessionData("record-1", OTHER_TYPE, entry("payload"), TENANT_ID);
        store.storeSessionData("record-2", OTHER_TYPE, entry("payload"), TENANT_ID);

        store.clearSessionDataBatch(List.of("record-1", "record-2"), OTHER_TYPE);

        assertNull(store.getSessionData("record-1", OTHER_TYPE));
        assertNull(store.getSessionData("record-2", OTHER_TYPE));
        // A record of another type is not a session, so it is not indexed by tenant.
        assertEquals(0L, redis.exists(tenantKey(TENANT_ID)));
    }

    @Test
    void testARegisteredSerializerIsUsed() {

        // A deployment that registers its own serializer must get it here too, or the two stores write
        // bytes the other cannot read. It is read from the data holder per operation, as the relational
        // store reads it from the data holder of the framework, so registering it after the store was
        // built is enough.
        RedisSessionStoreDataHolder.setSessionSerializer(new PrefixSerializer());
        try {
            store.storeSessionData(SESSION_ID, TYPE, entry("payload"), TENANT_ID);

            byte[] stored = redis.hget(sessionKey(SESSION_ID), RedisConstants.FIELD_SESSION_OBJECT);
            assertEquals(PrefixSerializer.PREFIX + "payload", new String(stored, StandardCharsets.UTF_8));
            assertEquals("payload", store.getSessionData(SESSION_ID, TYPE));
        } finally {
            RedisSessionStoreDataHolder.setSessionSerializer(new JavaSessionSerializer());
        }
    }

    @Test
    void testEveryOperationDegradesWhenRedisIsUnreachable() {

        // The central guarantee of the store: an outage must not fail an authentication, so a write is
        // dropped and a read reports the session as absent.
        RedisSessionContext unreachable = new RedisSessionContext(config()
                .hosts("localhost:1")
                .connectionTimeout(200)
                .commandTimeout(200)
                .build());
        RedisSessionDataStore unreachableStore = new RedisSessionDataStore(unreachable);

        unreachableStore.storeSessionData(SESSION_ID, TYPE, entry("payload"), TENANT_ID);
        unreachableStore.clearSessionData(SESSION_ID, TYPE);
        unreachableStore.clearSessionDataBatch(List.of(SESSION_ID), TYPE);
        unreachableStore.removeTempAuthnContextData(SESSION_ID, TYPE);

        assertNull(unreachableStore.getSessionData(SESSION_ID, TYPE));
        assertNull(unreachableStore.getSessionContextData(SESSION_ID, TYPE));
        assertFalse(unreachableStore.validateLastOperationOnSessionData(SESSION_ID, TYPE,
                RedisConstants.OPERATION_STORE));
        assertFalse(unreachable.isConnected());
        unreachable.close();
    }

    private static RedisStoreConfig.Builder config() {

        String uri = System.getProperty("redis.test.uri", DEFAULT_URI);
        RedisURI parsed = RedisURI.create(uri);
        return new RedisStoreConfig.Builder()
                .hosts(parsed.getHost() + ":" + parsed.getPort())
                .keyPrefix(PREFIX)
                .batchSize(2)
                .commandTimeout(2000);
    }

    /**
     * Records a federated mapping the way the federated store does: a field on the session, the mapping
     * itself and the index of its identity provider session, the last two with an expiry of their own.
     */
    private void federate(String sessionId, String idpSessionId, long expiry) {

        redis.hset(sessionKey(sessionId),
                RedisKeyUtils.getPrefixedFederatedField(TENANT_ID, IDP_ID, idpSessionId), bytes(""));
        redis.hset(mappingKey(idpSessionId), RedisConstants.FIELD_FED_SESSION_ID, bytes(sessionId));
        redis.pexpire(mappingKey(idpSessionId), expiry);
        redis.sadd(idpIndexKey(idpSessionId), bytes(TENANT_ID + ":" + IDP_ID));
        redis.pexpire(idpIndexKey(idpSessionId), expiry);
    }

    private static String mappingKey(String idpSessionId) {

        return PREFIX + ":fed:" + TENANT_ID + ":" + IDP_ID + ":" + idpSessionId;
    }

    private static String idpIndexKey(String idpSessionId) {

        return PREFIX + ":fed:idp:" + idpSessionId;
    }

    private static String sessionKey(String sessionId) {

        return PREFIX + ":s:" + TYPE + ":" + sessionId;
    }

    private static String userKey(String userId) {

        return PREFIX + ":u:" + userId;
    }

    private static String tenantKey(int tenantId) {

        return PREFIX + ":tid:" + tenantId;
    }

    private static byte[] bytes(String value) {

        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static TestCacheEntry entry(String value) {

        return new TestCacheEntry(value, TimeUnit.MINUTES.toNanos(30));
    }

    private static String value(Object entry) {

        return entry instanceof TestCacheEntry ? ((TestCacheEntry) entry).getValue() : null;
    }

    /**
     * A record carrying its own validity period, so the store does not have to resolve one from the
     * timeout configuration of a tenant.
     */
    private static class TestCacheEntry extends CacheEntry {

        private static final long serialVersionUID = 1L;

        private final String value;

        TestCacheEntry(String value, long validityPeriodNano) {

            this.value = value;
            setValidityPeriod(validityPeriodNano);
        }

        String getValue() {

            return value;
        }

        @Override
        public String toString() {

            return value;
        }
    }

    /**
     * A serializer that is obviously not Java serialization, so which one the store used is visible in
     * the stored bytes.
     */
    private static class PrefixSerializer implements SessionSerializer {

        private static final String PREFIX = "custom:";

        @Override
        public InputStream serializeSessionObject(Object session) {

            return new ByteArrayInputStream((PREFIX + session).getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public Object deSerializeSessionObject(InputStream inputStream) {

            try {
                String value = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
                return value.startsWith(PREFIX) ? value.substring(PREFIX.length()) : value;
            } catch (java.io.IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
