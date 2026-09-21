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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.wso2.carbon.identity.application.authentication.framework.exception.DuplicatedAuthUserException;
import org.wso2.carbon.identity.application.authentication.framework.exception.UserSessionException;
import org.wso2.carbon.identity.session.store.redis.config.RedisStoreConfig;
import org.wso2.carbon.identity.session.store.redis.util.RedisConstants;
import org.wso2.carbon.identity.session.store.redis.util.RedisKeyUtils;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Runs the by user id read path of the DAO against a Redis server, since it is the read behind
 * {@code GET /{user-id}/sessions}, {@code GET /me/sessions} and {@code DELETE /{user-id}/sessions} and
 * a defect in it answers all three with nothing rather than with an error.
 * <p>
 * Skipped when no server is reachable. The server can be given with
 * {@code -Dredis.test.uri=redis://host:port} and defaults to port 6380, so a test run cannot reach a
 * deployed server. One can be started with {@code docker run -d --rm -p 6380:6379 redis:7-alpine}.
 */
public class RedisUserSessionDAOIntegrationTest {

    private static final String DEFAULT_URI = "redis://localhost:6380";
    private static final RedisCodec<String, byte[]> CODEC =
            RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE);

    private static final String PREFIX = "itest";
    private static final String TYPE = RedisConstants.TYPE_SESSION_CONTEXT_CACHE;
    private static final int TENANT_ID = 3;
    private static final String USER_ID = "user-1";
    private static final long LIVE = 60000L;

    private static RedisClient client;
    private static StatefulRedisConnection<String, byte[]> connection;
    private static RedisCommands<String, byte[]> redis;

    private RedisSessionContext context;
    private RedisUserSessionDAOImpl dao;

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
        dao = new RedisUserSessionDAOImpl(context);
    }

    @AfterEach
    void release() {

        context.close();
    }

    @Test
    void testTheSessionsOfAUserAreRead() throws UserSessionException {

        storeSession("session-1");
        storeSession("session-2");
        storeSession("session-3");
        indexForUser("session-1", "session-2", "session-3");

        assertEquals(List.of("session-1", "session-2", "session-3"), dao.getActiveSessionIds(USER_ID));
        assertEquals(List.of("session-1", "session-2", "session-3"), dao.getSessionId(USER_ID));
    }

    @Test
    void testASessionWithoutARecordIsNotReported() throws UserSessionException {

        storeSession("session-1");
        // A session removed without its index entry, which is what a lost cleanup leaves behind.
        indexForUser("session-1", "session-gone");

        // Both reads answer from the sessions, not from the index alone, so neither reports the drift.
        assertEquals(List.of("session-1"), dao.getSessionId(USER_ID));
        assertEquals(List.of("session-1"), dao.getActiveSessionIds(USER_ID));
        assertNull(redis.zscore(userKey(USER_ID), bytes("session-gone")),
                "a session that is no longer available is pruned from the sessions of the user");
        assertNotNull(redis.zscore(userKey(USER_ID), bytes("session-1")));
    }

    @Test
    void testAnEntryWrittenAheadOfItsSessionIsNotReported() throws UserSessionException {

        // The user of a session can be written before the session itself, so the hash can exist while
        // holding no session. The relational query joins the session store, so it reports neither.
        redis.hset(sessionKey("session-1"), RedisConstants.FIELD_USER_ID, bytes(USER_ID));
        indexForUser("session-1");

        assertEquals(List.of(), dao.getSessionId(USER_ID));
        assertEquals(List.of(), dao.getActiveSessionIds(USER_ID));
    }

    @Test
    void testTheUserOfASessionIsRecordedOnTheSessionAndInTheIndex() throws UserSessionException {

        storeSession("session-1", null);

        dao.storeUserSessionData(USER_ID, "session-1");

        assertEquals(USER_ID, string(redis.hget(sessionKey("session-1"), RedisConstants.FIELD_USER_ID)));
        assertNotNull(redis.zscore(userKey(USER_ID), bytes("session-1")));
    }

    @Test
    void testTheUserOfASessionAddsItToTheSessionsOfItsTenant() throws UserSessionException {

        // The active session count reports the sessions of a tenant that carry a user, the way the
        // relational query joins the mapping of the user, so the session is indexed by tenant here
        // rather than when it is stored.
        storeSession("session-1", null);

        dao.storeUserSessionData(USER_ID, "session-1");

        Double score = redis.zscore(tenantKey(TENANT_ID), bytes("session-1"));
        assertNotNull(score, "a session that carries a user must be counted for its tenant");
        assertTrue(score > System.currentTimeMillis(),
                "the score must be the expiry of the session: " + score);
    }

    @Test
    void testAUserRecordedAgainstASessionThatIsGoneIsNotCountedForTheTenant() throws UserSessionException {

        // Nothing holds the session, so recording the user is dropped, and an index member would then
        // count a session no read can report.
        dao.storeUserSessionData(USER_ID, "session-1");

        assertEquals(0L, redis.exists(tenantKey(TENANT_ID)));
        assertEquals(0L, redis.exists(userKey(USER_ID)));
    }

    @Test
    void testASessionHoldsASingleUser() throws UserSessionException {

        // A session belongs to the user it was created for, and only that user refreshes the expiry the
        // sessions of a user are scored by, so a second user is recorded neither on the session nor in
        // an index of their own.
        storeSession("session-1", null);
        dao.storeUserSessionData(USER_ID, "session-1");

        assertThrows(DuplicatedAuthUserException.class,
                () -> dao.storeUserSessionData("user-2", "session-1"));

        assertEquals(USER_ID, string(redis.hget(sessionKey("session-1"), RedisConstants.FIELD_USER_ID)));
        assertEquals(0L, redis.exists(userKey("user-2")),
                "a user the session does not hold is not indexed against it");
        assertFalse(dao.isExistingMapping("user-2", "session-1"));
    }

    @Test
    void testRecordingTheSameUserAgainIsReportedAsADuplicate() throws UserSessionException {

        storeSession("session-1", null);
        dao.storeUserSessionData(USER_ID, "session-1");

        assertThrows(DuplicatedAuthUserException.class,
                () -> dao.storeUserSessionData(USER_ID, "session-1"));
    }

    @Test
    void testAMappingOfALiveSessionIsReportedAsExisting() throws UserSessionException {

        storeSession("session-1");
        indexForUser("session-1");

        assertTrue(dao.isExistingMapping(USER_ID, "session-1"));
    }

    @Test
    void testAnExpiredMappingIsNotReportedAsExisting() throws UserSessionException {

        // The mapping is scored by the expiry of the session, and the by user id reads take the members
        // scored later than now, so a member scored in the past is not a mapping this check can report.
        storeSession("session-1");
        redis.zadd(userKey(USER_ID), (double) (System.currentTimeMillis() - LIVE), bytes("session-1"));

        assertFalse(dao.isExistingMapping(USER_ID, "session-1"));
        assertEquals(List.of(), dao.getSessionId(USER_ID));
    }

    @Test
    void testAnExpiredMappingIsRecordedAgain() throws UserSessionException {

        // A mapping that went stale is not reported as existing, so the login path records it again
        // rather than leaving the session invisible to the by user id reads.
        storeSession("session-1", null);
        dao.storeUserSessionData(USER_ID, "session-1");
        redis.zadd(userKey(USER_ID), (double) (System.currentTimeMillis() - LIVE), bytes("session-1"));

        dao.storeUserSessionData(USER_ID, "session-1");

        assertEquals(List.of("session-1"), dao.getSessionId(USER_ID));
    }

    @Test
    void testASessionStillAvailableIsTerminated() {

        // Every detail of a session is in the session key, so a terminated session leaves nothing of its
        // own to remove. One that is still there was not removed when it was terminated, so it is here.
        storeSession("session-1");
        redis.hset(sessionKey("session-1"), RedisKeyUtils.getMetadataField("IP"), bytes("10.0.0.5"));
        redis.hset(sessionKey("session-1"), RedisKeyUtils.getAppField(3, "oauth2", "alice"), bytes(""));
        indexForUser("session-1");
        indexForTenant("session-1");

        dao.removeTerminatedSessionRecords(List.of("session-1"));

        assertEquals(0L, redis.exists(sessionKey("session-1")),
                "a session that is still available is removed with everything belonging to it");
        assertNull(redis.zscore(userKey(USER_ID), bytes("session-1")),
                "a terminated session is pruned from the sessions of the user");
        assertNull(redis.zscore(tenantKey(TENANT_ID), bytes("session-1")),
                "a terminated session is pruned from the sessions of the tenant");
    }

    @Test
    void testAnEntryHoldingNoSessionIsTerminated() {

        // The writes drop a field whose session is not there, so an entry like this is one an older
        // version of the store left behind. Nothing in it belongs to a session that exists, so a
        // termination clears it rather than leaving it to expire on its own.
        redis.hset(sessionKey("session-1"), RedisConstants.FIELD_USER_ID, bytes(USER_ID));
        redis.hset(sessionKey("session-1"), RedisKeyUtils.getMetadataField("IP"), bytes("10.0.0.5"));
        indexForUser("session-1");

        dao.removeTerminatedSessionRecords(List.of("session-1"));

        assertEquals(0L, redis.exists(sessionKey("session-1")));
        assertNull(redis.zscore(userKey(USER_ID), bytes("session-1")),
                "an entry holding no session is pruned from the sessions of the user");
    }

    @Test
    void testTerminatingASessionThatIsGoneIsNotAnError() {

        dao.removeTerminatedSessionRecords(List.of("session-gone"));

        assertEquals(0L, redis.exists(sessionKey("session-gone")));
    }

    @Test
    void testAnExpiredSessionIsNotReported() throws UserSessionException {

        storeSession("session-1");
        storeSession("session-2");
        indexForUser("session-1");
        // The index is scored by expiry, so a member scored in the past is expired.
        redis.zadd(userKey(USER_ID), (double) (System.currentTimeMillis() - LIVE), bytes("session-2"));

        assertEquals(List.of("session-1"), dao.getSessionId(USER_ID));
        assertEquals(List.of("session-1"), dao.getActiveSessionIds(USER_ID));
    }

    @Test
    void testAUserWithoutSessionsIsReadAsEmpty() throws UserSessionException {

        assertEquals(List.of(), dao.getActiveSessionIds("no-such-user"));
    }

    @Test
    void testTheSessionsOfAUserAreReadInBatches() throws UserSessionException {

        // The configured batch size is smaller than the number of sessions, so the read is pipelined in
        // more than one batch and every batch has to be carried into the result.
        List<String> sessionIds = List.of("session-1", "session-2", "session-3", "session-4",
                "session-5");
        for (String sessionId : sessionIds) {
            storeSession(sessionId);
        }
        indexForUser(sessionIds.toArray(new String[0]));

        assertEquals(sessionIds, dao.getActiveSessionIds(USER_ID));
    }

    @Test
    void testTheReadDegradesWhenRedisIsUnreachable() {

        // An outage must not fail the request with an error the API cannot answer, so the read reports
        // the user as having no sessions.
        RedisSessionContext unreachable = new RedisSessionContext(config()
                .hosts("localhost:1")
                .connectionTimeout(200)
                .commandTimeout(200)
                .build());
        try {
            assertTrue(new RedisUserSessionDAOImpl(unreachable).getActiveSessionIds(USER_ID).isEmpty());
        } catch (UserSessionException e) {
            // The DAO is allowed to report the outage, as long as it does not report a wrong answer.
            assertNotNull(e.getMessage());
        } finally {
            unreachable.close();
        }
    }

    /**
     * Writes a session record the way the session data store does, as the creation time is what marks
     * the hash as holding a session.
     */
    private void storeSession(String sessionId) {

        storeSession(sessionId, USER_ID);
    }

    /**
     * Writes a session record holding the given user, or none when it is {@code null}, which is what a
     * session looks like before the user of it is recorded.
     */
    private void storeSession(String sessionId, String userId) {

        String key = sessionKey(sessionId);
        redis.hset(key, RedisConstants.FIELD_TIME_CREATED,
                bytes(String.valueOf(System.nanoTime())));
        redis.hset(key, RedisConstants.FIELD_TENANT_ID, bytes(String.valueOf(TENANT_ID)));
        redis.hset(key, RedisConstants.FIELD_SESSION_OBJECT, bytes("payload"));
        if (userId != null) {
            redis.hset(key, RedisConstants.FIELD_USER_ID, bytes(userId));
        }
        redis.pexpire(key, LIVE);
    }

    /**
     * Adds sessions to the sessions of the user, scored by expiry as the DAO writes them.
     */
    private void indexForUser(String... sessionIds) {

        double expiry = System.currentTimeMillis() + LIVE;
        for (String sessionId : sessionIds) {
            redis.zadd(userKey(USER_ID), expiry, bytes(sessionId));
        }
    }

    /**
     * Adds sessions to the sessions of the tenant, scored by expiry as the DAO writes them.
     */
    private void indexForTenant(String... sessionIds) {

        double expiry = System.currentTimeMillis() + LIVE;
        for (String sessionId : sessionIds) {
            redis.zadd(tenantKey(TENANT_ID), expiry, bytes(sessionId));
        }
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

    private static String string(byte[] value) {

        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }
}
