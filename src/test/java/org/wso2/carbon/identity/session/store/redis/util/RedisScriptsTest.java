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

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Runs the scripts of the store against a Redis server, since the guarantees they hold cannot be
 * verified without one.
 * <p>
 * Skipped when no server is reachable. The server can be given with
 * {@code -Dredis.test.uri=redis://host:port} and defaults to port 6380, so a test run cannot reach a
 * deployed server. One can be started with {@code docker run -d --rm -p 6380:6379 redis:7-alpine}.
 */
public class RedisScriptsTest {

    private static final String DEFAULT_URI = "redis://localhost:6380";
    private static final RedisCodec<String, byte[]> CODEC =
            RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE);

    private static final String SESSION_KEY = "idn:s:AppAuthFrameworkSessionContextCache:test-session";
    private static final String USER_KEY = "idn:u:test-user";
    private static final String TENANT_KEY = "idn:tid:1";
    private static final String SET_KEY = "idn:fed:s:test-session";
    private static final String MAPPING_KEY = "idn:fed:1:7:test-idp-session";
    private static final long EXPIRY = 60000L;
    private static final long CREATED = RedisConstants.STORE_STATUS_CREATED;
    private static final long EXTENDED = RedisConstants.STORE_STATUS_EXTENDED;

    // Nanosecond values, so the comparison is exercised at the magnitude it sees at runtime.
    private static final long TIME_1 = 1754899200123456789L;
    private static final long TIME_2 = TIME_1 + 1000L;

    private static RedisClient client;
    private static StatefulRedisConnection<String, byte[]> connection;
    private static RedisCommands<String, byte[]> redis;

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

        redis.del(SESSION_KEY, USER_KEY, TENANT_KEY, SET_KEY, MAPPING_KEY);
    }

    @Test
    void testStoreSession() {

        assertEquals(1L, storeSession(TIME_1));

        Map<String, byte[]> hash = redis.hgetall(SESSION_KEY);
        assertEquals(String.valueOf(TIME_1), toString(hash.get(RedisConstants.FIELD_TIME_CREATED)));
        assertEquals("1", toString(hash.get(RedisConstants.FIELD_TENANT_ID)));
        assertEquals("payload", toString(hash.get(RedisConstants.FIELD_SESSION_OBJECT)));
        assertTrue(redis.pttl(SESSION_KEY) > 0);
    }

    @Test
    void testNewerStoreReplacesTheSession() {

        storeSession(TIME_1);

        assertEquals(EXTENDED, storeSession(TIME_2));
        assertEquals(String.valueOf(TIME_2), toString(redis.hget(SESSION_KEY,
                RedisConstants.FIELD_TIME_CREATED)));
    }

    @Test
    void testOlderStoreIsRejected() {

        storeSession(TIME_2);

        assertEquals(0L, storeSession(TIME_1));
        assertEquals(String.valueOf(TIME_2), toString(redis.hget(SESSION_KEY,
                RedisConstants.FIELD_TIME_CREATED)));
    }

    @Test
    void testStoreWithTheSameTimeIsApplied() {

        storeSession(TIME_1);

        assertEquals(EXTENDED, storeSession(TIME_1));
    }

    @Test
    void testAFirstStoreAndAnExtensionAreToldApart() {

        // Only an extension has indexes and mappings scored by an earlier expiry, so the caller has to be
        // able to tell the two apart without reading the session again.
        assertEquals(CREATED, storeSession(TIME_1));

        assertEquals(EXTENDED, storeSession(TIME_2));
    }

    @Test
    void testAFirstStoreReportsNoUserAndNoFederatedMapping() {

        // A session stored for the first time can hold neither, so the script does not go looking.
        List<Object> reply = storeSessionReply(TIME_1);

        assertEquals(String.valueOf(CREATED), RedisValueUtils.toString(reply.get(0)));
        assertEquals("", RedisValueUtils.toString(reply.get(1)));
        assertTrue(((List<?>) reply.get(2)).isEmpty());
    }

    @Test
    void testStoreSessionReturnsTheFederatedMappingsOfTheSession() {

        // The mappings are fields of the session, so the store reads them in the call that extends it and
        // the caller needs no read of its own to raise their expiry.
        storeSession(TIME_1);
        redis.hset(SESSION_KEY, RedisKeyUtils.getPrefixedFederatedField(1, 7, "idp-session-1"), toBytes(""));
        redis.hset(SESSION_KEY, RedisKeyUtils.getMetadataField("IP"), toBytes("10.0.0.5"));

        List<Object> reply = storeSessionReply(TIME_2);

        assertEquals(List.of("1:7:idp-session-1"), members(reply.get(2)));
    }

    @Test
    void testComparisonIsExactForNanosecondValues() {

        // A numeric comparison in Lua would lose this difference, as its numbers are doubles.
        storeSession(TIME_1 + 1);

        assertEquals(0L, storeSession(TIME_1));
        assertEquals(String.valueOf(TIME_1 + 1), toString(redis.hget(SESSION_KEY,
                RedisConstants.FIELD_TIME_CREATED)));
    }

    @Test
    void testComparisonHandlesDifferentDigitCounts() {

        storeSession(1000000000000000000L);

        assertEquals(0L, storeSession(999999999999999999L));
    }

    @Test
    void testDeleteSessionReturnsItsUserTenantAndFederatedMappings() {

        storeSession(TIME_1);
        redis.hset(SESSION_KEY, RedisConstants.FIELD_USER_ID, toBytes("user-1"));
        redis.hset(SESSION_KEY, RedisKeyUtils.getMetadataField("IP"), toBytes("10.0.0.5"));
        redis.hset(SESSION_KEY, RedisKeyUtils.getPrefixedFederatedField(1, 7, "idp-session-1"), toBytes(""));

        List<Object> result = deleteSession();

        assertEquals("user-1", toString((byte[]) result.get(0)));
        assertEquals("1", toString((byte[]) result.get(1)));
        // Read before the deletion, since afterwards there is nothing left to read it from.
        assertEquals(List.of("1:7:idp-session-1"), members(result.get(2)));
        assertEquals(0L, redis.exists(SESSION_KEY));
    }

    @Test
    void testDeleteSessionThatDoesNotExist() {

        List<Object> result = deleteSession();

        assertEquals(3, result.size());
        assertEquals("", toString((byte[]) result.get(0)));
        assertEquals("", toString((byte[]) result.get(1)));
        assertTrue(((List<?>) result.get(2)).isEmpty());
    }

    @Test
    void testDeleteSessionIsIdempotent() {

        storeSession(TIME_1);

        assertNotNull(deleteSession());
        assertNotNull(deleteSession());
        assertEquals(0L, redis.exists(SESSION_KEY));
    }

    @Test
    void testSetFieldsKeepsTheSessionExpiry() {

        storeSession(TIME_1);
        long before = redis.pttl(SESSION_KEY);

        assertEquals(1L, setField("m:IP", "10.0.0.5"));

        long after = redis.pttl(SESSION_KEY);
        assertTrue(after > before - 2000 && after <= before + 10);
        assertEquals("10.0.0.5", toString(redis.hget(SESSION_KEY, "m:IP")));
    }

    @Test
    void testSetFieldsDropsTheFieldsWhenTheSessionIsNotThere() {

        // The callers store the session first, so an entry without one holds a session that is gone, and
        // writing the fields would recreate it as a hash that no session ever fills.
        assertEquals(0L, setField("m:IP", "10.0.0.5"));

        assertEquals(0L, redis.exists(SESSION_KEY));
    }

    @Test
    void testSetFieldsDropsTheFieldsWhenOnlyTheUserIsThere() {

        // A user written against a session that is gone leaves a hash behind, which is still not a session.
        redis.hset(SESSION_KEY, RedisConstants.FIELD_USER_ID, toBytes("user-1"));

        assertEquals(0L, setField("m:IP", "10.0.0.5"));

        assertFalse(redis.hexists(SESSION_KEY, "m:IP"));
    }

    @Test
    void testSetSeveralFields() {

        storeSession(TIME_1);
        Long result = evaluate(RedisScripts.SET_FIELDS, ScriptOutputType.INTEGER, SESSION_KEY,
                toBytes("m:IP"), toBytes("10.0.0.5"),
                toBytes("m:User Agent"), toBytes("Mozilla/5.0"));

        assertEquals(1L, result);
        assertEquals("10.0.0.5", toString(redis.hget(SESSION_KEY, "m:IP")));
        assertEquals("Mozilla/5.0", toString(redis.hget(SESSION_KEY, "m:User Agent")));
    }

    @Test
    void testSetSessionUser() {

        storeSession(TIME_1);

        assertEquals("user-1", setUser("user-1"));
        assertEquals("user-1", toString(redis.hget(SESSION_KEY, RedisConstants.FIELD_USER_ID)));

        // Setting the same user again leaves the single user of the session as it is.
        assertEquals("user-1", setUser("user-1"));
        assertEquals("user-1", toString(redis.hget(SESSION_KEY, RedisConstants.FIELD_USER_ID)));
    }

    @Test
    void testSetSessionUserKeepsTheUserTheSessionHolds() {

        storeSession(TIME_1);
        setUser("user-1");

        // The reply is the user of the session, so a caller can tell that its own user was not recorded.
        assertEquals("user-1", setUser("user-2"));
        assertEquals("user-1", toString(redis.hget(SESSION_KEY, RedisConstants.FIELD_USER_ID)));
    }

    @Test
    void testSetSessionUserOverAnEmptyValue() {

        storeSession(TIME_1);
        redis.hset(SESSION_KEY, RedisConstants.FIELD_USER_ID, toBytes(""));

        assertEquals("user-1", setUser("user-1"));
        assertEquals("user-1", toString(redis.hget(SESSION_KEY, RedisConstants.FIELD_USER_ID)));
    }

    @Test
    void testSetSessionUserDropsTheUserWhenTheSessionIsNotThere() {

        // The session is always stored before its user is recorded, so an entry without one holds a
        // session that is gone, and writing the user would leave a hash no session ever fills.
        assertEquals("", RedisValueUtils.toString(setUserReply("user-1").get(0)));

        assertEquals(0L, redis.exists(SESSION_KEY));
    }

    @Test
    void testSetSessionUserReturnsTheTenantOfTheSession() {

        storeSession(TIME_1);

        // The caller adds the session to the index of this tenant, which is what the active session count
        // is answered from, so it comes back with the user rather than with a read of its own.
        assertEquals("1", RedisValueUtils.toString(setUserReply("user-1").get(2)));
    }

    @Test
    void testSetSessionUserReportsNoTenantWhenTheSessionIsNotThere() {

        List<Object> reply = setUserReply("user-1");

        assertEquals(3, reply.size());
        assertEquals("", RedisValueUtils.toString(reply.get(2)));
    }

    @Test
    void testSetSessionUserReturnsTheExpiryOfTheSession() {

        storeSession(TIME_1);

        long expiry = RedisValueUtils.toLong(setUserReply("user-1").get(1));

        // The caller scores the sessions of the user by this, so it is the expiry of the session itself.
        assertTrue(expiry > 0 && expiry <= EXPIRY, "the expiry of the session was returned: " + expiry);
        assertTrue(expiry > EXPIRY - 2000, "the expiry of the session was returned: " + expiry);
    }

    @Test
    void testSetSessionUserKeepsTheSessionExpiry() {

        storeSession(TIME_1);
        long before = redis.pttl(SESSION_KEY);

        setUser("user-1");

        long after = redis.pttl(SESSION_KEY);
        assertTrue(after > before - 2000 && after <= before + 10);
    }

    @Test
    void testAddUserSession() {

        long now = System.currentTimeMillis();

        assertEquals(1L, addUserSession(now, now + EXPIRY, "session-1"));
        assertNotNull(redis.zscore(USER_KEY, toBytes("session-1")));
        assertTrue(redis.pttl(USER_KEY) > 0);
    }

    @Test
    void testAddUserSessionReportsAnExistingMapping() {

        long now = System.currentTimeMillis();

        assertEquals(1L, addUserSession(now, now + EXPIRY, "session-1"));
        assertEquals(0L, addUserSession(now, now + EXPIRY, "session-1"));
    }

    @Test
    void testAddUserSessionRemovesTheExpiredOnes() {

        long now = System.currentTimeMillis();
        redis.zadd(USER_KEY, (double) (now - EXPIRY), toBytes("expired-session"));

        addUserSession(now, now + EXPIRY, "session-1");

        assertNull(redis.zscore(USER_KEY, toBytes("expired-session")));
        assertNotNull(redis.zscore(USER_KEY, toBytes("session-1")));
    }

    @Test
    void testAnExpiredMappingIsNotReportedAsExisting() {

        long now = System.currentTimeMillis();
        redis.zadd(USER_KEY, (double) (now - EXPIRY), toBytes("session-1"));

        assertEquals(1L, addUserSession(now, now + EXPIRY, "session-1"));
    }

    @Test
    void testStoreSessionReturnsTheUserOfTheSession() {

        storeSession(TIME_1);
        setUser("user-1");

        List<Object> reply = storeSessionReply(TIME_2);

        assertEquals(String.valueOf(EXTENDED), RedisValueUtils.toString(reply.get(0)));
        assertEquals("user-1", RedisValueUtils.toString(reply.get(1)));
    }

    @Test
    void testStoreSessionReturnsNoUserWhenItIsRejected() {

        storeSession(TIME_2);
        setUser("user-1");

        List<Object> reply = storeSessionReply(TIME_1);

        assertEquals("0", RedisValueUtils.toString(reply.get(0)));
        assertEquals("", RedisValueUtils.toString(reply.get(1)));
    }

    @Test
    void testRefreshRaisesTheScoreOfAnExtendedSession() {

        long now = System.currentTimeMillis();
        addUserSession(now, now + EXPIRY, "session-1");

        assertEquals(1L, refreshUserSession(now, now + 2 * EXPIRY, "session-1"));

        assertEquals((double) (now + 2 * EXPIRY), redis.zscore(USER_KEY, toBytes("session-1")));
    }

    @Test
    void testRefreshKeepsASessionThatIsAlreadyScoredAsExpired() {

        // The score is written once, when the mapping is created, so a session extended past it is scored
        // as expired while it is still live. The refresh has to rescue that member rather than trim it.
        long now = System.currentTimeMillis();
        redis.zadd(USER_KEY, (double) (now - EXPIRY), toBytes("session-1"));

        assertEquals(1L, refreshUserSession(now, now + EXPIRY, "session-1"));

        assertEquals((double) (now + EXPIRY), redis.zscore(USER_KEY, toBytes("session-1")));
    }

    @Test
    void testRefreshDoesNotAddASessionThatIsNotIndexed() {

        // The mapping is created by the DAO and removed by a logout, so a store must not create one.
        long now = System.currentTimeMillis();

        assertEquals(0L, refreshUserSession(now, now + EXPIRY, "session-1"));

        assertNull(redis.zscore(USER_KEY, toBytes("session-1")));
        assertEquals(0L, redis.exists(USER_KEY));
    }

    @Test
    void testRefreshNeverLowersTheScore() {

        // Stores from two nodes are not ordered, so an earlier expiry can arrive last.
        long now = System.currentTimeMillis();
        addUserSession(now, now + 2 * EXPIRY, "session-1");

        refreshUserSession(now, now + EXPIRY, "session-1");

        assertEquals((double) (now + 2 * EXPIRY), redis.zscore(USER_KEY, toBytes("session-1")));
    }

    @Test
    void testRefreshTrimsTheExpiredMembersAndExtendsTheIndex() {

        long now = System.currentTimeMillis();
        addUserSession(now, now + EXPIRY, "session-1");
        redis.zadd(USER_KEY, (double) (now - EXPIRY), toBytes("expired-session"));

        refreshUserSession(now, now + 2 * EXPIRY, "session-1");

        assertNull(redis.zscore(USER_KEY, toBytes("expired-session")));
        long ttl = redis.pttl(USER_KEY);
        assertTrue(ttl > EXPIRY, "the index must live as long as the session it was extended for: " + ttl);
    }

    @Test
    void testRemoveUserSessionDropsAnEmptyKey() {

        long now = System.currentTimeMillis();
        addUserSession(now, now + EXPIRY, "session-1");

        Long removed = evaluate(RedisScripts.REMOVE_INDEXED_SESSION, ScriptOutputType.INTEGER, USER_KEY,
                toBytes("session-1"));

        assertEquals(1L, removed);
        assertEquals(0L, redis.exists(USER_KEY));
    }

    @Test
    void testExtendExpiryDoesNotCreateAKeyThatIsGone() {

        // The mappings of a session expire on their own, so an extension can arrive after one is gone.
        // PEXPIRE would answer 0 for it, but the guard keeps the intent in the script rather than in Redis.
        assertEquals(0L, extendExpiry(SESSION_KEY, EXPIRY));

        assertEquals(0L, redis.exists(SESSION_KEY));
    }

    @Test
    void testExtendExpiryRaisesAnExpiryAndNeverLowersIt() {

        storeSession(TIME_1);

        assertEquals(1L, extendExpiry(SESSION_KEY, 2 * EXPIRY));
        assertTrue(redis.pttl(SESSION_KEY) > EXPIRY);

        assertEquals(0L, extendExpiry(SESSION_KEY, EXPIRY));
        assertTrue(redis.pttl(SESSION_KEY) > EXPIRY);
    }

    @Test
    void testExtendExpiryIgnoresANonPositiveExpiry() {

        // PEXPIRE with a non-positive value deletes the key it is applied to.
        storeSession(TIME_1);

        assertEquals(0L, extendExpiry(SESSION_KEY, 0));

        assertEquals(1L, redis.exists(SESSION_KEY));
        assertTrue(redis.pttl(SESSION_KEY) > 0);
    }

    @Test
    void testExtendExpiryGivesOneToAKeyCarryingNone() {

        // A key holding no expiry is a leak, so the extension applies one rather than leaving it.
        redis.hset(SESSION_KEY, RedisConstants.FIELD_USER_ID, toBytes("user-1"));

        assertEquals(1L, extendExpiry(SESSION_KEY, EXPIRY));

        assertTrue(redis.pttl(SESSION_KEY) > 0);
    }

    @Test
    void testAddSetMemberDoesNotLowerTheExpiry() {

        // A reverse index must outlive every mapping it holds, so a shorter expiry does not replace it.
        evaluate(RedisScripts.ADD_SET_MEMBER, ScriptOutputType.INTEGER, SET_KEY, toBytes("member-1"),
                toBytes("60000"));
        evaluate(RedisScripts.ADD_SET_MEMBER, ScriptOutputType.INTEGER, SET_KEY, toBytes("member-2"),
                toBytes("1000"));

        assertTrue(redis.pttl(SET_KEY) > 50000L);
        assertEquals(2L, redis.scard(SET_KEY));
    }

    @Test
    void testScriptIsSentAgainWhenTheServerDoesNotHaveIt() throws Exception {

        // A script cache is not persisted or replicated, so it is empty after a restart, on a promoted
        // replica and on a cluster node that has not run the script.
        RedisTemplate template = new RedisTemplate(new RedisConnectionManager() {
            @Override
            public RedisClusterCommands<String, byte[]> getCommands() {

                return redis;
            }
        }, 2000L);

        redis.scriptFlush();
        List<Object> first = template.executeScript(RedisScripts.STORE_SESSION, ScriptOutputType.MULTI,
                SESSION_KEY, toBytes(String.valueOf(TIME_1)), toBytes("1"), toBytes("payload"),
                toBytes(String.valueOf(EXPIRY)));
        assertEquals(String.valueOf(CREATED), RedisValueUtils.toString(first.get(0)));

        List<Object> second = template.executeScript(RedisScripts.STORE_SESSION, ScriptOutputType.MULTI,
                SESSION_KEY, toBytes(String.valueOf(TIME_2)), toBytes("1"), toBytes("payload"),
                toBytes(String.valueOf(EXPIRY)));
        assertEquals(String.valueOf(EXTENDED), RedisValueUtils.toString(second.get(0)));
        assertEquals(String.valueOf(TIME_2), toString(redis.hget(SESSION_KEY,
                RedisConstants.FIELD_TIME_CREATED)));
    }

    @Test
    void testAddTenantSessionExpiresTheIndex() {

        // The tenant index used to be scored by creation time and never expired, so it kept one dead
        // member per session ever created. Scored by expiry it can be trimmed, and the key itself
        // expires with the last session in it.
        long now = System.currentTimeMillis();

        assertEquals(1L, addTenantSession(now, now + EXPIRY, "session-1"));

        assertNotNull(redis.zscore(TENANT_KEY, toBytes("session-1")));
        long ttl = redis.pttl(TENANT_KEY);
        assertTrue(ttl > 0 && ttl <= EXPIRY, "the tenant index must expire with its last session: " + ttl);
    }

    @Test
    void testAddTenantSessionRemovesTheExpiredOnes() {

        long now = System.currentTimeMillis();
        redis.zadd(TENANT_KEY, (double) (now - EXPIRY), toBytes("expired-session"));

        addTenantSession(now, now + EXPIRY, "session-1");

        assertNull(redis.zscore(TENANT_KEY, toBytes("expired-session")));
        assertNotNull(redis.zscore(TENANT_KEY, toBytes("session-1")));
    }

    @Test
    void testAddTenantSessionUpdatesTheScoreOfAReStoredSession() {

        // Unlike the user index, a session can be stored again with a later expiry, and the index has to
        // follow it or the session would be trimmed while it is still live.
        long now = System.currentTimeMillis();
        addTenantSession(now, now + EXPIRY, "session-1");

        addTenantSession(now, now + EXPIRY * 2, "session-1");

        assertEquals(1L, redis.zcard(TENANT_KEY));
        assertEquals((double) (now + EXPIRY * 2), redis.zscore(TENANT_KEY, toBytes("session-1")));
        assertTrue(redis.pttl(TENANT_KEY) > EXPIRY);
    }

    @Test
    void testAddTenantSessionNeverLowersTheScore() {

        // Stores from two nodes are not ordered, so an earlier expiry can arrive last, and applying it
        // would trim a live session out of the listing of its tenant.
        long now = System.currentTimeMillis();
        addTenantSession(now, now + 2 * EXPIRY, "session-1");

        addTenantSession(now, now + EXPIRY, "session-1");

        assertEquals((double) (now + 2 * EXPIRY), redis.zscore(TENANT_KEY, toBytes("session-1")));
    }

    @Test
    void testTenantIndexIsDroppedWhenEveryMemberHasExpired() {

        long now = System.currentTimeMillis();
        redis.zadd(TENANT_KEY, (double) (now - EXPIRY), toBytes("expired-session"));

        addTenantSession(now, now - 1, "also-expired");

        assertEquals(0L, redis.exists(TENANT_KEY));
    }

    @Test
    void testAddSetMemberIgnoresANonPositiveExpiry() {

        // PEXPIRE with a non-positive value deletes the key. The set holds the mappings of other, still
        // live sessions, so applying one would take those with it.
        evaluate(RedisScripts.ADD_SET_MEMBER, ScriptOutputType.INTEGER, SET_KEY, toBytes("member-1"),
                toBytes("60000"));

        evaluate(RedisScripts.ADD_SET_MEMBER, ScriptOutputType.INTEGER, SET_KEY, toBytes("member-2"),
                toBytes("0"));

        assertEquals(2L, redis.scard(SET_KEY));
        assertTrue(redis.pttl(SET_KEY) > 0);
    }

    @Test
    void testAddSetMemberWithANonPositiveExpiryOnASetWithoutOne() {

        // The set has no expiry of its own here, which used to take the branch that applied the given one
        // and so deleted the whole set.
        redis.sadd(SET_KEY, toBytes("member-1"));

        evaluate(RedisScripts.ADD_SET_MEMBER, ScriptOutputType.INTEGER, SET_KEY, toBytes("member-2"),
                toBytes("-1"));

        assertEquals(2L, redis.scard(SET_KEY));
    }

    @Test
    void testStoreFederatedMappingAppliesItsExpiry() {

        Long result = evaluate(RedisScripts.STORE_FEDERATED_MAPPING, ScriptOutputType.INTEGER, MAPPING_KEY,
                toBytes(String.valueOf(EXPIRY)),
                toBytes(RedisConstants.FIELD_FED_SESSION_ID), toBytes("test-session"),
                toBytes(RedisConstants.FIELD_FED_IDP_NAME), toBytes("google"));

        assertEquals(1L, result);
        assertEquals("test-session", toString(redis.hget(MAPPING_KEY,
                RedisConstants.FIELD_FED_SESSION_ID)));
        assertEquals("google", toString(redis.hget(MAPPING_KEY, RedisConstants.FIELD_FED_IDP_NAME)));
        long ttl = redis.pttl(MAPPING_KEY);
        assertTrue(ttl > 0 && ttl <= EXPIRY, "the mapping must carry an expiry: " + ttl);
    }

    @Test
    void testStoreFederatedMappingIgnoresANonPositiveExpiry() {

        // A non-positive expiry would delete the mapping that was just written.
        evaluate(RedisScripts.STORE_FEDERATED_MAPPING, ScriptOutputType.INTEGER, MAPPING_KEY,
                toBytes("0"),
                toBytes(RedisConstants.FIELD_FED_SESSION_ID), toBytes("test-session"));

        assertEquals(1L, redis.exists(MAPPING_KEY));
    }

    private static List<String> members(Object reply) {

        List<String> members = new ArrayList<>();
        for (Object member : (List<?>) reply) {
            members.add(RedisValueUtils.toString(member));
        }
        return members;
    }

    private Long extendExpiry(String key, long expiry) {

        return evaluate(RedisScripts.EXTEND_EXPIRY, ScriptOutputType.INTEGER, key,
                toBytes(String.valueOf(expiry)));
    }

    private Long addTenantSession(long now, long expiry, String sessionId) {

        return evaluate(RedisScripts.ADD_TENANT_SESSION, ScriptOutputType.INTEGER, TENANT_KEY,
                toBytes(String.valueOf(now)), toBytes(String.valueOf(expiry)), toBytes(sessionId));
    }

    private Long storeSession(long timeCreated) {

        return Long.valueOf(RedisValueUtils.toString(storeSessionReply(timeCreated).get(0)));
    }

    private List<Object> storeSessionReply(long timeCreated) {

        return evaluate(RedisScripts.STORE_SESSION, ScriptOutputType.MULTI, SESSION_KEY,
                toBytes(String.valueOf(timeCreated)), toBytes("1"), toBytes("payload"),
                toBytes(String.valueOf(EXPIRY)));
    }

    private Long refreshUserSession(long now, long expiry, String sessionId) {

        return evaluate(RedisScripts.REFRESH_INDEXED_SESSION, ScriptOutputType.INTEGER, USER_KEY,
                toBytes(String.valueOf(now)), toBytes(String.valueOf(expiry)), toBytes(sessionId));
    }

    private List<Object> deleteSession() {

        return evaluate(RedisScripts.DELETE_SESSION, ScriptOutputType.MULTI, SESSION_KEY);
    }

    private Long setField(String field, String value) {

        return evaluate(RedisScripts.SET_FIELDS, ScriptOutputType.INTEGER, SESSION_KEY,
                toBytes(field), toBytes(value));
    }

    private String setUser(String userId) {

        return RedisValueUtils.toString(setUserReply(userId).get(0));
    }

    private List<Object> setUserReply(String userId) {

        return evaluate(RedisScripts.SET_SESSION_USER, ScriptOutputType.MULTI, SESSION_KEY,
                toBytes(userId));
    }

    private Long addUserSession(long now, long expiry, String sessionId) {

        return evaluate(RedisScripts.ADD_USER_SESSION, ScriptOutputType.INTEGER, USER_KEY,
                toBytes(String.valueOf(now)), toBytes(String.valueOf(expiry)), toBytes(sessionId));
    }

    private <T> T evaluate(String script, ScriptOutputType output, String key, byte[]... args) {

        return redis.eval(script, output, new String[]{key}, args);
    }

    private static byte[] toBytes(String value) {

        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String toString(byte[] value) {

        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }
}
