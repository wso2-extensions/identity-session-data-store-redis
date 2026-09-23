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

/**
 * Lua scripts of the Redis session store, in the role {@code SQLQueries} plays for the relational
 * store. Each script is atomic and touches a single key, so all of them are safe in cluster mode.
 */
public class RedisScripts {

    /**
     * Compares two decimal integer strings. A string comparison, since a Lua number is a double and
     * would lose precision on a nanosecond creation time.
     */
    private static final String NEWER_FUNCTION =
            "local function newer(a, b) "
                    + "if #a ~= #b then return #a > #b end "
                    + "return a > b "
                    + "end ";

    /**
     * Collects the federated mapping members a session holds, which are carried as the field names of the
     * session itself. Defines {@code federatedMembers(key)} for the scripts that report them.
     */
    private static final String FEDERATED_MEMBERS_FUNCTION =
            "local function federatedMembers(key) "
                    + "local prefix = '" + RedisConstants.FIELD_PREFIX_FEDERATED + "' "
                    + "local width = #prefix "
                    + "local members = {} "
                    + "for _, field in ipairs(redis.call('HKEYS', key)) do "
                    + "if string.sub(field, 1, width) == prefix then "
                    + "members[#members + 1] = string.sub(field, width + 1) "
                    + "end "
                    + "end "
                    + "return members "
                    + "end ";

    /**
     * Stores a session, unless a newer store has already been applied, and returns what the session held
     * before it: the user, and the federated mappings that belong to it.
     * <p>
     * KEYS: session key.
     * ARGV: creation time, tenant id, session object, expiry in milliseconds.
     * Returns the status, the user identifier and the federated mapping members. The status is
     * {@value RedisConstants#STORE_STATUS_CREATED} when the session was not there,
     * {@value RedisConstants#STORE_STATUS_EXTENDED} when it was, and
     * {@value RedisConstants#STORE_STATUS_STALE} when a newer store was found. Nothing but the status is
     * reported for a session stored for the first time, which can hold neither a user nor a mapping.
     * The result is processed at {&#064;SessionStoreResult }.
     */
    public static final String STORE_SESSION =
            NEWER_FUNCTION
                    + FEDERATED_MEMBERS_FUNCTION
                    + "local current = redis.call('HGET', KEYS[1], '" + RedisConstants.FIELD_TIME_CREATED + "') "
                    + "if current and newer(current, ARGV[1]) then return {"
                    + RedisConstants.STORE_STATUS_STALE + ", '', {}} end "
                    + "local status = " + RedisConstants.STORE_STATUS_CREATED + " "
                    + "local user = '' "
                    + "local members = {} "
                    + "if current then "
                    + "status = " + RedisConstants.STORE_STATUS_EXTENDED + " "
                    + "user = redis.call('HGET', KEYS[1], '" + RedisConstants.FIELD_USER_ID + "') or '' "
                    + "members = federatedMembers(KEYS[1]) "
                    + "end "
                    + "redis.call('HSET', KEYS[1], "
                    + "'" + RedisConstants.FIELD_TIME_CREATED + "', ARGV[1], "
                    + "'" + RedisConstants.FIELD_TENANT_ID + "', ARGV[2], "
                    + "'" + RedisConstants.FIELD_SESSION_OBJECT + "', ARGV[3]) "
                    + "redis.call('PEXPIRE', KEYS[1], ARGV[4]) "
                    + "return {status, user, members}";

    /**
     * Deletes a session and returns everything that has to be cleaned up after it: the user and the
     * tenant whose indexes referred to it, and the federated mappings that belonged to it. All of it is
     * read before the deletion, since afterwards there is nothing left to read it from.
     * <p>
     * KEYS: session key. Returns the user identifier, the tenant id and the federated mapping members,
     * with the first two empty when the entry held none.
     */
    public static final String DELETE_SESSION =
            FEDERATED_MEMBERS_FUNCTION
                    + "local user = redis.call('HGET', KEYS[1], '" + RedisConstants.FIELD_USER_ID + "') "
                    + "local tenantId = redis.call('HGET', KEYS[1], '"
                    + RedisConstants.FIELD_TENANT_ID + "') "
                    + "local members = federatedMembers(KEYS[1]) "
                    + "redis.call('UNLINK', KEYS[1]) "
                    + "return {user or '', tenantId or '', members}";

    /**
     * Sets metadata or application fields without changing the expiry of the session, and drops them when
     * the session itself is not there. The callers store the session before the fields belonging to it,
     * so an entry without one holds a session that has expired or been removed.
     * <p>
     * KEYS: session key. ARGV: at least one field and value pair.
     * Returns 1 when the fields were written and 0 when the session was not there.
     */
    public static final String SET_FIELDS =
            "if redis.call('HEXISTS', KEYS[1], '"
                    + RedisConstants.FIELD_TIME_CREATED + "') == 0 then return 0 end "
                    + "redis.call('HSET', KEYS[1], unpack(ARGV)) "
                    + "return 1";

    /**
     * Records a federated mapping on the session it belongs to. The mapping is dropped when the session is not there.
     * <p>
     * KEYS: session key. (<prefix>:s:<type>:<sid>)
     * ARGV: federated field name. (f:<tenantId>:<idpId>:<idpSessionId>)
     * Returns the remaining expiry of the session in milliseconds, and 0 when the session is not there or
     * carries no expiry of its own.
     */
    public static final String ATTACH_FEDERATED_MAPPING =
            "if redis.call('HEXISTS', KEYS[1], '"
                    + RedisConstants.FIELD_TIME_CREATED + "') == 0 then return 0 end "
                    + "local expiry = redis.call('PTTL', KEYS[1]) "
                    + "if expiry <= 0 then return 0 end "
                    + "redis.call('HSET', KEYS[1], ARGV[1], '') "
                    + "return expiry";

    /**
     * Records the user of a session, which a session holds only one of, and drops the user when the
     * session itself is not there. The expiry and the tenant of the session are returned with the user,
     * so the caller scores the indexes the session belongs to without a read of its own.
     * <p>
     * KEYS: session key. ARGV: user id.
     * Returns the user identifier of the session, its remaining expiry in milliseconds and its tenant,
     * and an empty user when the session was not there.
     */
    public static final String SET_SESSION_USER =
            "if redis.call('HEXISTS', KEYS[1], '"
                    + RedisConstants.FIELD_TIME_CREATED + "') == 0 then return {'', 0, ''} end "
                    + "local current = redis.call('HGET', KEYS[1], '" + RedisConstants.FIELD_USER_ID + "') "
                    + "if not current or current == '' then "
                    + "redis.call('HSET', KEYS[1], '" + RedisConstants.FIELD_USER_ID + "', ARGV[1]) "
                    + "current = ARGV[1] "
                    + "end "
                    + "return {current, redis.call('PTTL', KEYS[1]), "
                    + "redis.call('HGET', KEYS[1], '" + RedisConstants.FIELD_TENANT_ID + "') or ''}";

    /**
     * Trims the members that have expired, which is the whole cleanup an expiry scored index needs.
     * Applied before an addition, so a stale member cannot be reported as a duplicate.
     */
    private static final String TRIM_EXPIRED_MEMBERS =
            "redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1]) ";

    /**
     * Expires the index at the latest expiry it holds, since a sorted set member cannot carry one of its
     * own, and drops it when nothing in it can still be live.
     */
    private static final String EXPIRE_INDEX_AT_LATEST_MEMBER =
            "local latest = redis.call('ZRANGE', KEYS[1], -1, -1, 'WITHSCORES') "
                    + "if not latest[2] then "
                    + "redis.call('UNLINK', KEYS[1]) "
                    + "else "
                    + "local remaining = tonumber(latest[2]) - tonumber(ARGV[1]) "
                    + "if remaining > 0 then redis.call('PEXPIRE', KEYS[1], math.ceil(remaining)) "
                    + "else redis.call('UNLINK', KEYS[1]) end "
                    + "end ";

    /**
     * Adds a session to the sessions of a user, trimming the expired ones.
     * <p>
     * KEYS: user key. ARGV: current time, session expiry, session id. Returns 1 when the session was
     * added and 0 when the mapping already existed.
     */
    public static final String ADD_USER_SESSION =
            TRIM_EXPIRED_MEMBERS
                    + "local added = redis.call('ZADD', KEYS[1], 'NX', ARGV[2], ARGV[3]) "
                    + EXPIRE_INDEX_AT_LATEST_MEMBER
                    + "return added";

    /**
     * Adds a session to the sessions of its tenant, trimming the expired ones. The score is the expiry of
     * the session, so the index holds only sessions that can still be live, and a session that is
     * extended is rescored through {@link #REFRESH_INDEXED_SESSION}. The score is never lowered, so a
     * store that is applied out of order cannot shorten the window a live session is counted for.
     * <p>
     * KEYS: tenant key. ARGV: current time, session expiry, session id.
     */
    public static final String ADD_TENANT_SESSION =
            TRIM_EXPIRED_MEMBERS
                    + "local current = redis.call('ZSCORE', KEYS[1], ARGV[3]) "
                    + "if not current or tonumber(current) < tonumber(ARGV[2]) then "
                    + "redis.call('ZADD', KEYS[1], ARGV[2], ARGV[3]) "
                    + "end "
                    + EXPIRE_INDEX_AT_LATEST_MEMBER
                    + "return 1";

    /**
     * Raises the expiry a session is scored by in an index, so that a session which is extended is not
     * trimmed from an index while it is still live. A member that is not there is never added, since it
     * was removed by a logout and a store must not bring it back, and the score is never lowered, so a
     * store that is applied out of order cannot shorten the window a live session is visible for.
     * <p>
     * KEYS: index key. ARGV: current time, session expiry, session id. Returns 1 when the session was in
     * the index and 0 when it was not.
     */
    public static final String REFRESH_INDEXED_SESSION =
            "local current = redis.call('ZSCORE', KEYS[1], ARGV[3]) "
                    + "if not current then return 0 end "
                    + "if tonumber(current) < tonumber(ARGV[2]) then "
                    + "redis.call('ZADD', KEYS[1], ARGV[2], ARGV[3]) "
                    + "end "
                    // Trimmed after the refresh rather than before it, so the member being refreshed is
                    // not removed by the very score the refresh is replacing.
                    + TRIM_EXPIRED_MEMBERS
                    + EXPIRE_INDEX_AT_LATEST_MEMBER
                    + "return 1";

    /**
     * Removes a session from an expiry scored index and drops the key once it is empty.
     * <p>
     * KEYS: index key. ARGV: session id.
     */
    public static final String REMOVE_INDEXED_SESSION =
            "local removed = redis.call('ZREM', KEYS[1], ARGV[1]) "
                    + "if redis.call('ZCARD', KEYS[1]) == 0 then redis.call('UNLINK', KEYS[1]) end "
                    + "return removed";

    /**
     * Stores a federated authentication session mapping and its expiry in one operation.
     * <p>
     * KEYS: mapping key. (<prefix>:fed:<tenantId>:<idpId>:<idpSessionId>)
     * ARGV: expiry in milliseconds, then field and value pairs.
     */
    public static final String STORE_FEDERATED_MAPPING =
            "redis.call('HSET', KEYS[1], unpack(ARGV, 2)) "
                    + "local expiry = tonumber(ARGV[1]) "
                    + "if expiry and expiry > 0 then redis.call('PEXPIRE', KEYS[1], expiry) end "
                    + "return 1";

    /**
     * Adds a member to a set and raises its expiry when the given one is later, never lowering it, so a
     * reverse index outlives every mapping it points at. A non-positive expiry is ignored rather than
     * applied, since it would delete the set and every member in it.
     * <p>
     * KEYS: set key. (<prefix>:fed:idp:<idpSessionId>)
     * ARGV: member(<tenantId>:<idpId>), expiry in milliseconds.
     */
    public static final String ADD_SET_MEMBER =
            "redis.call('SADD', KEYS[1], ARGV[1]) "
                    + "local expiry = tonumber(ARGV[2]) "
                    + "if expiry and expiry > 0 then "
                    + "local current = redis.call('PTTL', KEYS[1]) "
                    + "if current == -1 or current < expiry then "
                    + "redis.call('PEXPIRE', KEYS[1], expiry) "
                    + "end "
                    + "end "
                    + "return 1";

    /**
     * Raises the expiry of a key when the given one is later, never lowering it, and leaves a key that is
     * not there alone rather than creating one.
     * <p>
     * KEYS: key. ARGV: expiry in milliseconds. Returns 1 when the expiry was raised and 0 otherwise.
     */
    public static final String EXTEND_EXPIRY =
            "local expiry = tonumber(ARGV[1]) "
                    + "if not expiry or expiry <= 0 then return 0 end "
                    // PTTL answers -2 for a key that is gone and -1 for one carrying no expiry.
                    + "local current = redis.call('PTTL', KEYS[1]) "
                    + "if current == -2 then return 0 end "
                    + "if current == -1 or current < expiry then "
                    + "redis.call('PEXPIRE', KEYS[1], expiry) "
                    + "return 1 "
                    + "end "
                    + "return 0";

    /**
     * Removes a member from a set and drops the key once it is empty.
     * <p>
     * KEYS: set key. ARGV: member.
     */
    public static final String REMOVE_SET_MEMBER =
            "local removed = redis.call('SREM', KEYS[1], ARGV[1]) "
                    + "if redis.call('SCARD', KEYS[1]) == 0 then redis.call('UNLINK', KEYS[1]) end "
                    + "return removed";

    /**
     * The arguments the expiry scored index scripts take: the current time, the time the session expires
     * at and the session. Built here, so every index scores a session from one reading of the clock.
     *
     * @param sessionId       Session identifier.
     * @param remainingMillis Remaining expiry of the session in milliseconds.
     * @return the script arguments.
     */
    public static byte[][] createSessionMember(String sessionId, long remainingMillis) {

        long currentTime = System.currentTimeMillis();
        return new byte[][]{
                RedisValueUtils.toBytes(String.valueOf(currentTime)),
                RedisValueUtils.toBytes(String.valueOf(currentTime + remainingMillis)),
                RedisValueUtils.toBytes(sessionId)};
    }

    private RedisScripts() {

    }
}
