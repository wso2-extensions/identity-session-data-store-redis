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

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.identity.application.authentication.framework.exception.SessionSerializerException;
import org.wso2.carbon.identity.application.authentication.framework.store.SessionContextDO;
import org.wso2.carbon.identity.application.authentication.framework.store.SessionDataStore;
import org.wso2.carbon.identity.application.authentication.framework.util.FrameworkUtils;
import org.wso2.carbon.identity.session.store.redis.exception.RedisSessionStoreException;
import org.wso2.carbon.identity.session.store.redis.internal.RedisSessionStoreDataHolder;
import org.wso2.carbon.identity.session.store.redis.model.SessionRemovalResult;
import org.wso2.carbon.identity.session.store.redis.model.SessionStoreResult;
import org.wso2.carbon.identity.session.store.redis.util.RedisConstants;
import org.wso2.carbon.identity.session.store.redis.util.RedisKeyUtils;
import org.wso2.carbon.identity.session.store.redis.util.RedisScripts;
import org.wso2.carbon.identity.session.store.redis.util.RedisTemplate;
import org.wso2.carbon.identity.session.store.redis.util.RedisValueUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A record is a single hash, and expiry is the key expiry rather than a stored column, so a session and
 * everything belonging to it is removed together. Writes are synchronous.
 * <p>
 * Every failure is logged and swallowed: reads return {@code null} and writes have no effect, so a Redis outage
 * cannot fail an authentication.
 */
public class RedisSessionDataStore extends SessionDataStore {

    private static final Log LOG = LogFactory.getLog(RedisSessionDataStore.class);

    private final RedisSessionContext context;
    private final RedisTemplate redisTemplate;
    private final RedisKeyUtils keyUtils;
    private final RedisFederatedSessionStore federatedSessionStore;

    /**
     * Creates a store over the given context.
     *
     * @param context Shared Redis session context.
     */
    public RedisSessionDataStore(RedisSessionContext context) {

        this.context = context;
        this.redisTemplate = context.getRedisTemplate();
        this.keyUtils = context.getKeyUtils();
        this.federatedSessionStore = new RedisFederatedSessionStore(context);
    }

    /**
     * The name this store is selected by.
     *
     * @return the store name.
     */
    @Override
    public String getStoreName() {

        return RedisConstants.STORE_NAME;
    }

    /**
     * Stores a record, replacing an earlier one of the same key and type.
     *
     * @param key      Record key.
     * @param type     Record type.
     * @param entry    Value to store.
     * @param tenantId Tenant of the record.
     */
    @Override
    public void storeSessionData(String key, String type, Object entry, int tenantId) {

        persistSessionData(key, type, entry, FrameworkUtils.getCurrentStandardNano(), tenantId);
    }

    /**
     * Stores a record with the given creation time. A record older than the stored one is not applied.
     *
     * @param key      Record key.
     * @param type     Record type.
     * @param entry    Value to store.
     * @param nanoTime Creation time of the record.
     * @param tenantId Tenant of the record.
     */
    @Override
    public void persistSessionData(String key, String type, Object entry, long nanoTime, int tenantId) {

        try {
            long expiry = RedisValueUtils.getExpiryMillis(nanoTime + getValidityPeriodNano(entry, type, tenantId));
            if (expiry <= 0) {
                // The record arrived already expired, so nothing is persisted. An earlier record is left to
                // its own expiry rather than deleted, so a late write cannot remove a newer one.
                return;
            }
            SessionStoreResult stored = SessionStoreResult.build(redisTemplate.executeScript(
                    RedisScripts.STORE_SESSION, ScriptOutputType.MULTI,
                    keyUtils.getSessionKey(type, key),
                    RedisValueUtils.toBytes(String.valueOf(nanoTime)),
                    RedisValueUtils.toBytes(String.valueOf(tenantId)),
                    serialize(entry),
                    RedisValueUtils.toBytes(String.valueOf(expiry))));
            if (!stored.isStored()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("A newer record of the session data: " + key + " of type: " + type
                            + " is already stored, so this one is not applied.");
                }
                return;
            }
            if (isSessionContextType(type)) {
                refreshTenantIndex(tenantId, key, expiry);
                if (stored.getUserId() != null) {
                    refreshUserIndex(key, stored.getUserId(), expiry);
                }
                if (!stored.getFederatedMembers().isEmpty()) {
                    federatedSessionStore.refreshExpiry(stored.getFederatedMembers(), expiry);
                }
            }
        } catch (RedisSessionStoreException | RuntimeException e) {
            LOG.error("Error while storing session data of type: " + type, e);
        }
    }

    /**
     * Removes a record.
     *
     * @param key  Record key.
     * @param type Record type.
     */
    @Override
    public void clearSessionData(String key, String type) {

        removeSessionData(key, type, FrameworkUtils.getCurrentStandardNano());
    }

    /**
     * Removes a record. The timestamp is not used, since a removal is idempotent.
     *
     * @param key      Record key.
     * @param type     Record type.
     * @param nanoTime Removal timestamp.
     */
    @Override
    public void removeSessionData(String key, String type, long nanoTime) {

        try {
            removeSession(key, type);
        } catch (RedisSessionStoreException | RuntimeException e) {
            LOG.error("Error while removing session data of type: " + type, e);
        }
    }

    /**
     * Removes several records of the same type.
     *
     * @param keys Record keys.
     * @param type Record type.
     */
    @Override
    public void clearSessionDataBatch(List<String> keys, String type) {

        if (keys == null || keys.isEmpty()) {
            return;
        }
        List<String> keysToRemove = new ArrayList<>(keys.size());
        for (String key : keys) {
            if (StringUtils.isNotBlank(key)) {
                keysToRemove.add(key);
            }
        }
        int batchSize = context.getBatchSize();
        try {
            for (int index = 0; index < keysToRemove.size(); index += batchSize) {
                int end = Math.min(index + batchSize, keysToRemove.size());
                removeBatch(keysToRemove.subList(index, end), type);
            }
        } catch (RedisSessionStoreException | RuntimeException e) {
            LOG.error("Error while removing a batch of session data of type: " + type, e);
        }
    }

    /**
     * Removes a temporary authentication context record.
     *
     * @param key  Record key.
     * @param type Record type.
     */
    @Override
    public void removeTempAuthnContextData(String key, String type) {

        removeSessionData(key, type, FrameworkUtils.getCurrentStandardNano());
    }

    /**
     * Not required, as Redis expires the records of the store by itself.
     */
    @Override
    public void removeExpiredSessionData() {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Expired session data is removed by Redis, so no cleanup is performed.");
        }
    }

    /**
     * Whether the store runs a cleanup of expired records, which Redis makes unnecessary.
     *
     * @return false, as Redis expires the records itself.
     */
    @Override
    public boolean isSessionDataCleanupEnabled() {

        return false;
    }

    /**
     * Retrieves the value of a record.
     *
     * @param key  Record key.
     * @param type Record type.
     * @return the stored value, or null when the record is not available.
     */
    @Override
    public Object getSessionData(String key, String type) {

        SessionContextDO sessionContextDO = getSessionContextData(key, type);
        return sessionContextDO == null ? null : sessionContextDO.getEntry();
    }

    /**
     * Retrieves a record with its creation time.
     *
     * @param key  Record key.
     * @param type Record type.
     * @return the record, or null when it is not available.
     */
    @Override
    public SessionContextDO getSessionContextData(String key, String type) {

        try {
            return getSessionContextDO(key, type);
        } catch (RedisSessionStoreException | RuntimeException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Error while retrieving session data of type: " + type, e);
            }
            return null;
        }
    }

    /**
     * Retrieves the value of a record when the given operation is the last one applied.
     *
     * @param key       Record key.
     * @param type      Record type.
     * @param operation Operation to match.
     * @return the stored value, or null when the operation does not match.
     */
    @Override
    public Object getSessionData(String key, String type, String operation) {

        SessionContextDO sessionContextDO = getSessionContextData(key, type, operation);
        return sessionContextDO == null ? null : sessionContextDO.getEntry();
    }

    /**
     * Returns the record when the given operation is the last one applied. As a removal deletes the
     * record, {@value RedisConstants#OPERATION_DELETE} is never the last operation of an available one.
     *
     * @param key       Record key.
     * @param type      Record type.
     * @param operation Operation to match.
     * @return the record, or {@code null} if the operation does not match.
     */
    @Override
    public SessionContextDO getSessionContextData(String key, String type, String operation) {

        if (!RedisConstants.OPERATION_STORE.equalsIgnoreCase(operation)) {
            return null;
        }
        return getSessionContextData(key, type);
    }

    /**
     * Checks whether the given operation is the last one applied to a record.
     * {@value RedisConstants#OPERATION_DELETE} is never the last operation, as a removal deletes the
     * record.
     *
     * @param key               Record key.
     * @param type              Record type.
     * @param requiredOperation Operation to match.
     * @return true when the operation matches.
     */
    @Override
    public boolean validateLastOperationOnSessionData(String key, String type, String requiredOperation) {

        return RedisConstants.OPERATION_STORE.equalsIgnoreCase(requiredOperation)
                && getSessionContextData(key, type) != null;
    }

    /**
     * Releases the resources of the store when the bundle is stopped.
     */
    @Override
    public void stopService() {

        try {
            context.close();
        } catch (RuntimeException e) {
            LOG.warn("Error while releasing the Redis client on shutdown.", e);
        }
    }

    private SessionContextDO getSessionContextDO(String key, String type) throws RedisSessionStoreException {

        List<KeyValue<String, byte[]>> fields = redisTemplate.execute(commands -> commands.hmget(
                keyUtils.getSessionKey(type, key),
                RedisConstants.FIELD_TIME_CREATED,
                RedisConstants.FIELD_SESSION_OBJECT));

        long timeCreated = 0;
        byte[] sessionObject = null;
        for (KeyValue<String, byte[]> field : fields) {
            if (!field.hasValue()) {
                continue;
            }
            if (RedisConstants.FIELD_TIME_CREATED.equals(field.getKey())) {
                timeCreated = RedisValueUtils.toLong(field.getValue());
            } else if (RedisConstants.FIELD_SESSION_OBJECT.equals(field.getKey())) {
                sessionObject = field.getValue();
            }
        }
        if (sessionObject == null || sessionObject.length == 0) {
            return null;
        }
        return new SessionContextDO(key, type, deserialize(sessionObject), timeCreated);
    }

    /**
     * Removes a session and the indexes referring to it. The steps are sequential and idempotent.
     *
     * @param key  Record key.
     * @param type Record type.
     * @throws RedisSessionStoreException if the session could not be removed.
     */
    private void removeSession(String key, String type) throws RedisSessionStoreException {

        if (!isSessionContextType(type)) {
            redisTemplate.execute(commands -> commands.unlink(keyUtils.getSessionKey(type, key)));
            return;
        }
        SessionRemovalResult removed = SessionRemovalResult.build(redisTemplate.executeScript(
                RedisScripts.DELETE_SESSION, ScriptOutputType.MULTI, keyUtils.getSessionKey(type, key)));
        removeSessionIndexes(key, removed.getUserId(), removed.getTenantId());
        // The session carried its federated mappings, so the deletion reported them and they need no read.
        federatedSessionStore.removeMappings(removed.getFederatedMembers());
    }

    /**
     * Removes several sessions of the same type, pipelining each stage.
     */
    private void removeBatch(List<String> keys, String type) throws RedisSessionStoreException {

        if (!isSessionContextType(type)) {
            RedisTemplate.BatchResult<Long> unlinked = redisTemplate.collectBatch(commands -> {
                List<RedisFuture<Long>> pending = new ArrayList<>(keys.size());
                for (String key : keys) {
                    pending.add(commands.unlink(keyUtils.getSessionKey(type, key)));
                }
                return pending;
            });
            logFailedRemovals(keys, unlinked, type);
            return;
        }

        List<RedisTemplate.ScriptCall> deletions = new ArrayList<>(keys.size());
        for (String key : keys) {
            deletions.add(new RedisTemplate.ScriptCall(keyUtils.getSessionKey(type, key)));
        }

        RedisTemplate.BatchResult<Object> deleted = redisTemplate.collectScriptBatch(RedisScripts.DELETE_SESSION,
                ScriptOutputType.MULTI, deletions);

        // Resolve the user and tenant of each deleted session.
        List<RedisTemplate.ScriptCall> indexRemovals = new ArrayList<>();
        List<String> removedMembers = new ArrayList<>();
        for (int index = 0; index < keys.size(); index++) {
            String key = keys.get(index);
            String failure = deleted.getFailure(index);
            if (failure != null) {
                // The session is still there, so its indexes are left alone rather than orphaning it.
                LOG.error("Error while removing the session: " + key + " of type: " + type
                        + ". Redis reported: " + failure);
                continue;
            }
            Object reply = deleted.getReplies().get(index);
            SessionRemovalResult result = SessionRemovalResult.build(
                    reply instanceof List ? (List<?>) reply : null);
            byte[] sessionId = RedisValueUtils.toBytes(key);
            if (result.getUserId() != null) {
                indexRemovals.add(new RedisTemplate.ScriptCall(keyUtils.getUserKey(result.getUserId()),
                        sessionId));
            }
            if (result.getTenantId() != null) {
                indexRemovals.add(new RedisTemplate.ScriptCall(
                        keyUtils.getTenantKey(result.getTenantId()), sessionId));
            }
            removedMembers.addAll(result.getFederatedMembers());
        }
        redisTemplate.executeScriptBatch(RedisScripts.REMOVE_INDEXED_SESSION, ScriptOutputType.INTEGER, indexRemovals);
        federatedSessionStore.removeMappings(removedMembers);
    }

    /**
     * Reports the removals the server rejected, against the record each of them belongs to.
     */
    private void logFailedRemovals(List<String> keys, RedisTemplate.BatchResult<?> result, String type) {

        if (!result.hasFailures()) {
            return;
        }
        for (Map.Entry<Integer, String> failure : result.getFailures().entrySet()) {
            LOG.error("Error while removing the session data: " + keys.get(failure.getKey()) + " of type: "
                    + type + ". Redis reported: " + failure.getValue());
        }
    }

    private void removeSessionIndexes(String sessionId, String userId, Integer tenantId)
            throws RedisSessionStoreException {

        byte[] session = RedisValueUtils.toBytes(sessionId);
        if (userId != null) {
            redisTemplate.executeScript(RedisScripts.REMOVE_INDEXED_SESSION, ScriptOutputType.INTEGER,
                    keyUtils.getUserKey(userId), session);
        }
        if (tenantId != null) {
            redisTemplate.executeScript(RedisScripts.REMOVE_INDEXED_SESSION, ScriptOutputType.INTEGER,
                    keyUtils.getTenantKey(tenantId), session);
        }
    }

    /**
     * Raises the expiry the index of the user of the session scores it by, so that a session which is
     * extended stays visible to the by user id reads for as long as it is live. The score is written
     * once, when the mapping between the user and the session is created, and the extension of a session
     * does not go through that path, so without this the session would be trimmed from the sessions of
     * its user while it is still stored.
     * <p>
     * A user index that no longer holds the session is left alone: the mapping is created by the DAO and
     * removed by a logout, and a store must not create either.
     *
     * @param sessionId Session identifier.
     * @param userId    User identifier held by the session, as stored on the session.
     * @param expiry    Remaining expiry of the session in milliseconds.
     * @throws RedisSessionStoreException if the index could not be updated.
     */
    private void refreshUserIndex(String sessionId, String userId, long expiry)
            throws RedisSessionStoreException {

        if (userId == null) {
            return;
        }
        redisTemplate.executeScript(RedisScripts.REFRESH_INDEXED_SESSION, ScriptOutputType.INTEGER,
                keyUtils.getUserKey(userId), RedisScripts.createSessionMember(sessionId, expiry));
    }

    /**
     * Raises the expiry the index of the tenant of the session scores it by, so that a session which is
     * extended is still counted as active while it is live. The member is created when the user of the
     * session is recorded, since the count reports only the sessions that carry a user, so a session
     * that has none is not brought into the index here. The tenant may be
     * {@link RedisConstants#UNSPECIFIED_TENANT_ID}, which is a valid index of its own.
     *
     * @param tenantId  Tenant identifier.
     * @param sessionId Session identifier.
     * @param expiry    Remaining expiry of the session in milliseconds.
     * @throws RedisSessionStoreException if the index could not be updated.
     */
    private void refreshTenantIndex(int tenantId, String sessionId, long expiry)
            throws RedisSessionStoreException {

        redisTemplate.executeScript(RedisScripts.REFRESH_INDEXED_SESSION, ScriptOutputType.INTEGER,
                keyUtils.getTenantKey(tenantId), RedisScripts.createSessionMember(sessionId, expiry));
    }

    private byte[] serialize(Object entry) throws RedisSessionStoreException {

        try (InputStream inputStream = RedisSessionStoreDataHolder.getSessionSerializer()
                .serializeSessionObject(entry)) {
            return inputStream.readAllBytes();
        } catch (SessionSerializerException | IOException e) {
            throw new RedisSessionStoreException("Error while serializing the session object.", e);
        }
    }

    private Object deserialize(byte[] value) throws RedisSessionStoreException {

        try {
            return RedisSessionStoreDataHolder.getSessionSerializer()
                    .deSerializeSessionObject(new ByteArrayInputStream(value));
        } catch (SessionSerializerException | RuntimeException e) {
            throw new RedisSessionStoreException("Error while deserializing the session object.", e);
        }
    }

    private static boolean isSessionContextType(String type) {

        return RedisConstants.TYPE_SESSION_CONTEXT_CACHE.equals(type);
    }
}
