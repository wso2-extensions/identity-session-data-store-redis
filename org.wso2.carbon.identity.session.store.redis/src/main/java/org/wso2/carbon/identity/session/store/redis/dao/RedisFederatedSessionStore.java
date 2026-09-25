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

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.identity.application.authentication.framework.context.AuthHistory;
import org.wso2.carbon.identity.session.store.redis.exception.RedisSessionStoreException;
import org.wso2.carbon.identity.session.store.redis.model.FederatedSessionRef;
import org.wso2.carbon.identity.session.store.redis.util.RedisConstants;
import org.wso2.carbon.identity.session.store.redis.util.RedisKeyUtils;
import org.wso2.carbon.identity.session.store.redis.util.RedisScripts;
import org.wso2.carbon.identity.session.store.redis.util.RedisTemplate;
import org.wso2.carbon.identity.session.store.redis.util.RedisValueUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The federated authentication session mappings, which are the Redis counterpart of
 * {@code IDN_FED_AUTH_SESSION_MAPPING}. A mapping is a hash keyed by tenant, identity provider and
 * identity provider session index, reachable from the session that owns it and from the identity
 * provider session index.
 * <p>
 * The session records the mappings it owns as fields of its own, so they expire with the session and
 * carry no lifetime to maintain, and a session store or deletion reports them without a read. The index
 * of an identity provider session is a key of its own, and it outlives the mappings in it, so every read
 * confirms that the mapping still exists and prunes the index entry when it does not.
 */
public class RedisFederatedSessionStore {

    private static final Log LOG = LogFactory.getLog(RedisFederatedSessionStore.class);

    private final RedisTemplate redisTemplate;
    private final RedisKeyUtils keyUtils;
    private final RedisSessionContext context;

    /**
     * Creates a federated mapping store.
     *
     * @param context Shared Redis session context.
     */
    public RedisFederatedSessionStore(RedisSessionContext context) {

        this.context = context;
        this.redisTemplate = context.getRedisTemplate();
        this.keyUtils = context.getKeyUtils();
    }

    /**
     * Stores a mapping, the reference to it on the session it belongs to, and the index of its identity
     * provider session, all with the expiry of the session.
     *
     * @param sessionContextKey Session context key.
     * @param authHistory       Authentication history of the flow.
     * @param tenantId          Tenant identifier.
     * @param idpId             Identity provider identifier.
     * @throws RedisSessionStoreException if the mapping could not be stored.
     */
    public void store(String sessionContextKey, AuthHistory authHistory, int tenantId, int idpId)
            throws RedisSessionStoreException {

        String idpSessionId = authHistory.getIdpSessionIndex();
        // Record the mapping on the session: <prefix>:s:<type>:<sid> field f:<tenantId>:<idpId>:<idpSessionId>
        Long expiry = redisTemplate.executeScript(RedisScripts.ATTACH_FEDERATED_MAPPING,
                ScriptOutputType.INTEGER, context.getSessionContextKey(sessionContextKey),
                RedisValueUtils.toBytes(RedisKeyUtils.getPrefixedFederatedField(tenantId, idpId, idpSessionId)));
        if (expiry == null || expiry <= 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Session: " + sessionContextKey + " is not available, so the federated "
                        + "authentication session mapping of the identity provider session index: "
                        + idpSessionId + " is dropped.");
            }
            return;
        }

        Map<String, String> mapping = new LinkedHashMap<>();
        mapping.put(RedisConstants.FIELD_FED_SESSION_ID, sessionContextKey);
        mapping.put(RedisConstants.FIELD_FED_IDP_NAME, StringUtils.defaultString(authHistory.getIdpName()));
        mapping.put(RedisConstants.FIELD_FED_AUTHENTICATOR_ID,
                StringUtils.defaultString(authHistory.getAuthenticatorName()));
        mapping.put(RedisConstants.FIELD_FED_PROTOCOL_TYPE,
                StringUtils.defaultString(authHistory.getRequestType()));
        mapping.put(RedisConstants.FIELD_FED_TIME_CREATED, String.valueOf(System.currentTimeMillis()));

        byte[][] args = new byte[1 + mapping.size() * 2][];
        args[0] = RedisValueUtils.toBytes(String.valueOf(expiry));
        int index = 1;
        for (Map.Entry<String, String> field : mapping.entrySet()) {
            args[index++] = RedisValueUtils.toBytes(field.getKey());
            args[index++] = RedisValueUtils.toBytes(field.getValue());
        }
        // Store key: <prefix>:fed:<tenantId>:<idpId>:<idpSessionId>
        redisTemplate.executeScript(RedisScripts.STORE_FEDERATED_MAPPING, ScriptOutputType.INTEGER,
                keyUtils.getFederatedKey(tenantId, idpId, idpSessionId), args);

        // Store the index entry, <prefix>:fed:idp:<idpSessionId> -> <tenantId>:<idpId>
        redisTemplate.executeScript(RedisScripts.ADD_SET_MEMBER, ScriptOutputType.INTEGER,
                keyUtils.getFederatedByIdpSessionKey(idpSessionId),
                RedisValueUtils.toBytes(RedisKeyUtils.getFederatedIdpMember(tenantId, idpId)),
                RedisValueUtils.toBytes(String.valueOf(expiry)));
    }

    /**
     * Raises the expiry of the mappings of a session and of the indexes that hold them, so that a session
     * which is extended keeps its federated mappings for as long as it is live.
     *
     * @param members Federated mapping members of the session, as the store reported them.
     * @param expiry  Remaining expiry of the session in milliseconds.
     * @throws RedisSessionStoreException if the expiry could not be raised.
     */
    public void refreshExpiry(List<String> members, long expiry) throws RedisSessionStoreException {

        // Most sessions federate nothing, and for them the store reports no member and this costs nothing.
        List<RedisTemplate.ScriptCall> calls = new ArrayList<>();
        byte[] expiryArg = RedisValueUtils.toBytes(String.valueOf(expiry));
        for (FederatedSessionRef reference : parse(members)) {
            calls.add(new RedisTemplate.ScriptCall(keyUtils.getFederatedKey(reference.getTenantId(),
                    reference.getIdpId(), reference.getIdpSessionId()), expiryArg));
            calls.add(new RedisTemplate.ScriptCall(
                    keyUtils.getFederatedByIdpSessionKey(reference.getIdpSessionId()), expiryArg));
        }
        redisTemplate.executeScriptBatch(RedisScripts.EXTEND_EXPIRY, ScriptOutputType.INTEGER, calls);
    }

    /**
     * Stops the previous session from referring to a mapping that now belongs to another one.
     *
     * @param sessionContextKey Session the mapping now belongs to.
     * @param idpSessionId      Identity provider session index of the mapping.
     * @param tenantId          Tenant identifier.
     * @param idpId             Identity provider identifier.
     * @throws RedisSessionStoreException if the previous reference could not be removed.
     */
    public void detachPreviousSession(String sessionContextKey, String idpSessionId, int tenantId,
                                      int idpId) throws RedisSessionStoreException {

        String previousSessionId = redisTemplate.execute(commands -> RedisValueUtils.toString(
                commands.hget(keyUtils.getFederatedKey(tenantId, idpId, idpSessionId),
                        RedisConstants.FIELD_FED_SESSION_ID)));
        if (previousSessionId == null || previousSessionId.equals(sessionContextKey)) {
            return;
        }
        String field = RedisKeyUtils.getPrefixedFederatedField(tenantId, idpId, idpSessionId);
        redisTemplate.execute(commands -> commands.hdel(
                context.getSessionContextKey(previousSessionId), field));
    }

    /**
     * Whether a mapping exists.
     *
     * @param tenantId     Tenant identifier.
     * @param idpId        Identity provider identifier.
     * @param idpSessionId Identity provider session index.
     * @return true when the mapping key exists.
     * @throws RedisSessionStoreException if the mapping could not be read.
     */
    public boolean hasMapping(int tenantId, int idpId, String idpSessionId)
            throws RedisSessionStoreException {

        Long count = redisTemplate.execute(commands -> commands.exists(
                keyUtils.getFederatedKey(tenantId, idpId, idpSessionId)));
        return count != null && count > 0;
    }

    /**
     * The mappings of an identity provider session index that still exist, pruning the index entries of
     * those that do not.
     *
     * @param idpSessionId Identity provider session index.
     * @return the live mappings, as tenant and identity provider references.
     * @throws RedisSessionStoreException if the mappings could not be read.
     */
    public List<FederatedSessionRef> getActiveFedSessionMappings(String idpSessionId)
            throws RedisSessionStoreException {

        List<FederatedSessionRef> activeFedSessions = new ArrayList<>();
        for (FederatedSessionRef reference : getIndexedFedSessionMappings(idpSessionId)) {
            if (hasMapping(reference.getTenantId(), reference.getIdpId(), idpSessionId)) {
                activeFedSessions.add(reference);
            } else {
                pruneIdpSessionIndex(idpSessionId, reference);
            }
        }
        return activeFedSessions;
    }

    /**
     * Reads every mapping of an identity provider session index, pruning the index entries of those
     * that are gone. Reading a mapping already reports whether it is there, so this needs no existence
     * check of its own.
     *
     * @param idpSessionId Identity provider session index.
     * @return the fields of each mapping that still exists.
     * @throws RedisSessionStoreException if the mappings could not be read.
     */
    public List<Map<String, byte[]>> getActiveMappings(String idpSessionId)
            throws RedisSessionStoreException {

        List<Map<String, byte[]>> mappings = new ArrayList<>();
        for (FederatedSessionRef reference : getIndexedFedSessionMappings(idpSessionId)) {
            Map<String, byte[]> mapping = getMapping(reference, idpSessionId);
            if (mapping == null || mapping.isEmpty()) {
                pruneIdpSessionIndex(idpSessionId, reference);
                continue;
            }
            mappings.add(mapping);
        }
        return mappings;
    }

    /**
     * Reads a mapping.
     *
     * @param reference    Tenant and identity provider of the mapping.
     * @param idpSessionId Identity provider session index.
     * @return the fields of the mapping, which is empty when it is gone.
     * @throws RedisSessionStoreException if the mapping could not be read.
     */
    private Map<String, byte[]> getMapping(FederatedSessionRef reference, String idpSessionId)
            throws RedisSessionStoreException {

        return redisTemplate.execute(commands -> commands.hgetall(
                keyUtils.getFederatedKey(reference.getTenantId(), reference.getIdpId(), idpSessionId)));
    }

    /**
     * Removes a member of the identity provider session index whose mapping is gone.
     *
     * @param idpSessionId Identity provider session index.
     * @param reference    Tenant and identity provider of the mapping.
     * @throws RedisSessionStoreException if the member could not be removed.
     */
    private void pruneIdpSessionIndex(String idpSessionId, FederatedSessionRef reference)
            throws RedisSessionStoreException {

        redisTemplate.executeScript(RedisScripts.REMOVE_SET_MEMBER, ScriptOutputType.INTEGER,
                keyUtils.getFederatedByIdpSessionKey(idpSessionId),
                RedisValueUtils.toBytes(RedisKeyUtils.getFederatedIdpMember(reference.getTenantId(),
                        reference.getIdpId())));
    }

    /**
     * Removes every mapping of a session and the index entries that referred to them.
     *
     * @param sessionId Session identifier.
     * @throws RedisSessionStoreException if the mappings could not be removed.
     */
    public void removeBySession(String sessionId) throws RedisSessionStoreException {

        removeMappings(sessionId, getSessionMembers(sessionId));
    }

    /**
     * Removes the mappings a session held and the index entries that referred to them. The members are
     * the ones the deletion of the session reported, so this needs no read of its own.
     *
     * @param members Federated mapping members of the session.
     * @throws RedisSessionStoreException if the mappings could not be removed.
     */
    public void removeMappings(List<String> members) throws RedisSessionStoreException {

        // The session is already gone, so the fields that held these members went with it.
        removeMappings(null, members);
    }

    /**
     * Removes the given mappings, the index entries referring to them, and, when the session is still
     * there, the fields that recorded them on it.
     */
    private void removeMappings(String sessionId, List<String> members) throws RedisSessionStoreException {

        List<FederatedSessionRef> references = parse(members);
        if (references.isEmpty()) {
            return;
        }
        List<String> mappingKeys = new ArrayList<>(references.size());
        List<RedisTemplate.ScriptCall> pruneCalls = new ArrayList<>(references.size());
        for (FederatedSessionRef reference : references) {
            mappingKeys.add(keyUtils.getFederatedKey(reference.getTenantId(), reference.getIdpId(),
                    reference.getIdpSessionId()));
            pruneCalls.add(new RedisTemplate.ScriptCall(
                    keyUtils.getFederatedByIdpSessionKey(reference.getIdpSessionId()),
                    RedisValueUtils.toBytes(RedisKeyUtils.getFederatedIdpMember(
                            reference.getTenantId(), reference.getIdpId()))));
        }
        redisTemplate.executeScriptBatch(RedisScripts.REMOVE_SET_MEMBER, ScriptOutputType.INTEGER,
                pruneCalls);
        redisTemplate.executeBatch(commands -> {
            List<RedisFuture<Long>> pending = new ArrayList<>(mappingKeys.size());
            for (String mappingKey : mappingKeys) {
                pending.add(commands.unlink(mappingKey));
            }
            return pending;
        });
        if (sessionId != null) {
            String[] fields = new String[references.size()];
            for (int index = 0; index < references.size(); index++) {
                FederatedSessionRef reference = references.get(index);
                fields[index] = RedisKeyUtils.getPrefixedFederatedField(reference.getTenantId(),
                        reference.getIdpId(), reference.getIdpSessionId());
            }
            redisTemplate.execute(commands -> commands.hdel(
                    context.getSessionContextKey(sessionId), fields));
        }
    }

    /**
     * Removes the mappings of a session that belong to one identity provider.
     *
     * @param sessionId Session identifier.
     * @param idpId     Identity provider identifier.
     * @throws RedisSessionStoreException if the mappings could not be removed.
     */
    public void removeBySession(String sessionId, int idpId) throws RedisSessionStoreException {

        List<String> idpMembers = new ArrayList<>();
        for (String member : getSessionMembers(sessionId)) {
            FederatedSessionRef reference = FederatedSessionRef.buildFromMember(member);
            if (reference != null && reference.getIdpId() == idpId) {
                idpMembers.add(member);
            }
        }
        removeMappings(sessionId, idpMembers);
    }

    /**
     * The federated mapping members a session holds, read from the fields of the session itself.
     */
    private List<String> getSessionMembers(String sessionId) throws RedisSessionStoreException {

        List<String> members = new ArrayList<>();
        for (String fedSessionRef : redisTemplate.execute(commands -> commands.hkeys(
                context.getSessionContextKey(sessionId)))) {
            String member = RedisKeyUtils.stripFederatedFieldPrefix(fedSessionRef);
            if (member != null) {
                members.add(member);
            }
        }
        return members;
    }

    private static List<FederatedSessionRef> parse(List<String> members) {

        List<FederatedSessionRef> references = new ArrayList<>(members.size());
        for (String member : members) {
            FederatedSessionRef reference = FederatedSessionRef.buildFromMember(member);
            if (reference != null) {
                references.add(reference);
            }
        }
        return references;
    }

    private List<FederatedSessionRef> getIndexedFedSessionMappings(String idpSessionId)
            throws RedisSessionStoreException {

        List<FederatedSessionRef> references = new ArrayList<>();
        for (byte[] member : redisTemplate.execute(commands -> commands.smembers(
                keyUtils.getFederatedByIdpSessionKey(idpSessionId)))) {
            // A member of this index carries only the tenant and the identity provider, so the session
            // index of the reference is the one that was asked for.
            FederatedSessionRef reference = FederatedSessionRef.buildFromIdpMember(
                    RedisValueUtils.toString(member), idpSessionId);
            if (reference != null) {
                references.add(reference);
            }
        }
        return references;
    }
}
