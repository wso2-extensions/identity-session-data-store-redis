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
import io.lettuce.core.Range;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.database.utils.jdbc.exceptions.DataAccessException;
import org.wso2.carbon.identity.application.authentication.framework.context.AuthHistory;
import org.wso2.carbon.identity.application.authentication.framework.dao.UserSessionDAO;
import org.wso2.carbon.identity.application.authentication.framework.exception.DuplicatedAuthUserException;
import org.wso2.carbon.identity.application.authentication.framework.exception.UserSessionException;
import org.wso2.carbon.identity.application.authentication.framework.exception.session.mgt.SessionManagementServerException;
import org.wso2.carbon.identity.application.authentication.framework.model.Application;
import org.wso2.carbon.identity.application.authentication.framework.model.FederatedUserSession;
import org.wso2.carbon.identity.application.authentication.framework.model.UserSession;
import org.wso2.carbon.identity.application.authentication.framework.store.UserSessionStore;
import org.wso2.carbon.identity.application.authentication.framework.util.SessionMgtConstants;
import org.wso2.carbon.identity.application.authentication.framework.util.SessionMgtUtils;
import org.wso2.carbon.identity.application.common.model.User;
import org.wso2.carbon.identity.core.model.ExpressionNode;
import org.wso2.carbon.identity.core.util.IdentityTenantUtil;
import org.wso2.carbon.identity.session.store.redis.exception.RedisSessionStoreException;
import org.wso2.carbon.identity.session.store.redis.model.FederatedSessionRef;
import org.wso2.carbon.identity.session.store.redis.model.SessionInfo;
import org.wso2.carbon.identity.session.store.redis.model.SessionRemovalResult;
import org.wso2.carbon.identity.session.store.redis.util.RedisConstants;
import org.wso2.carbon.identity.session.store.redis.util.RedisKeyUtils;
import org.wso2.carbon.identity.session.store.redis.util.RedisScripts;
import org.wso2.carbon.identity.session.store.redis.util.RedisTemplate;
import org.wso2.carbon.identity.session.store.redis.util.RedisValueUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.wso2.carbon.identity.application.authentication.framework.util.SessionMgtConstants.ErrorMessages.ERROR_CODE_UNABLE_TO_GET_APP_DATA;
import static org.wso2.carbon.identity.application.authentication.framework.util.SessionMgtConstants.ErrorMessages.ERROR_CODE_UNABLE_TO_GET_FED_USER_SESSION;
import static org.wso2.carbon.identity.application.authentication.framework.util.SessionMgtConstants.ErrorMessages.ERROR_CODE_UNABLE_TO_GET_SESSION;
import static org.wso2.carbon.identity.session.store.redis.util.RedisConstants.UNSPECIFIED_IDP_ID;
import static org.wso2.carbon.identity.session.store.redis.util.RedisConstants.UNSPECIFIED_TENANT_ID;

/**
 * Redis backed implementation of {@link UserSessionDAO}. Handles the session scoped operations that are not part of
 * {@code SessionDataStore}.
 * <p>
 * Session metadata, application information and the user of a session are fields of the session hash,
 * and the sessions of a user and of a tenant are sorted sets scored by session expiry, so everything
 * here expires with the session it belongs to. A session holds a single user.
 */
public class RedisUserSessionDAOImpl implements UserSessionDAO {

    private static final Log LOG = LogFactory.getLog(RedisUserSessionDAOImpl.class);

    private final RedisSessionContext context;
    private final RedisTemplate redisTemplate;
    private final RedisKeyUtils keyUtils;
    private final RedisFederatedSessionStore federatedSessionStore;

    /**
     * Creates a DAO on the shared session context, so that it and the session data store use the same
     * connection and key scheme.
     *
     * @param context Shared Redis session context.
     */
    public RedisUserSessionDAOImpl(RedisSessionContext context) {

        this.context = context;
        this.redisTemplate = context.getRedisTemplate();
        this.keyUtils = context.getKeyUtils();
        this.federatedSessionStore = new RedisFederatedSessionStore(context);
    }

    /**
     * Orders applications by identifier, so the aggregated applications value is deterministic.
     */
    private static Comparator<SessionInfo.AppInfo> getApplicationComparator() {

        return Comparator.comparing((SessionInfo.AppInfo appInfo) -> toAppId(appInfo.getAppId()))
                .thenComparing(appInfo -> appInfo.getSubject() == null
                        ? StringUtils.EMPTY : appInfo.getSubject());
    }

    private static long toAppId(String appId) {

        try {
            return Long.parseLong(appId.trim());
        } catch (NumberFormatException e) {
            return Long.MAX_VALUE;
        }
    }

    private static String getField(Map<String, byte[]> hash, String field) {

        return RedisValueUtils.toString(hash.get(field));
    }

    /**
     * The name this DAO is selected by.
     *
     * @return the store name.
     */
    @Override
    public String getStoreName() {

        return RedisConstants.STORE_NAME;
    }

    /**
     * Retrieves a session with its metadata and applications.
     *
     * @param sessionId Session identifier.
     * @return the session, or null when it is not available.
     * @throws SessionManagementServerException if the session could not be retrieved.
     */
    @Override
    public UserSession getSession(String sessionId) throws SessionManagementServerException {

        try {
            SessionInfo session = getSessionInfo(sessionId);
            if (session == null) {
                return null;
            }
            List<Application> applications = resolveApplications(session.getApplications());
            if (applications.isEmpty()) {
                return null;
            }
            UserSession userSession = new UserSession();
            userSession.setSessionId(sessionId);
            setMetadata(userSession, session);
            userSession.setApplications(applications);
            return userSession;
        } catch (RedisSessionStoreException | UserSessionException e) {
            throw new SessionManagementServerException(
                    ERROR_CODE_UNABLE_TO_GET_SESSION, ERROR_CODE_UNABLE_TO_GET_SESSION.getDescription(), e);
        }
    }

    /**
     * Retrieves a session of a user.
     *
     * @param userId    User identifier.
     * @param sessionId Session identifier.
     * @return the session, or empty when it is not available or does not belong to the user.
     * @throws SessionManagementServerException if the session could not be retrieved.
     */
    @Override
    public Optional<UserSession> getSession(String userId, String sessionId) throws SessionManagementServerException {

        try {
            if (!hasUserSessionMapping(userId, sessionId)) {
                return Optional.empty();
            }
            SessionInfo session = getSessionInfo(sessionId);
            if (session == null) {
                return Optional.empty();
            }
            List<Application> applications = resolveApplications(session.getApplications());
            if (applications.isEmpty()) {
                return Optional.empty();
            }
            UserSession userSession = new UserSession();
            userSession.setSessionId(sessionId);
            userSession.setUserId(userId);
            setMetadata(userSession, session);
            userSession.setApplications(applications);
            return Optional.of(userSession);
        } catch (RedisSessionStoreException | UserSessionException e) {
            throw new SessionManagementServerException(
                    ERROR_CODE_UNABLE_TO_GET_SESSION, ERROR_CODE_UNABLE_TO_GET_SESSION.getDescription(), e);
        }
    }

    /**
     * Retrieves the sessions of a tenant. This is not supported in Redis, as the sessions of a tenant are not indexed.
     *
     * @param tenantId  Tenant identifier.
     * @param filter    Filter expression tree to filter the sessions of the tenant.
     * @param limit     Limit of the number of sessions to retrieve.
     * @param sortOrder Order to sort the sessions of the tenant.
     * @return the sessions of the tenant.
     * @throws UserSessionException if the sessions could not be read.
     */
    @Override
    public List<UserSession> getSessions(int tenantId, List<ExpressionNode> filter, Integer limit,
                                         String sortOrder) throws UserSessionException {

        throw new UnsupportedOperationException("Retrieving sessions by tenant is not supported in Redis.");
    }

    /**
     * Records the user of the session. The session hash is written first, and the record is dropped when the
     * session is not there. The sessions of a user are scored by the expiry of the session, and only the user of the
     * session refreshes those scores.
     * <p>
     * The session is added to the sessions of its tenant here rather than when it is stored, so that the
     * index holds only the sessions that carry a user, which are the ones the active session count
     * reports. The tenant comes back with the user, since the session is stored before its user.
     *
     * @param userId    User identifier.
     * @param sessionId Session identifier.
     * @throws UserSessionException if the mapping could not be stored, or already exists.
     */
    @Override
    public void storeUserSessionData(String userId, String sessionId) throws UserSessionException {

        try {
            // Retrieve the user of the session and its expiry.
            List<Object> stored = redisTemplate.executeScript(RedisScripts.SET_SESSION_USER,
                    ScriptOutputType.MULTI,
                    context.getSessionContextKey(sessionId),
                    RedisValueUtils.toBytes(userId));
            String owner = getUserIdOfSession(stored);
            if (owner == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Session Id: " + sessionId + " is not available, so user Id: " + userId
                            + " is not recorded against it.");
                }
                return;
            }
            if (!StringUtils.equals(userId, owner)) {
                throw new DuplicatedAuthUserException("Mapping between user Id: " + userId
                        + " and session Id: " + sessionId + " already exists in the database.");
            }

            long expiry = getStoredExpiry(stored);
            Long added = redisTemplate.executeScript(RedisScripts.ADD_USER_SESSION,
                    ScriptOutputType.INTEGER, keyUtils.getUserKey(userId),
                    RedisScripts.createSessionMember(sessionId, expiry));
            if (added == null || added == 0) {
                throw new DuplicatedAuthUserException("Mapping between user Id: " + userId
                        + " and session Id: " + sessionId + " already exists in the database.");
            }
            addToTenantIndex(sessionId, getStoredTenantId(stored), expiry);
        } catch (RedisSessionStoreException e) {
            throw new UserSessionException("Error while storing mapping between user Id: " + userId
                    + " and session Id: " + sessionId, e);
        }
    }

    /**
     * Checks whether a user is already recorded against a session.
     *
     * @param userId    User identifier.
     * @param sessionId Session identifier.
     * @return true when the mapping exists and the session it maps has not expired.
     * @throws UserSessionException if the mapping could not be read.
     */
    @Override
    public boolean isExistingMapping(String userId, String sessionId) throws UserSessionException {

        try {
            return hasUserSessionMapping(userId, sessionId);
        } catch (RedisSessionStoreException e) {
            throw new UserSessionException("Error while retrieving existing mapping between user Id: "
                    + userId + " and session Id: " + sessionId, e);
        }
    }

    /**
     * Retrieves the sessions of a user. The relational store answers this from the mapping records alone,
     * which outlive the sessions they point at. Here the sessions of a user are written with the session
     * and scored by its expiry, so a mapping that outlives its session is drift rather than a state of
     * its own, and this reports the same sessions as {@link #getActiveSessionIds(String)}.
     *
     * @param userId User identifier.
     * @return the session identifiers.
     * @throws UserSessionException if the sessions could not be read.
     */
    @Override
    public List<String> getSessionId(String userId) throws UserSessionException {

        return getActiveSessionIds(userId);
    }

    /**
     * Retrieves the sessions of a user that are still available, which is the read behind every by user
     * id operation of the session management API. The unexpired members of the sessions of the user are
     * the candidates, and a candidate is reported only while the session record it points at is still
     * stored.
     *
     * @param userId User identifier.
     * @return the identifiers of the sessions of the user that are still available.
     * @throws UserSessionException if the sessions could not be read.
     */
    @Override
    public List<String> getActiveSessionIds(String userId) throws UserSessionException {

        try {
            List<String> candidates = getIndexedSessionIds(userId);
            List<String> active = new ArrayList<>(candidates.size());
            List<String> missing = new ArrayList<>();
            List<List<KeyValue<String, byte[]>>> replies = readFields(candidates,
                    RedisConstants.FIELD_TIME_CREATED);
            for (int index = 0; index < candidates.size(); index++) {
                // In redis, even the session obj and metadata are stored under the same key, they are done at
                // different times. So, it is possible that the session obj is not stored, but the user mapping is
                // already stored.
                if (toHash(replies.get(index)).containsKey(RedisConstants.FIELD_TIME_CREATED)) {
                    active.add(candidates.get(index));
                } else {
                    missing.add(candidates.get(index));
                }
            }
            removeFromUserIndex(userId, missing);
            return active;
        } catch (RedisSessionStoreException e) {
            throw new UserSessionException("Error while retrieving active session Ids of user Id: " + userId, e);
        }
    }

    /**
     * The unexpired members of the sessions of a user, which are the candidates of a by user id read.
     */
    private List<String> getIndexedSessionIds(String userId) throws RedisSessionStoreException {

        List<String> sessionIds = new ArrayList<>();
        // Sessions are scored by expiry, so the range excludes the expired ones.
        Range<Number> range = Range.from(Range.Boundary.excluding(System.currentTimeMillis()),
                Range.Boundary.unbounded());
        List<byte[]> members = redisTemplate.execute(
                commands -> commands.zrangebyscore(keyUtils.getUserKey(userId), range));
        for (byte[] member : members) {
            sessionIds.add(RedisValueUtils.toString(member));
        }
        return sessionIds;
    }

    /**
     * Retrieves the sessions of a user of an identity provider.
     *
     * @param user  User.
     * @param idpId Identity provider identifier.
     * @return the session identifiers.
     * @throws UserSessionException if the sessions could not be read.
     */
    @Override
    public List<String> getSessionId(User user, int idpId) throws UserSessionException {

        String userId = getUserId(user, idpId);
        return userId == null ? new ArrayList<>() : getSessionId(userId);
    }

    /**
     * Checks whether a user of an identity provider is already recorded against a session.
     *
     * @param user      User.
     * @param idpId     Identity provider identifier.
     * @param sessionId Session identifier.
     * @return true when the mapping exists.
     * @throws UserSessionException if the mapping could not be read.
     */
    @Override
    public boolean isExistingMapping(User user, int idpId, String sessionId) throws UserSessionException {

        String userId = getUserId(user, idpId);
        return userId != null && isExistingMapping(userId, sessionId);
    }

    /**
     * Not required, as Redis removes a session and the information belonging to it together.
     */
    @Override
    public void removeExpiredSessionRecords() {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Expired session records are removed by Redis, so no cleanup is performed.");
        }
    }

    /**
     * Terminates sessions that are still available. Redis holds a session and the user, metadata and
     * applications belonging to it in one key. So terminated session has already taken all of them with it. An entry
     * that is still there was therefore not removed when its session was terminated, so it is logged and removed
     * here.
     *
     * @param sessionIdList Identifiers of the terminated sessions.
     */
    @Override
    public void removeTerminatedSessionRecords(List<String> sessionIdList) {

        if (sessionIdList == null || sessionIdList.isEmpty()) {
            return;
        }
        for (String sessionId : sessionIdList) {
            if (StringUtils.isBlank(sessionId)) {
                continue;
            }
            try {
                removeTerminatedSession(sessionId);
            } catch (RedisSessionStoreException e) {
                LOG.error("Error while removing a terminated session. The session and the indexes may "
                        + "hold it until they expire.", e);
            }
        }
    }

    /**
     * Records that a session was used to log in to an application.
     *
     * @param sessionId   Session identifier.
     * @param subject     Subject in the application.
     * @param appID       Application identifier.
     * @param inboundAuth Inbound authentication protocol.
     * @throws DataAccessException if the application data could not be stored.
     */
    @Override
    public void storeAppSessionData(String sessionId, String subject, int appID, String inboundAuth)
            throws DataAccessException {

        try {
            setSessionFields(sessionId, Collections.singletonMap(
                    RedisKeyUtils.getAppField(appID, inboundAuth, subject), StringUtils.EMPTY));
        } catch (RedisSessionStoreException e) {
            throw new DataAccessException("Error while storing application data of session id: " + sessionId
                    + ", subject: " + subject + ", app Id: " + appID + ", protocol: " + inboundAuth + ".", e);
        }
    }

    /**
     * Checks whether a session is already recorded against an application.
     *
     * @param sessionId   Session identifier.
     * @param subject     Subject in the application.
     * @param appID       Application identifier.
     * @param inboundAuth Inbound authentication protocol.
     * @return true when the application data exists.
     * @throws UserSessionException if the application data could not be read.
     */
    @Override
    public boolean isExistingAppSession(String sessionId, String subject, int appID, String inboundAuth)
            throws UserSessionException {

        try {
            Boolean exists = redisTemplate.execute(commands -> commands.hexists(
                    context.getSessionContextKey(sessionId),
                    RedisKeyUtils.getAppField(appID, inboundAuth, subject)));
            return Boolean.TRUE.equals(exists);
        } catch (RedisSessionStoreException e) {
            throw new UserSessionException("Error while retrieving application data of session id: " + sessionId
                    + ", subject: " + subject + ", app Id: " + appID + ", protocol: " + inboundAuth + ".", e);
        }
    }

    /**
     * Stores the metadata of a session.
     *
     * @param sessionId Session identifier.
     * @param metaData  Metadata by property type.
     * @throws UserSessionException if the metadata could not be stored.
     */
    @Override
    public void storeSessionMetaData(String sessionId, Map<String, String> metaData) throws UserSessionException {

        if (metaData == null || metaData.isEmpty()) {
            return;
        }
        Map<String, String> fields = new LinkedHashMap<>();
        for (Map.Entry<String, String> property : metaData.entrySet()) {
            fields.put(RedisKeyUtils.getMetadataField(property.getKey()),
                    Objects.toString(property.getValue(), StringUtils.EMPTY));
        }
        try {
            setSessionFields(sessionId, fields);
        } catch (RedisSessionStoreException e) {
            throw new UserSessionException("Error while storing metadata of session: " + sessionId + ".", e);
        }
    }

    /**
     * Updates one metadata property of a session.
     *
     * @param sessionId    Session identifier.
     * @param propertyType Metadata property type.
     * @param value        Value of the property.
     * @throws UserSessionException if the metadata could not be updated.
     */
    @Override
    public void updateSessionMetaData(String sessionId, String propertyType, String value) throws UserSessionException {

        String fieldKey = RedisKeyUtils.getMetadataField(propertyType);
        String fieldValue = Objects.toString(value, StringUtils.EMPTY);
        Map<String, String> sessionFields = Map.of(fieldKey, fieldValue);

        try {
            setSessionFields(sessionId, sessionFields);
        } catch (RedisSessionStoreException e) {
            String errorMessage = String.format("Error while updating %s of session: %s.", propertyType, sessionId);
            throw new UserSessionException(errorMessage, e);
        }
    }

    /**
     * Stores a federated authentication session mapping without specifying a tenant or an identity provider.
     *
     * @param sessionContextKey Session context key.
     * @param authHistory       Authentication history of the flow.
     * @throws UserSessionException if the mapping could not be stored.
     */
    @Override
    public void storeFederatedAuthSessionInfo(String sessionContextKey, AuthHistory authHistory)
            throws UserSessionException {

        storeFederatedAuthSessionInfo(sessionContextKey, authHistory, UNSPECIFIED_TENANT_ID, UNSPECIFIED_IDP_ID);
    }

    /**
     * Stores a federated authentication session mapping of a tenant.
     *
     * @param sessionContextKey Session context key.
     * @param authHistory       Authentication history of the flow.
     * @param tenantId          Tenant identifier.
     * @throws UserSessionException if the mapping could not be stored.
     */
    @Override
    public void storeFederatedAuthSessionInfo(String sessionContextKey, AuthHistory authHistory,
                                              int tenantId) throws UserSessionException {

        storeFederatedAuthSessionInfo(sessionContextKey, authHistory, tenantId, UNSPECIFIED_IDP_ID);
    }

    /**
     * Stores a federated authentication session mapping of an identity provider.
     *
     * @param sessionContextKey Session context key.
     * @param authHistory       Authentication history of the flow.
     * @param idpId             Identity provider identifier.
     * @throws UserSessionException if the mapping could not be stored.
     */
    @Override
    public void storeFederatedAuthSessionInfoWithIdpId(String sessionContextKey, AuthHistory authHistory,
                                                       int idpId) throws UserSessionException {

        storeFederatedAuthSessionInfo(sessionContextKey, authHistory, UNSPECIFIED_TENANT_ID, idpId);
    }

    /**
     * Stores a federated authentication session mapping. The key of the mapping is its uniqueness
     * constraint, so a store and an update are the same operation.
     *
     * @param sessionContextKey Session context key.
     * @param authHistory       Authentication history.
     * @param tenantId          Tenant identifier.
     * @param idpId             Identity provider identifier.
     * @throws UserSessionException if the mapping could not be stored.
     */
    @Override
    public void storeFederatedAuthSessionInfo(String sessionContextKey, AuthHistory authHistory,
                                              int tenantId, int idpId) throws UserSessionException {

        try {
            federatedSessionStore.store(sessionContextKey, authHistory, tenantId, idpId);
        } catch (RedisSessionStoreException e) {
            throw new UserSessionException("Error while adding session details of the session index:"
                    + sessionContextKey + ", IdP:" + authHistory.getIdpName(), e);
        }
    }

    /**
     * Points a federated authentication session mapping at another session.
     *
     * @param sessionContextKey Session context key.
     * @param authHistory       Authentication history of the flow.
     * @throws UserSessionException if the mapping could not be updated.
     */
    @Override
    public void updateFederatedAuthSessionInfo(String sessionContextKey, AuthHistory authHistory)
            throws UserSessionException {

        updateFederatedAuthSessionInfo(sessionContextKey, authHistory, UNSPECIFIED_TENANT_ID, UNSPECIFIED_IDP_ID);
    }

    /**
     * Points a federated authentication session mapping of a tenant at another session.
     *
     * @param sessionContextKey Session context key.
     * @param authHistory       Authentication history of the flow.
     * @param tenantId          Tenant identifier.
     * @throws UserSessionException if the mapping could not be updated.
     */
    @Override
    public void updateFederatedAuthSessionInfo(String sessionContextKey, AuthHistory authHistory,
                                               int tenantId) throws UserSessionException {

        updateFederatedAuthSessionInfo(sessionContextKey, authHistory, tenantId, UNSPECIFIED_IDP_ID);
    }

    /**
     * Points a federated authentication session mapping of an identity provider at another session.
     *
     * @param sessionContextKey Session context key.
     * @param authHistory       Authentication history of the flow.
     * @param idpId             Identity provider identifier.
     * @throws UserSessionException if the mapping could not be updated.
     */
    @Override
    public void updateFederatedAuthSessionInfoWithIdpId(String sessionContextKey, AuthHistory authHistory,
                                                        int idpId) throws UserSessionException {

        updateFederatedAuthSessionInfo(sessionContextKey, authHistory, UNSPECIFIED_TENANT_ID, idpId);
    }

    /**
     * Points a federated authentication session mapping at another session, refreshing its expiry.
     *
     * @param sessionContextKey Session context key.
     * @param authHistory       Authentication history.
     * @param tenantId          Tenant identifier.
     * @param idpId             Identity provider identifier.
     * @throws UserSessionException if the mapping could not be updated.
     */
    @Override
    public void updateFederatedAuthSessionInfo(String sessionContextKey, AuthHistory authHistory, int tenantId,
                                               int idpId) throws UserSessionException {

        try {
            federatedSessionStore.detachPreviousSession(sessionContextKey,
                    authHistory.getIdpSessionIndex(), tenantId, idpId);
            federatedSessionStore.store(sessionContextKey, authHistory, tenantId, idpId);
        } catch (RedisSessionStoreException e) {
            throw new UserSessionException("Error while updating session details of the session index:"
                    + sessionContextKey + ", IdP:" + authHistory.getIdpName(), e);
        }
    }

    /**
     * Checks whether a federated authentication session exists for an identity provider session index.
     *
     * @param idpSessionIndex Identity provider session index.
     * @return true when a mapping exists.
     * @throws UserSessionException if the mappings could not be read.
     */
    @Override
    public boolean hasExistingFederatedAuthSession(String idpSessionIndex) throws UserSessionException {

        // In Redis, null means "don't filter on this dimension"
        return hasFederatedAuthSession(idpSessionIndex, null, null);
    }

    /**
     * Checks whether a federated authentication session exists for a session index of a tenant.
     *
     * @param idpSessionIndex Identity provider session index.
     * @param tenantId        Tenant identifier.
     * @return true when a mapping exists.
     * @throws UserSessionException if the mappings could not be read.
     */
    @Override
    public boolean isExistingFederatedAuthSessionAvailable(String idpSessionIndex, int tenantId)
            throws UserSessionException {

        return hasFederatedAuthSession(idpSessionIndex, tenantId, null);
    }

    /**
     * Checks whether a federated authentication session exists for a session index of an identity
     * provider.
     *
     * @param idpSessionIndex Identity provider session index.
     * @param idpId           Identity provider identifier.
     * @return true when a mapping exists.
     * @throws UserSessionException if the mappings could not be read.
     */
    @Override
    public boolean hasExistingFederatedAuthSessionWithIdpId(String idpSessionIndex, int idpId)
            throws UserSessionException {

        return hasFederatedAuthSession(idpSessionIndex, null, idpId);
    }

    /**
     * Checks whether a federated authentication session exists for a session index of a tenant and
     * an identity provider.
     *
     * @param idpSessionIndex Identity provider session index.
     * @param tenantId        Tenant identifier.
     * @param idpId           Identity provider identifier.
     * @return true when a mapping exists.
     * @throws UserSessionException if the mapping could not be read.
     */
    @Override
    public boolean hasExistingFederatedAuthSession(String idpSessionIndex, int tenantId, int idpId)
            throws UserSessionException {

        try {
            return federatedSessionStore.hasMapping(tenantId, idpId, idpSessionIndex);
        } catch (RedisSessionStoreException e) {
            throw new UserSessionException("Error while checking for a federated authentication session "
                    + "with the session index: " + idpSessionIndex + ".", e);
        }
    }

    /**
     * Retrieves the federated authentication session of an identity provider session index.
     *
     * @param fedIdpSessionId Identity provider session index.
     * @return the federated session, or null when no mapping exists.
     * @throws SessionManagementServerException if the session could not be retrieved.
     */
    @Override
    public FederatedUserSession getFederatedAuthSessionDetails(String fedIdpSessionId)
            throws SessionManagementServerException {

        List<FederatedUserSession> sessions = getFederatedAuthSessionsDetails(fedIdpSessionId);
        return sessions.isEmpty() ? null : sessions.get(0);
    }

    /**
     * Retrieves the federated authentication sessions of an identity provider session index, which is
     * all a back channel logout carries.
     *
     * @param fedIdpSessionId Identity provider session index.
     * @return the federated authentication sessions found.
     * @throws SessionManagementServerException if the sessions could not be retrieved.
     */
    @Override
    public List<FederatedUserSession> getFederatedAuthSessionsDetails(String fedIdpSessionId)
            throws SessionManagementServerException {

        if (StringUtils.isBlank(fedIdpSessionId)) {
            return new ArrayList<>();
        }
        List<FederatedUserSession> sessions = new ArrayList<>();
        try {
            for (Map<String, byte[]> mapping : federatedSessionStore.getActiveMappings(fedIdpSessionId)) {
                sessions.add(toFederatedUserSession(fedIdpSessionId, mapping));
            }
            return sessions;
        } catch (RedisSessionStoreException e) {
            throw new SessionManagementServerException(
                    ERROR_CODE_UNABLE_TO_GET_FED_USER_SESSION,
                    ERROR_CODE_UNABLE_TO_GET_FED_USER_SESSION.getDescription(), e);
        }
    }

    /**
     * Builds a federated authentication session from the fields of its mapping.
     *
     * @param idpSessionId Identity provider session index of the mapping.
     * @param mapping      Fields of the mapping.
     * @return the federated authentication session.
     */
    private static FederatedUserSession toFederatedUserSession(String idpSessionId, Map<String, byte[]> mapping) {

        return new FederatedUserSession(idpSessionId,
                getField(mapping, RedisConstants.FIELD_FED_SESSION_ID),
                getField(mapping, RedisConstants.FIELD_FED_IDP_NAME),
                getField(mapping, RedisConstants.FIELD_FED_AUTHENTICATOR_ID),
                getField(mapping, RedisConstants.FIELD_FED_PROTOCOL_TYPE));
    }

    /**
     * Removes every federated authentication session mapping of a session.
     *
     * @param sessionContextKey Session context key.
     * @throws UserSessionException if the mappings could not be removed.
     */
    @Override
    public void removeFederatedAuthSessionInfo(String sessionContextKey) throws UserSessionException {

        try {
            federatedSessionStore.removeBySession(sessionContextKey);
        } catch (RedisSessionStoreException e) {
            throw new UserSessionException("Error while removing federated authentication session "
                    + "details of the session index:" + sessionContextKey, e);
        }
    }

    /**
     * Removes the federated authentication session mappings of a session that belong to one identity
     * provider.
     *
     * @param sessionContextKey Session context key.
     * @param idpId             Identity provider identifier.
     * @throws UserSessionException if the mappings could not be removed.
     */
    @Override
    public void removeFederatedAuthSessionInfo(String sessionContextKey, int idpId)
            throws UserSessionException {

        try {
            federatedSessionStore.removeBySession(sessionContextKey, idpId);
        } catch (RedisSessionStoreException e) {
            throw new UserSessionException("Error while removing federated authentication session "
                    + "details of the session index:" + sessionContextKey, e);
        }
    }

    /**
     * Counts the active sessions of a tenant, which are the sessions of the tenant that carry a user and
     * have not expired.
     * <p>
     * A session is stored with the expiry the framework computes for it, and the index is scored by that
     * same expiry, so a member scored ahead of now is a session that is still stored and no session has
     * to be read. A session that carries no tenant of its own is counted for no tenant, the way the
     * relational query selects the sessions of the tenant alone.
     *
     * @param tenantDomain Tenant domain.
     * @return the number of active sessions.
     * @throws UserSessionException if the count could not be retrieved.
     */
    @Override
    public int getActiveSessionCount(String tenantDomain) throws UserSessionException {

        int tenantId = IdentityTenantUtil.getTenantId(tenantDomain);
        try {
            Range<Number> live = Range.from(Range.Boundary.excluding(System.currentTimeMillis()),
                    Range.Boundary.unbounded());
            Long count = redisTemplate.execute(
                    commands -> commands.zcount(keyUtils.getTenantKey(tenantId), live));
            return count == null ? 0 : (int) Math.min(count, Integer.MAX_VALUE);
        } catch (RedisSessionStoreException e) {
            throw new UserSessionException("Error while retrieving active session count of the tenant "
                    + "domain, " + tenantDomain, e);
        }
    }

    /**
     * Reads the given fields of each session, pipelined in batches of the configured batch size.
     */
    private List<List<KeyValue<String, byte[]>>> readFields(List<String> sessionIds, String... fields)
            throws RedisSessionStoreException {

        List<List<KeyValue<String, byte[]>>> replies = new ArrayList<>(sessionIds.size());
        for (List<String> batch : getBatches(sessionIds)) {
            replies.addAll(redisTemplate.executeBatch(commands -> {
                List<RedisFuture<List<KeyValue<String, byte[]>>>> pending = new ArrayList<>(batch.size());
                for (String sessionId : batch) {
                    pending.add(commands.hmget(context.getSessionContextKey(sessionId), fields));
                }
                return pending;
            }));
        }
        return replies;
    }

    private List<List<String>> getBatches(List<String> sessionIds) {

        List<List<String>> batches = new ArrayList<>();
        int batchSize = context.getBatchSize();
        for (int index = 0; index < sessionIds.size(); index += batchSize) {
            batches.add(sessionIds.subList(index, Math.min(index + batchSize, sessionIds.size())));
        }
        return batches;
    }

    private static Map<String, byte[]> toHash(List<KeyValue<String, byte[]>> fields) {

        Map<String, byte[]> hash = new HashMap<>();
        for (KeyValue<String, byte[]> field : fields) {
            if (field.hasValue()) {
                hash.put(field.getKey(), field.getValue());
            }
        }
        return hash;
    }

    /**
     * Removes sessions that are no longer available from the sessions of a user.
     */
    private void removeFromUserIndex(String userId, List<String> sessionIds) {

        if (sessionIds.isEmpty()) {
            return;
        }
        try {
            List<RedisTemplate.ScriptCall> removals = new ArrayList<>(sessionIds.size());
            for (String sessionId : sessionIds) {
                removals.add(new RedisTemplate.ScriptCall(keyUtils.getUserKey(userId),
                        RedisValueUtils.toBytes(sessionId)));
            }
            logFailedIndexRemovals(sessionIds, redisTemplate.collectScriptBatch(
                    RedisScripts.REMOVE_INDEXED_SESSION, ScriptOutputType.INTEGER, removals),
                    "the sessions of the user: " + userId);
        } catch (RedisSessionStoreException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Could not remove sessions that are no longer available from the sessions of "
                        + "the user: " + userId + ".", e);
            }
        }
    }

    /**
     * Reports the index removals the server rejected, against the session each of them belongs to. The
     * cleanup is best effort, so a failure is logged rather than raised.
     */
    private void logFailedIndexRemovals(List<String> sessionIds, RedisTemplate.BatchResult<?> result,
                                        String index) {

        if (!result.hasFailures() || !LOG.isDebugEnabled()) {
            return;
        }
        for (Map.Entry<Integer, String> failure : result.getFailures().entrySet()) {
            LOG.debug("Could not remove the session: " + sessionIds.get(failure.getKey()) + " from "
                    + index + ". Redis reported: " + failure.getValue());
        }
    }

    /**
     * Removes a session and everything belonging to it, the way the delete path of the session data store
     * does, so the two paths cannot leave a session in different states. The deletion is the same script,
     * so the user, the tenant and the federated mappings needed to clean up come back with it rather than
     * with a read of their own.
     */
    private void removeTerminatedSession(String sessionId) throws RedisSessionStoreException {

        SessionRemovalResult deleted = SessionRemovalResult.build(redisTemplate.executeScript(
                RedisScripts.DELETE_SESSION, ScriptOutputType.MULTI, context.getSessionContextKey(sessionId)));
        Integer tenantId = deleted.getTenantId();
        // The tenant is written with the session record, so an entry that carries one was a session
        // rather than fields written ahead of it, and a session should already have been removed.
        if (tenantId != null && LOG.isDebugEnabled()) {
            LOG.debug("A terminated session was still available and has been removed. Session id: "
                    + sessionId + ".");
        }
        byte[] session = RedisValueUtils.toBytes(sessionId);
        String userId = deleted.getUserId();
        if (userId != null) {
            redisTemplate.executeScript(RedisScripts.REMOVE_INDEXED_SESSION, ScriptOutputType.INTEGER,
                    keyUtils.getUserKey(userId), session);
        }
        if (tenantId != null) {
            redisTemplate.executeScript(RedisScripts.REMOVE_INDEXED_SESSION, ScriptOutputType.INTEGER,
                    keyUtils.getTenantKey(tenantId), session);
        }
        // The session is already gone, so its mappings are the ones the deletion reported.
        federatedSessionStore.removeMappings(deleted.getFederatedMembers());
    }

    /**
     * The user identifier a store of the user returned, which is the first element of its reply.
     */
    private static String getUserIdOfSession(List<?> result) {

        return result != null && !result.isEmpty() ? RedisValueUtils.toUserId(result.get(0)) : null;
    }

    /**
     * The remaining expiry of the session a store of the user returned, which is the second element of
     * its reply.
     */
    private static long getStoredExpiry(List<?> result) {

        return result != null && result.size() > 1 ? RedisValueUtils.toLong(result.get(1)) : 0;
    }

    /**
     * The tenant a store of the user returned, which is the third element of its reply and is absent when
     * the session carried no tenant.
     */
    private static Integer getStoredTenantId(List<?> result) {

        return result != null && result.size() > 2
                ? RedisValueUtils.toInteger(RedisValueUtils.toString(result.get(2))) : null;
    }

    /**
     * Adds a session to the sessions of its tenant, which is the index the active session count is
     * answered from.
     */
    private void addToTenantIndex(String sessionId, Integer tenantId, long expiry)
            throws RedisSessionStoreException {

        if (tenantId == null) {
            return;
        }
        redisTemplate.executeScript(RedisScripts.ADD_TENANT_SESSION, ScriptOutputType.INTEGER,
                keyUtils.getTenantKey(tenantId), RedisScripts.createSessionMember(sessionId, expiry));
    }

    /**
     * Reads a session and the information belonging to it.
     *
     * @param sessionId Session identifier.
     * @return the session, or {@code null} when it is not available.
     * @throws RedisSessionStoreException if the session could not be read.
     */
    private SessionInfo getSessionInfo(String sessionId) throws RedisSessionStoreException {

        Map<String, byte[]> hash = redisTemplate.execute(commands -> commands.hgetall(
                context.getSessionContextKey(sessionId)));
        if (hash == null || hash.isEmpty()) {
            return null;
        }
        SessionInfo session = new SessionInfo(sessionId, hash);
        return session.isStored() ? session : null;
    }

    /**
     * Writes metadata or application fields onto the session hash, and drops them when the session is not
     * there, since the callers store the session before the fields belonging to it. An empty map is
     * skipped, as the script requires at least one field and value pair.
     */
    private void setSessionFields(String sessionId, Map<String, String> fields)
            throws RedisSessionStoreException {

        if (fields.isEmpty()) {
            return;
        }
        byte[][] args = new byte[fields.size() * 2][];
        int index = 0;
        for (Map.Entry<String, String> field : fields.entrySet()) {
            args[index++] = RedisValueUtils.toBytes(field.getKey());
            args[index++] = RedisValueUtils.toBytes(field.getValue());
        }
        Long written = redisTemplate.executeScript(RedisScripts.SET_FIELDS, ScriptOutputType.INTEGER,
                context.getSessionContextKey(sessionId), args);
        if ((written == null || written == 0) && LOG.isDebugEnabled()) {
            LOG.debug("Session Id: " + sessionId + " is not available, so the fields written against it "
                    + "are dropped.");
        }
    }

    /**
     * Whether the sessions of a user hold a session that has not expired. The index is scored by the
     * expiry of the session, so a member scored in the past is a mapping of a session that is gone, and
     * the by user id reads already range past it. Reading such a member as no mapping keeps this check
     * consistent with those reads, and lets a login record the mapping again instead of skipping it as
     * one that already exists.
     */
    private boolean hasUserSessionMapping(String userId, String sessionId) throws RedisSessionStoreException {

        Double score = redisTemplate.execute(commands -> commands.zscore(keyUtils.getUserKey(userId),
                RedisValueUtils.toBytes(sessionId)));
        // The same bound the reads range by, which take the members scored later than now.
        return score != null && score > System.currentTimeMillis();
    }

    private boolean hasFederatedAuthSession(String idpSessionIndex, Integer tenantId, Integer idpId)
            throws UserSessionException {

        for (FederatedSessionRef reference : getActiveFedSessionMappings(idpSessionIndex)) {
            if ((tenantId == null || tenantId == reference.getTenantId())
                    && (idpId == null || idpId == reference.getIdpId())) {
                return true;
            }
        }
        return false;
    }

    private List<FederatedSessionRef> getActiveFedSessionMappings(String idpSessionId) throws UserSessionException {

        if (StringUtils.isBlank(idpSessionId)) {
            return new ArrayList<>();
        }
        try {
            return federatedSessionStore.getActiveFedSessionMappings(idpSessionId);
        } catch (RedisSessionStoreException e) {
            throw new UserSessionException("Error while retrieving the federated authentication sessions "
                    + "of the session index: " + idpSessionId + ".", e);
        }
    }

    private void setMetadata(UserSession userSession, SessionInfo session) {

        session.getMetadata().forEach((property, value) -> {
            switch (property) {
                case SessionMgtConstants.USER_AGENT:
                    userSession.setUserAgent(value);
                    break;
                case SessionMgtConstants.IP_ADDRESS:
                    userSession.setIp(value);
                    break;
                case SessionMgtConstants.LAST_ACCESS_TIME:
                    userSession.setLastAccessTime(value);
                    break;
                case SessionMgtConstants.LOGIN_TIME:
                    userSession.setLoginTime(value);
                    break;
                default:
                    break;
            }
        });
    }

    /**
     * Resolves the applications of a session, dropping those without an application record.
     */
    private List<Application> resolveApplications(List<SessionInfo.AppInfo> appInfoList) throws UserSessionException {

        if (appInfoList.isEmpty()) {
            return new ArrayList<>();
        }
        Set<String> appIds = new LinkedHashSet<>();
        for (SessionInfo.AppInfo appInfo : appInfoList) {
            appIds.add(appInfo.getAppId());
        }
        Map<String, Application> resolved = getApplicationsFromAppId(appIds);

        List<SessionInfo.AppInfo> ordered = new ArrayList<>(appInfoList);
        ordered.sort(getApplicationComparator());

        List<Application> applications = new ArrayList<>();
        for (SessionInfo.AppInfo appInfo : ordered) {
            Application application = resolved.get(appInfo.getAppId());
            if (application != null) {
                applications.add(new Application(appInfo.getSubject(), application.getAppName(),
                        appInfo.getAppId(), application.getResourceId()));
            }
        }
        return applications;
    }

    /**
     * Retrieves the applications of the given identifiers from the application records.
     *
     * @param appIds Application identifiers.
     * @return the applications by identifier.
     * @throws UserSessionException if the applications could not be retrieved.
     */
    private Map<String, Application> getApplicationsFromAppId(Set<String> appIds) throws UserSessionException {

        try {
            return SessionMgtUtils.getApplicationsByIds(appIds);
        } catch (DataAccessException e) {
            throw new UserSessionException(ERROR_CODE_UNABLE_TO_GET_APP_DATA.getDescription(), e);
        }
    }

    /**
     * Retrieves the identifier of a user from the user records.
     *
     * @param user  User.
     * @param idpId Identity provider identifier.
     * @return the user identifier, or {@code null} when the user has never held a session.
     * @throws UserSessionException if the identifier could not be retrieved.
     */
    private String getUserId(User user, int idpId) throws UserSessionException {

        return UserSessionStore.getInstance().getUserId(user.getUserName(),
                IdentityTenantUtil.getTenantId(user.getTenantDomain()), user.getUserStoreDomain(), idpId);
    }
}
