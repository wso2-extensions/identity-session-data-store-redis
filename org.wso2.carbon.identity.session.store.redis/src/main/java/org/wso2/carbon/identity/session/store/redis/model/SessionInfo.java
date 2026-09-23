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

import org.wso2.carbon.identity.session.store.redis.util.RedisConstants;
import org.wso2.carbon.identity.session.store.redis.util.RedisValueUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A session hash read from Redis, with its metadata and application information decoded.
 */
public class SessionInfo {

    /**
     * Application information of a session, decoded from a hash field name.
     */
    public static class AppInfo {

        private final String appId;
        private final String subject;

        AppInfo(String appId, String subject) {

            this.appId = appId;
            this.subject = subject;
        }

        /**
         * The application of the record.
         *
         * @return the application identifier.
         */
        public String getAppId() {

            return appId;
        }

        /**
         * The subject of the record.
         *
         * @return the subject in the application.
         */
        public String getSubject() {

            return subject;
        }
    }

    private final String sessionId;
    private final boolean stored;
    private final Integer tenantId;
    private final String userId;
    private final Map<String, String> metadata = new LinkedHashMap<>();
    private final List<AppInfo> applications = new ArrayList<>();

    /**
     * Decodes a session hash read from Redis.
     *
     * @param sessionId Session identifier.
     * @param hash      Fields of the session hash.
     */
    public SessionInfo(String sessionId, Map<String, byte[]> hash) {

        this.sessionId = sessionId;
        this.stored = hash.containsKey(RedisConstants.FIELD_TIME_CREATED);
        this.tenantId = RedisValueUtils.toInteger(
                RedisValueUtils.toString(hash.get(RedisConstants.FIELD_TENANT_ID)));
        this.userId = RedisValueUtils.toUserId(hash.get(RedisConstants.FIELD_USER_ID));

        for (Map.Entry<String, byte[]> field : hash.entrySet()) {
            String name = field.getKey();
            if (name.startsWith(RedisConstants.FIELD_PREFIX_METADATA)) {
                metadata.put(name.substring(RedisConstants.FIELD_PREFIX_METADATA.length()),
                        RedisValueUtils.toString(field.getValue()));
            } else if (name.startsWith(RedisConstants.FIELD_PREFIX_APP)) {
                AppInfo appInfo = parseAppField(
                        name.substring(RedisConstants.FIELD_PREFIX_APP.length()));
                if (appInfo != null) {
                    applications.add(appInfo);
                }
            }
        }
    }

    private static AppInfo parseAppField(String field) {

        String[] components = field.split(RedisConstants.FIELD_VALUE_SEPARATOR,
                RedisConstants.APP_FIELD_COMPONENTS);
        if (components.length < RedisConstants.APP_FIELD_COMPONENTS) {
            return null;
        }
        return new AppInfo(components[0], components[2]);
    }

    /**
     * Whether the session itself was stored, rather than only fields written before it.
     *
     * @return true when the entry is a session.
     */
    public boolean isStored() {

        return stored;
    }

    /**
     * The identifier of the session.
     *
     * @return the session identifier.
     */
    public String getSessionId() {

        return sessionId;
    }

    /**
     * The tenant of the session.
     *
     * @return the tenant identifier, or null when it is not recorded.
     */
    public Integer getTenantId() {

        return tenantId;
    }

    /**
     * The user of the session, which a session holds only one of.
     *
     * @return the user identifier, or {@code null} when it is not recorded.
     */
    public String getUserId() {

        return userId;
    }

    /**
     * The metadata of the session, by property type.
     *
     * @return the metadata properties, as a read-only map.
     */
    public Map<String, String> getMetadata() {

        return Collections.unmodifiableMap(metadata);
    }

    /**
     * The applications of the session.
     *
     * @return the application records, as a read-only list.
     */
    public List<AppInfo> getApplications() {

        return Collections.unmodifiableList(applications);
    }
}
