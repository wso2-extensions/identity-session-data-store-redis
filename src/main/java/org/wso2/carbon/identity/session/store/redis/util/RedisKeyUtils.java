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
 * Builds the Redis keys and the composite hash field names of the session store.
 */
public class RedisKeyUtils {

    private final String prefix;

    /**
     * Creates a key builder that prefixes every key it builds.
     *
     * @param prefix Prefix of every key of the store.
     */
    public RedisKeyUtils(String prefix) {

        this.prefix = prefix;
    }

    /**
     * Builds the key of a session record.
     *
     * @param type      Session type.
     * @param sessionId Session identifier.
     * @return the key of the session hash.
     */
    public String getSessionKey(String type, String sessionId) {

        return joinKey(RedisConstants.SEGMENT_SESSION, type, sessionId);
    }

    /**
     * Builds the key of the sessions of a user.
     *
     * @param userId User identifier.
     * @return the key of the sorted set holding the sessions of a user.
     */
    public String getUserKey(String userId) {

        return joinKey(RedisConstants.SEGMENT_USER, userId);
    }

    /**
     * Builds the key of the sessions of a tenant.
     *
     * @param tenantId Tenant identifier.
     * @return the key of the sorted set holding the sessions of a tenant.
     */
    public String getTenantKey(int tenantId) {

        return joinKey(RedisConstants.SEGMENT_TENANT, String.valueOf(tenantId));
    }

    /**
     * Builds the key of a federated authentication session mapping.
     * Format: {@code <prefix>:fed:<tenantId>:<idpId>:<idpSessionId>}
     *
     * @param tenantId     Tenant identifier.
     * @param idpId        Identity provider identifier.
     * @param idpSessionId Identity provider session index.
     * @return the key of the federated session mapping.
     */
    public String getFederatedKey(int tenantId, int idpId, String idpSessionId) {

        return joinKey(RedisConstants.SEGMENT_FEDERATED, String.valueOf(tenantId),
                String.valueOf(idpId), idpSessionId);
    }

    /**
     * Builds the key of the federated authentication session mappings of an identity provider session.
     * Format: {@code <prefix>:fed:idp:<idpSessionId}}
     *
     * @param idpSessionId Identity provider session index.
     * @return the key of the set used to look up federated mappings.
     */
    public String getFederatedByIdpSessionKey(String idpSessionId) {

        return joinKey(RedisConstants.SEGMENT_FEDERATED, RedisConstants.SEGMENT_IDP, idpSessionId);
    }

    /**
     * Builds a member of the session to federated mapping set.
     * Format: {@code <tenantId>:<idpId>:<idpSessionId>}
     *
     * @param tenantId     Tenant identifier.
     * @param idpId        Identity provider identifier.
     * @param idpSessionId Identity provider session index.
     * @return a member of the session to federated mapping set.
     */
    public static String getFederatedMember(int tenantId, int idpId, String idpSessionId) {

        return joinMember(String.valueOf(tenantId), String.valueOf(idpId), idpSessionId);
    }

    /**
     * Builds a member of the identity provider session to federated mapping set.
     * Format: {@code <tenantId>:<idpId>}
     *
     * @param tenantId Tenant identifier.
     * @param idpId    Identity provider identifier.
     * @return a member of the identity provider session to federated mapping set.
     */
    public static String getFederatedIdpMember(int tenantId, int idpId) {

        return joinMember(String.valueOf(tenantId), String.valueOf(idpId));
    }

    /**
     * Builds the session hash field name that records a federated mapping of the session.
     * Format: {@code f:<tenantId>:<idpId>:<idpSessionId>}
     *
     * @param tenantId     Tenant identifier.
     * @param idpId        Identity provider identifier.
     * @param idpSessionId Identity provider session index.
     * @return the federated mapping field name.
     */
    public static String getPrefixedFederatedField(int tenantId, int idpId, String idpSessionId) {

        return RedisConstants.FIELD_PREFIX_FEDERATED + getFederatedMember(tenantId, idpId, idpSessionId);
    }

    /**
     * Reads the federated mapping a session hash field name records.
     *
     * @param field Field name of a session hash.
     * @return the member, or {@code null} when the field records something else.
     */
    public static String stripFederatedFieldPrefix(String field) {

        return field != null && field.startsWith(RedisConstants.FIELD_PREFIX_FEDERATED)
                ? field.substring(RedisConstants.FIELD_PREFIX_FEDERATED.length()) : null;
    }

    /**
     * Builds a metadata hash field name.
    /**
     * Builds a metadata hash field name.
     *
     * @param propertyType Metadata property type, which is kept verbatim.
     * @return the metadata hash field name.
     */
    public static String getMetadataField(String propertyType) {

        // Not joined, as the property type is the whole field name behind the prefix.
        return RedisConstants.FIELD_PREFIX_METADATA + propertyType;
    }

    /**
     * Builds an application hash field name, which carries the whole application record, since every
     * column of it is part of its primary key.
     *
     * @param appId       Application identifier.
     * @param inboundAuth Inbound authentication protocol.
     * @param subject     Subject in the application.
     * @return the application hash field name.
     */
    public static String getAppField(int appId, String inboundAuth, String subject) {

        return joinField(RedisConstants.FIELD_PREFIX_APP, String.valueOf(appId), inboundAuth, subject);
    }

    /**
     * Joins the segments of a top level key, behind the configured prefix.
     *
     * @param segments Segments of the key, in order.
     * @return the key.
     */
    private String joinKey(String... segments) {

        StringBuilder builder = new StringBuilder(prefix);
        for (String segment : segments) {
            builder.append(RedisConstants.KEY_SEPARATOR).append(segment);
        }
        return builder.toString();
    }

    /**
     * Joins the components of a set member with the key separator, as a member is a fragment of the key
     * it points at and reads like the tail of that key.
     *
     * @param components Components of the member, in order.
     * @return the member.
     */
    private static String joinMember(String... components) {

        return String.join(RedisConstants.KEY_SEPARATOR, components);
    }

    /**
     * Joins the components of a hash field name, behind the prefix that says which kind of field it is.
     *
     * @param fieldPrefix Prefix identifying the kind of field, which carries its own separator.
     * @param components  Components of the name, in order.
     * @return the hash field name.
     */
    private static String joinField(String fieldPrefix, String... components) {

        return fieldPrefix + String.join(RedisConstants.FIELD_VALUE_SEPARATOR, components);
    }
}
