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

import org.wso2.carbon.base.MultitenantConstants;

/**
 * Configuration keys, defaults and key-scheme constants of the Redis session store.
 */
public class RedisConstants {

    public static final String STORE_NAME = "redis";

    // Connection configuration, read from the session storage properties the framework hands the store.
    public static final String CONF_MODE = "mode";
    public static final String CONF_HOSTS = "hosts";
    public static final String CONF_MASTER_NAME = "master.name";
    public static final String CONF_USERNAME = "username";
    public static final String CONF_PASSWORD = "password";
    public static final String CONF_SENTINEL_PASSWORD = "sentinel.password";
    public static final String CONF_DATABASE = "database";
    public static final String CONF_SSL_ENABLED = "ssl.enabled";
    public static final String CONF_CONNECTION_TIMEOUT = "connection.timeout.millis";
    public static final String CONF_COMMAND_TIMEOUT = "command.timeout.millis";
    public static final String CONF_KEY_PREFIX = "key.prefix";
    public static final String CONF_BATCH_SIZE = "batch.size";

    // Cluster mode configuration.
    public static final String CONF_TOPOLOGY_REFRESH_ENABLED = "cluster.topology.refresh.enabled";
    public static final String CONF_TOPOLOGY_REFRESH_PERIOD = "cluster.topology.refresh.period.seconds";
    public static final String CONF_MAX_REDIRECTS = "cluster.max.redirects";

    // Defaults.
    public static final String DEFAULT_MODE = "standalone";
    public static final int DEFAULT_DATABASE = 0;
    public static final boolean DEFAULT_SSL_ENABLED = false;
    public static final long DEFAULT_CONNECTION_TIMEOUT = 2000L;
    public static final long DEFAULT_COMMAND_TIMEOUT = 1000L;
    public static final String DEFAULT_KEY_PREFIX = "idn";
    public static final int DEFAULT_BATCH_SIZE = 250;
    public static final boolean DEFAULT_TOPOLOGY_REFRESH_ENABLED = true;
    public static final long DEFAULT_TOPOLOGY_REFRESH_PERIOD = 60L;
    public static final int DEFAULT_MAX_REDIRECTS = 5;

    // Supported topologies.
    public static final String MODE_STANDALONE = "standalone";
    public static final String MODE_SENTINEL = "sentinel";
    public static final String MODE_CLUSTER = "cluster";

    public static final int DEFAULT_REDIS_PORT = 6379;
    public static final int DEFAULT_SENTINEL_PORT = 26379;

    // Record type of the session context, which is the only type holding a user, metadata and
    // application information.
    public static final String TYPE_SESSION_CONTEXT_CACHE = "AppAuthFrameworkSessionContextCache";

    // Key segments.
    public static final String KEY_SEPARATOR = ":";
    public static final String SEGMENT_SESSION = "s";
    public static final String SEGMENT_USER = "u";
    public static final String SEGMENT_TENANT = "tid";
    public static final String SEGMENT_FEDERATED = "fed";
    public static final String SEGMENT_IDP = "idp";

    public static final int UNSPECIFIED_TENANT_ID = MultitenantConstants.INVALID_TENANT_ID;

    // Identity provider of a federated mapping stored without one, as a key needs every segment.
    public static final int UNSPECIFIED_IDP_ID = -1;

    // Session hash fields.
    public static final String FIELD_TIME_CREATED = "t";
    public static final String FIELD_TENANT_ID = "tid";
    public static final String FIELD_SESSION_OBJECT = "o";
    public static final String FIELD_USER_ID = "u";
    public static final String FIELD_PREFIX_METADATA = "m:";
    public static final String FIELD_PREFIX_APP = "a:";
    public static final String FIELD_PREFIX_FEDERATED = "f:";

    // Federated mapping hash fields.
    public static final String FIELD_FED_SESSION_ID = "sid";
    public static final String FIELD_FED_IDP_NAME = "idp";
    public static final String FIELD_FED_AUTHENTICATOR_ID = "auth";
    public static final String FIELD_FED_PROTOCOL_TYPE = "proto";
    public static final String FIELD_FED_TIME_CREATED = "tc";

    // ASCII unit separator, used to join the components of a composite hash field name.
    public static final String FIELD_VALUE_SEPARATOR = "\u001f";

    // Components of an application field name: application id, inbound protocol and subject.
    public static final int APP_FIELD_COMPONENTS = 3;

    // Status a store of a session reports, so a caller can tell a rejection from a first store and from
    // an extension without inspecting the session again.
    public static final int STORE_STATUS_STALE = 0;
    public static final int STORE_STATUS_CREATED = 1;
    public static final int STORE_STATUS_EXTENDED = 2;

    // Minimum key expiry, so a sub-second validity period is not rounded down to "no expiry".
    public static final long MIN_EXPIRY_MILLIS = 1000L;

    // Longest cooldown between connection attempts, which the backoff grows towards.
    public static final long MAX_RECONNECT_COOLDOWN_MILLIS = 30000L;

    public static final String OPERATION_STORE = "STORE";
    public static final String OPERATION_DELETE = "DELETE";

    private RedisConstants() {

    }
}
