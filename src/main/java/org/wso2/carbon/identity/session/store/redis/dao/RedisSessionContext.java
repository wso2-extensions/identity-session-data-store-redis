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

import org.wso2.carbon.identity.session.store.redis.config.RedisStoreConfig;
import org.wso2.carbon.identity.session.store.redis.util.RedisConnectionManager;
import org.wso2.carbon.identity.session.store.redis.util.RedisConstants;
import org.wso2.carbon.identity.session.store.redis.util.RedisKeyUtils;
import org.wso2.carbon.identity.session.store.redis.util.RedisTemplate;

/**
 * What every part of this store shares: one connection, one key scheme and the settings that decide how
 * a key is written. Both the session data store and the user session DAO are constructed with it, so
 * neither has to expose its internals to the other.
 */
public class RedisSessionContext implements AutoCloseable {

    private final RedisConnectionManager connectionManager;
    private final RedisTemplate redisTemplate;
    private final RedisKeyUtils keyUtils;
    private final int batchSize;

    /**
     * Creates a context that owns its own connection manager.
     *
     * @param config Store configuration.
     */
    public RedisSessionContext(RedisStoreConfig config) {

        this(config, new RedisConnectionManager(config));
    }

    /**
     * Creates a context with the given configuration and connection manager.
     *
     * @param config            Store configuration.
     * @param connectionManager Connection manager to use.
     */
    public RedisSessionContext(RedisStoreConfig config, RedisConnectionManager connectionManager) {

        this.connectionManager = connectionManager;
        this.redisTemplate = new RedisTemplate(connectionManager, config.getCommandTimeout());
        this.keyUtils = new RedisKeyUtils(config.getKeyPrefix());
        this.batchSize = config.getBatchSize();
    }

    /**
     * The template used to access Redis.
     *
     * @return the template every command and script runs through.
     */
    public RedisTemplate getRedisTemplate() {

        return redisTemplate;
    }

    /**
     * The key builder of the store.
     *
     * @return the key builder of the store.
     */
    public RedisKeyUtils getKeyUtils() {

        return keyUtils;
    }

    /**
     * Whether Redis is reachable.
     *
     * @return true when the store is connected to Redis.
     */
    public boolean isConnected() {

        return connectionManager.isConnected();
    }

    /**
     * How many records are handled per pipelined batch when several are read or removed together.
     *
     * @return the batch size.
     */
    public int getBatchSize() {

        return batchSize;
    }

    /**
     * Builds the key of the session context record of a session.
     *
     * @param sessionId Session identifier.
     * @return the key of the session context hash.
     */
    public String getSessionContextKey(String sessionId) {

        return keyUtils.getSessionKey(RedisConstants.TYPE_SESSION_CONTEXT_CACHE, sessionId);
    }

    /**
     * Releases the Redis client and its connection.
     */
    @Override
    public void close() {

        connectionManager.close();
    }
}
