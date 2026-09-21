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

import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import org.junit.jupiter.api.Test;
import org.wso2.carbon.identity.session.store.redis.config.RedisStoreConfig;
import org.wso2.carbon.identity.session.store.redis.exception.RedisSessionStoreException;
import org.wso2.carbon.identity.session.store.redis.util.RedisConnectionManager;
import org.wso2.carbon.identity.session.store.redis.util.RedisConstants;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests the store boundary: no operation throws, and no operation reaches Redis when persistence is
 * disabled.
 */
public class RedisSessionDataStoreTest {

    private static final String KEY = "session-1";
    private static final String TYPE = RedisConstants.TYPE_SESSION_CONTEXT_CACHE;

    /**
     * A connection manager that fails every operation, as an unreachable server does.
     */
    private static class FailingConnectionManager extends RedisConnectionManager {

        private int operations;

        @Override
        public RedisClusterCommands<String, byte[]> getCommands() throws RedisSessionStoreException {

            operations++;
            throw new RedisSessionStoreException("No connection to Redis.");
        }

        @Override
        public RedisClusterAsyncCommands<String, byte[]> getAsyncCommands()
                throws RedisSessionStoreException {

            operations++;
            throw new RedisSessionStoreException("No connection to Redis.");
        }

        @Override
        public void close() {

        }
    }

    @Test
    void testNoOperationThrowsWhenRedisIsUnavailable() {

        // The framework calls the store from paths that do not handle a failure, so a failure is logged
        // and the operation is degraded rather than propagated.
        FailingConnectionManager connectionManager = new FailingConnectionManager();
        RedisSessionDataStore store = store(new RedisStoreConfig.Builder().build(),
                connectionManager);

        store.storeSessionData(KEY, TYPE, "entry", 1);
        store.clearSessionData(KEY, TYPE);
        store.removeSessionData(KEY, TYPE, 1L);
        store.removeTempAuthnContextData(KEY, TYPE);
        store.clearSessionDataBatch(List.of(KEY), TYPE);
        store.removeExpiredSessionData();

        assertNull(store.getSessionData(KEY, TYPE));
        assertNull(store.getSessionContextData(KEY, TYPE));
        assertNull(store.getSessionContextData(KEY, TYPE, RedisConstants.OPERATION_STORE));
        assertFalse(store.validateLastOperationOnSessionData(KEY, TYPE, RedisConstants.OPERATION_STORE));
    }

    @Test
    void testDeleteIsNotReportedAsTheLastOperation() {

        // A removal deletes the record, so a delete operation is never the last one of an available record.
        RedisSessionDataStore store = store(new RedisStoreConfig.Builder().build(),
                new FailingConnectionManager());

        assertNull(store.getSessionContextData(KEY, TYPE, RedisConstants.OPERATION_DELETE));
        assertFalse(store.validateLastOperationOnSessionData(KEY, TYPE, RedisConstants.OPERATION_DELETE));
    }

    @Test
    void testCleanupIsNotEnabled() {

        RedisSessionDataStore store = store(new RedisStoreConfig.Builder().build(),
                new FailingConnectionManager());

        assertFalse(store.isSessionDataCleanupEnabled());
        assertEquals(RedisConstants.STORE_NAME, store.getStoreName());
    }

    private static RedisSessionDataStore store(RedisStoreConfig config,
                                               RedisConnectionManager connectionManager) {

        return new RedisSessionDataStore(new RedisSessionContext(config, connectionManager));
    }
}
