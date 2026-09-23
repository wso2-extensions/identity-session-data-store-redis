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

package org.wso2.carbon.identity.session.store.redis.internal;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the check that decides whether the component registers the Redis session store.
 */
public class RedisSessionStoreServiceComponentTest {

    @Test
    void testSelectedWhenRedisIsConfigured() {

        assertTrue(RedisSessionStoreServiceComponent.isSelected("redis"));
    }

    @Test
    void testNotSelectedOnTheDefaultStore() {

        // The framework reports "jdbc" when no store is configured, which must not read the Redis
        // configuration or log its missing host.
        assertFalse(RedisSessionStoreServiceComponent.isSelected("jdbc"));
    }

    @Test
    void testNotSelectedForAnotherStore() {

        assertFalse(RedisSessionStoreServiceComponent.isSelected("custom"));
    }

    @Test
    void testNotSelectedWhenNameIsMissing() {

        assertFalse(RedisSessionStoreServiceComponent.isSelected(null));
    }
}
