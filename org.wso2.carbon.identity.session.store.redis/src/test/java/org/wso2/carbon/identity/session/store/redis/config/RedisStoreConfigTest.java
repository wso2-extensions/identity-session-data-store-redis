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

package org.wso2.carbon.identity.session.store.redis.config;

import org.junit.jupiter.api.Test;
import org.wso2.carbon.identity.session.store.redis.exception.RedisStoreConfigurationException;
import org.wso2.carbon.identity.session.store.redis.util.RedisConstants;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the defaults of the store configuration and how it is read from the configured properties.
 */
public class RedisStoreConfigTest {

    @Test
    void testDefaults() {

        RedisStoreConfig config = new RedisStoreConfig.Builder().build();

        assertEquals(RedisConstants.DEFAULT_MODE, config.getMode());
        assertEquals("", config.getHosts());
        assertEquals(RedisConstants.DEFAULT_DATABASE, config.getDatabase());
        assertEquals(RedisConstants.DEFAULT_CONNECTION_TIMEOUT, config.getConnectionTimeout());
        assertEquals(RedisConstants.DEFAULT_COMMAND_TIMEOUT, config.getCommandTimeout());
        assertEquals(RedisConstants.DEFAULT_KEY_PREFIX, config.getKeyPrefix());
        assertEquals(RedisConstants.DEFAULT_BATCH_SIZE, config.getBatchSize());
    }

    @Test
    void testClusterDefaults() {

        RedisStoreConfig config = new RedisStoreConfig.Builder().build();

        // A topology refresh is enabled by default, since without one a resharding or a failover leaves
        // commands routed to a node that no longer owns the keys.
        assertTrue(config.isTopologyRefreshEnabled());
        assertEquals(RedisConstants.DEFAULT_TOPOLOGY_REFRESH_PERIOD, config.getTopologyRefreshPeriod());
        assertEquals(RedisConstants.DEFAULT_MAX_REDIRECTS, config.getMaxRedirects());
    }

    @Test
    void testOverrides() {

        RedisStoreConfig config = new RedisStoreConfig.Builder()
                .mode(RedisConstants.MODE_CLUSTER)
                .hosts("10.0.0.1:7000")
                .keyPrefix("session")
                .batchSize(50)
                .commandTimeout(500)
                .maxRedirects(9)
                .topologyRefreshEnabled(false)
                .build();

        assertEquals(RedisConstants.MODE_CLUSTER, config.getMode());
        assertEquals("10.0.0.1:7000", config.getHosts());
        assertEquals("session", config.getKeyPrefix());
        assertEquals(50, config.getBatchSize());
        assertEquals(500, config.getCommandTimeout());
        assertEquals(9, config.getMaxRedirects());
        assertEquals(false, config.isTopologyRefreshEnabled());
    }

    @Test
    void testUnsupportedModeIsRejected() {

        // connect() treats anything that is not cluster as a single server, so an unrecognised mode has
        // to fail here rather than silently connect with the wrong topology.
        assertThrows(RedisStoreConfigurationException.class,
                () -> new RedisStoreConfig.Builder().mode("clustre").build());
    }

    @Test
    void testSupportedModesAreKeptVerbatim() {

        assertEquals(RedisConstants.MODE_CLUSTER,
                new RedisStoreConfig.Builder().mode(RedisConstants.MODE_CLUSTER).build().getMode());
        assertEquals("SENTINEL", new RedisStoreConfig.Builder().mode("SENTINEL").build().getMode());
    }

    @Test
    void testPasswordsAreHeldAsCharacters() {

        RedisStoreConfig config = new RedisStoreConfig.Builder()
                .password("secret")
                .build();

        assertTrue(config.hasPassword());
        assertArrayEquals("secret".toCharArray(), config.getPassword());
        assertFalse(config.hasSentinelPassword());
        assertEquals(0, config.getSentinelPassword().length);
    }

    @Test
    void testPasswordCopyCannotBeUsedToChangeTheConfiguration() {

        RedisStoreConfig config = new RedisStoreConfig.Builder().password("secret").build();

        char[] copy = config.getPassword();
        copy[0] = 'x';

        assertArrayEquals("secret".toCharArray(), config.getPassword());
    }

    @Test
    void testOnlyTheHostAndTheEnableFlagHaveToBeConfigured() {

        // The minimum configuration is a host. Everything else is left to its default, so a deployment
        // that has no opinion on timeouts, key prefix or batching does not have to hold one.
        Properties properties = new Properties();
        properties.setProperty(RedisConstants.CONF_HOSTS, "redis:6379");

        RedisStoreConfig config = RedisStoreConfig.from(properties);

        assertEquals("redis:6379", config.getHosts());
        assertEquals(RedisConstants.DEFAULT_MODE, config.getMode());
        assertEquals(RedisConstants.DEFAULT_KEY_PREFIX, config.getKeyPrefix());
        assertEquals(RedisConstants.DEFAULT_BATCH_SIZE, config.getBatchSize());
        assertEquals(RedisConstants.DEFAULT_CONNECTION_TIMEOUT, config.getConnectionTimeout());
        assertEquals(RedisConstants.DEFAULT_COMMAND_TIMEOUT, config.getCommandTimeout());
        assertEquals(RedisConstants.DEFAULT_DATABASE, config.getDatabase());
        assertFalse(config.isSslEnabled());
        assertFalse(config.hasPassword());
    }

    @Test
    void testEveryPropertyIsRead() {

        Properties properties = new Properties();
        properties.setProperty(RedisConstants.CONF_MODE, RedisConstants.MODE_SENTINEL);
        properties.setProperty(RedisConstants.CONF_HOSTS, "sentinel-1:26379, sentinel-2:26379");
        properties.setProperty(RedisConstants.CONF_MASTER_NAME, "mymaster");
        properties.setProperty(RedisConstants.CONF_USERNAME, "sessions");
        properties.setProperty(RedisConstants.CONF_PASSWORD, "secret");
        properties.setProperty(RedisConstants.CONF_SENTINEL_PASSWORD, "sentinel-secret");
        properties.setProperty(RedisConstants.CONF_DATABASE, "3");
        properties.setProperty(RedisConstants.CONF_SSL_ENABLED, "true");
        properties.setProperty(RedisConstants.CONF_CONNECTION_TIMEOUT, "5000");
        properties.setProperty(RedisConstants.CONF_COMMAND_TIMEOUT, "250");
        properties.setProperty(RedisConstants.CONF_KEY_PREFIX, "session");
        properties.setProperty(RedisConstants.CONF_BATCH_SIZE, "50");
        properties.setProperty(RedisConstants.CONF_TOPOLOGY_REFRESH_ENABLED, "false");
        properties.setProperty(RedisConstants.CONF_TOPOLOGY_REFRESH_PERIOD, "30");
        properties.setProperty(RedisConstants.CONF_MAX_REDIRECTS, "9");

        RedisStoreConfig config = RedisStoreConfig.from(properties);

        assertEquals(RedisConstants.MODE_SENTINEL, config.getMode());
        assertEquals("sentinel-1:26379, sentinel-2:26379", config.getHosts());
        assertEquals("mymaster", config.getMasterName());
        assertEquals("sessions", config.getUsername());
        assertArrayEquals("secret".toCharArray(), config.getPassword());
        assertArrayEquals("sentinel-secret".toCharArray(), config.getSentinelPassword());
        assertEquals(3, config.getDatabase());
        assertTrue(config.isSslEnabled());
        assertEquals(5000, config.getConnectionTimeout());
        assertEquals(250, config.getCommandTimeout());
        assertEquals("session", config.getKeyPrefix());
        assertEquals(50, config.getBatchSize());
        assertFalse(config.isTopologyRefreshEnabled());
        assertEquals(30, config.getTopologyRefreshPeriod());
        assertEquals(9, config.getMaxRedirects());
    }

    @Test
    void testAStoreWithoutAHostFailsToLoad() {

        // There is no default host, so a store selected without one fails to load rather than coming up
        // to persist nothing, or connecting to a Redis that was never configured.
        RedisStoreConfigurationException exception = assertThrows(RedisStoreConfigurationException.class,
                () -> RedisStoreConfig.from(new Properties()));

        assertTrue(exception.getMessage().contains(RedisConstants.CONF_HOSTS));
    }

    @Test
    void testABlankHostIsTreatedAsNoHost() {

        Properties properties = new Properties();
        properties.setProperty(RedisConstants.CONF_HOSTS, "   ");

        assertThrows(RedisStoreConfigurationException.class,
                () -> RedisStoreConfig.from(properties));
    }

    @Test
    void testInvalidNumbersFallBackToTheirDefaults() {

        Properties properties = new Properties();
        properties.setProperty(RedisConstants.CONF_HOSTS, "redis:6379");
        properties.setProperty(RedisConstants.CONF_BATCH_SIZE, "not-a-number");
        properties.setProperty(RedisConstants.CONF_COMMAND_TIMEOUT, "-1");

        RedisStoreConfig config = RedisStoreConfig.from(properties);

        assertEquals(RedisConstants.DEFAULT_BATCH_SIZE, config.getBatchSize());
        assertEquals(RedisConstants.DEFAULT_COMMAND_TIMEOUT, config.getCommandTimeout());
    }
}
