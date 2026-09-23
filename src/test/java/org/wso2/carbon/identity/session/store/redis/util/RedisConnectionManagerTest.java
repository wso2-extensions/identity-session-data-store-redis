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

import io.lettuce.core.RedisURI;
import org.junit.jupiter.api.Test;
import org.wso2.carbon.identity.session.store.redis.config.RedisStoreConfig;
import org.wso2.carbon.identity.session.store.redis.exception.RedisSessionStoreException;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the connection details built for each supported topology.
 */
public class RedisConnectionManagerTest {

    @Test
    void testStandaloneUriUsesTheFirstHost() throws Exception {

        RedisURI uri = RedisConnectionManager.buildStandaloneUri(new RedisStoreConfig.Builder()
                .hosts("10.0.0.1:6380,10.0.0.2:6381")
                .database(3)
                .build());

        assertEquals("10.0.0.1", uri.getHost());
        assertEquals(6380, uri.getPort());
        assertEquals(3, uri.getDatabase());
    }

    @Test
    void testStandaloneUriAppliesTheDefaultPort() throws Exception {

        RedisURI uri = RedisConnectionManager.buildStandaloneUri(new RedisStoreConfig.Builder()
                .hosts("10.0.0.1")
                .build());

        assertEquals(RedisConstants.DEFAULT_REDIS_PORT, uri.getPort());
    }

    @Test
    void testBracketedIpv6HostIsParsed() throws Exception {

        RedisURI uri = RedisConnectionManager.buildStandaloneUri(new RedisStoreConfig.Builder()
                .hosts("[2001:db8::1]:6380")
                .build());

        assertEquals("2001:db8::1", uri.getHost());
        assertEquals(6380, uri.getPort());
    }

    @Test
    void testBareIpv6HostTakesTheDefaultPort() throws Exception {

        // An unbracketed IPv6 literal holds the host and port separator itself, so it cannot carry a port
        // and must not be split at its last separator.
        RedisURI uri = RedisConnectionManager.buildStandaloneUri(new RedisStoreConfig.Builder()
                .hosts("::1")
                .build());

        assertEquals("::1", uri.getHost());
        assertEquals(RedisConstants.DEFAULT_REDIS_PORT, uri.getPort());
    }

    @Test
    void testSentinelUriHoldsEverySentinel() throws Exception {

        RedisURI uri = RedisConnectionManager.buildSentinelUri(new RedisStoreConfig.Builder()
                .mode(RedisConstants.MODE_SENTINEL)
                .hosts("10.0.0.1:26379,10.0.0.2:26379,10.0.0.3")
                .masterName("mymaster")
                .build());

        assertEquals("mymaster", uri.getSentinelMasterId());
        assertEquals(3, uri.getSentinels().size());
        assertEquals(RedisConstants.DEFAULT_SENTINEL_PORT, uri.getSentinels().get(2).getPort());
    }

    @Test
    void testSentinelPasswordIsAppliedToEverySentinel() throws Exception {

        RedisURI single = RedisConnectionManager.buildSentinelUri(new RedisStoreConfig.Builder()
                .mode(RedisConstants.MODE_SENTINEL)
                .hosts("10.0.0.1:26379")
                .masterName("mymaster")
                .sentinelPassword("sentinel-secret")
                .build());

        assertEquals("sentinel-secret", new String(single.getSentinels().get(0).getPassword()));

        RedisURI several = RedisConnectionManager.buildSentinelUri(new RedisStoreConfig.Builder()
                .mode(RedisConstants.MODE_SENTINEL)
                .hosts("10.0.0.1:26379,10.0.0.2:26379")
                .masterName("mymaster")
                .sentinelPassword("sentinel-secret")
                .build());

        for (RedisURI sentinel : several.getSentinels()) {
            assertEquals("sentinel-secret", new String(sentinel.getPassword()));
        }
    }

    @Test
    void testSentinelModeRequiresTheMasterName() {

        assertThrows(RedisSessionStoreException.class,
                () -> RedisConnectionManager.buildSentinelUri(new RedisStoreConfig.Builder()
                        .mode(RedisConstants.MODE_SENTINEL)
                        .hosts("10.0.0.1:26379")
                        .build()));
    }

    @Test
    void testClusterUrisHoldEverySeedHost() throws Exception {

        List<RedisURI> uris = RedisConnectionManager.buildClusterUris(clusterConfig(
                "10.0.0.1:7000,10.0.0.2:7001,10.0.0.3:7002"));

        assertEquals(3, uris.size());
        assertEquals("10.0.0.1", uris.get(0).getHost());
        assertEquals(7000, uris.get(0).getPort());
        assertEquals("10.0.0.3", uris.get(2).getHost());
        assertEquals(7002, uris.get(2).getPort());
    }

    @Test
    void testClusterUrisIgnoreBlankHosts() throws Exception {

        List<RedisURI> uris = RedisConnectionManager.buildClusterUris(clusterConfig(
                " 10.0.0.1:7000 , , 10.0.0.2:7001 "));

        assertEquals(2, uris.size());
        assertEquals("10.0.0.1", uris.get(0).getHost());
        assertEquals("10.0.0.2", uris.get(1).getHost());
    }

    @Test
    void testClusterUrisCarryCredentials() throws Exception {

        List<RedisURI> uris = RedisConnectionManager.buildClusterUris(new RedisStoreConfig.Builder()
                .mode(RedisConstants.MODE_CLUSTER)
                .hosts("10.0.0.1:7000,10.0.0.2:7001")
                .username("admin")
                .password("secret")
                .build());

        for (RedisURI uri : uris) {
            assertNotNull(uri.getCredentialsProvider());
        }
    }

    @Test
    void testClusterModeRejectsANonZeroDatabase() {

        // A clustered node supports only database zero, so the setting cannot be honoured.
        RedisStoreConfig config = new RedisStoreConfig.Builder()
                .mode(RedisConstants.MODE_CLUSTER)
                .hosts("10.0.0.1:7000")
                .database(3)
                .build();

        RedisSessionStoreException exception = assertThrows(RedisSessionStoreException.class,
                () -> RedisConnectionManager.buildClusterUris(config));
        assertTrue(exception.getMessage().contains("database 0"));
    }

    @Test
    void testClusterModeRequiresASeedHost() {

        assertThrows(RedisSessionStoreException.class,
                () -> RedisConnectionManager.buildClusterUris(clusterConfig(" , ")));
    }

    @Test
    void testInvalidPortIsRejected() {

        assertThrows(RedisSessionStoreException.class,
                () -> RedisConnectionManager.buildClusterUris(clusterConfig("10.0.0.1:invalid")));
    }

    private static RedisStoreConfig clusterConfig(String hosts) {

        return new RedisStoreConfig.Builder()
                .mode(RedisConstants.MODE_CLUSTER)
                .hosts(hosts)
                .build();
    }
}
