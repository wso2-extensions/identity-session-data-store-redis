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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.identity.session.store.redis.config.RedisStoreConfig;
import org.wso2.carbon.identity.session.store.redis.exception.RedisSessionStoreException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Owns the Redis client and the shared connection used by the session store.
 * <p>
 * All three topologies are served through {@link RedisClusterCommands}, so callers are unaware of which
 * one is configured. The connection is opened on first use, and an unreachable Redis is retried on a
 * later operation with a growing cooldown.
 */
public class RedisConnectionManager implements AutoCloseable {

    private static final Log LOG = LogFactory.getLog(RedisConnectionManager.class);

    private static final RedisCodec<String, byte[]> CODEC =
            RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE);
    private static final String HOST_SEPARATOR = ",";

    // Separator between a host and its port. Unrelated to the key separator, which shares its value.
    private static final String HOST_PORT_SEPARATOR = ":";
    private static final String IPV6_START = "[";
    private static final String IPV6_END = "]";

    private final RedisStoreConfig config;
    private final ReentrantLock connectLock = new ReentrantLock();

    private volatile AbstractRedisClient client;
    private volatile StatefulConnection<String, byte[]> connection;
    private volatile RedisClusterCommands<String, byte[]> syncCommands;
    private volatile RedisClusterAsyncCommands<String, byte[]> asyncCommands;
    private volatile long nextRetryTime;
    private final AtomicLong retryCooldown = new AtomicLong();

    /**
     * Creates a manager. Nothing is connected until an operation needs a connection.
     *
     * @param config Store configuration.
     */
    public RedisConnectionManager(RedisStoreConfig config) {

        this.config = config;
        this.retryCooldown.set(config == null
                ? RedisConstants.DEFAULT_CONNECTION_TIMEOUT : config.getConnectionTimeout());
    }

    /**
     * Creates a manager that connects to nothing, for a subclass that supplies its own command
     * interfaces.
     */
    protected RedisConnectionManager() {

        this.config = null;
        this.retryCooldown.set(RedisConstants.DEFAULT_CONNECTION_TIMEOUT);
    }

    /**
     * Whether a connection to Redis is currently open.
     *
     * @return true when the store is connected to Redis.
     */
    public boolean isConnected() {

        StatefulConnection<String, byte[]> current = connection;
        return current != null && current.isOpen();
    }

    /**
     * The command interface used for a single operation.
     *
     * @return the synchronous command interface. Thread safe.
     * @throws RedisSessionStoreException if no connection could be established.
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "The command interface is the "
            + "shared connection itself, not a value. A copy of it would address a different "
            + "connection than the one this manager owns.")
    public RedisClusterCommands<String, byte[]> getCommands() throws RedisSessionStoreException {

        ensureConnected();
        return syncCommands;
    }

    /**
     * The command interface used to pipeline a batch of operations.
     *
     * @return the asynchronous command interface, for pipelining a bounded batch of commands.
     * @throws RedisSessionStoreException if no connection could be established.
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "The command interface is the "
            + "shared connection itself, not a value. A copy of it would address a different "
            + "connection than the one this manager owns.")
    public RedisClusterAsyncCommands<String, byte[]> getAsyncCommands() throws RedisSessionStoreException {

        ensureConnected();
        return asyncCommands;
    }

    private void ensureConnected() throws RedisSessionStoreException {

        StatefulConnection<String, byte[]> current = connection;
        if (current == null || !current.isOpen()) {
            reconnect();
        }
    }

    /**
     * Attempts a connection on behalf of one thread only. A thread that does not get the lock fails at
     * once rather than queueing behind an attempt that can take the whole connect timeout.
     */
    private void reconnect() throws RedisSessionStoreException {

        if (System.currentTimeMillis() < nextRetryTime) {
            throw new RedisSessionStoreException("No connection to Redis at " + hosts()
                    + ". The previous attempt failed less than " + retryCooldown.get() + "ms ago.");
        }
        if (!connectLock.tryLock()) {
            throw new RedisSessionStoreException("No connection to Redis at " + hosts()
                    + ". Another thread is attempting to connect.");
        }
        try {
            if (isConnected()) {
                return;
            }
            if (System.currentTimeMillis() < nextRetryTime) {
                throw new RedisSessionStoreException("No connection to Redis at " + hosts()
                        + ". The previous attempt failed less than " + retryCooldown.get() + "ms ago.");
            }
            connect();
            resetRetryCooldown();
        } catch (RedisSessionStoreException e) {
            // The failure is reported by the operation that needed the connection, as the relational
            // store reports a connection it could not obtain.
            deferRetry();
            throw e;
        } finally {
            connectLock.unlock();
        }
    }

    private String hosts() {

        return config == null || StringUtils.isBlank(config.getHosts())
                ? "the configured hosts" : config.getHosts();
    }

    private void connect() throws RedisSessionStoreException {

        if (config == null) {
            throw new RedisSessionStoreException("Redis connection manager was created without a "
                    + "configuration and its command interfaces were not supplied.");
        }
        // Lettuce resolves optional libraries such as the netty DNS resolver through the thread context
        // class loader. Pinning it keeps resolution inside this bundle, whose netty copy is embedded.
        ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
        AbstractRedisClient newClient = null;
        try {
            Thread.currentThread().setContextClassLoader(RedisConnectionManager.class.getClassLoader());
            newClient = RedisConstants.MODE_CLUSTER.equalsIgnoreCase(config.getMode())
                    ? connectToCluster()
                    : connectToServer();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Connected to Redis. Mode: " + config.getMode() + ", hosts: "
                        + config.getHosts() + ", database: " + config.getDatabase());
            }
        } catch (RuntimeException e) {
            release(null, newClient);
            throw new RedisSessionStoreException("Error while connecting to Redis at " + config.getHosts(), e);
        } finally {
            Thread.currentThread().setContextClassLoader(previousClassLoader);
        }
    }

    private AbstractRedisClient connectToServer() throws RedisSessionStoreException {

        RedisURI uri = RedisConstants.MODE_SENTINEL.equalsIgnoreCase(config.getMode())
                ? buildSentinelUri(config)
                : buildStandaloneUri(config);

        RedisClient newClient = RedisClient.create(uri);
        newClient.setOptions(ClientOptions.builder()
                .autoReconnect(true)
                .socketOptions(SocketOptions.builder()
                        .connectTimeout(Duration.ofMillis(config.getConnectionTimeout()))
                        .build())
                .build());
        boolean adopted = false;
        try {
            StatefulRedisConnection<String, byte[]> newConnection = newClient.connect(CODEC);
            newConnection.setTimeout(Duration.ofMillis(config.getCommandTimeout()));
            adopt(newClient, newConnection, newConnection.sync(), newConnection.async());
            adopted = true;
            return newClient;
        } finally {
            if (!adopted) {
                // The client is not returned to connect() on a failure, so it has to be released here.
                release(null, newClient);
            }
        }
    }

    private AbstractRedisClient connectToCluster() throws RedisSessionStoreException {

        RedisClusterClient newClient = RedisClusterClient.create(buildClusterUris(config));
        ClusterClientOptions.Builder options = ClusterClientOptions.builder()
                .autoReconnect(true)
                .maxRedirects(config.getMaxRedirects())
                .socketOptions(SocketOptions.builder()
                        .connectTimeout(Duration.ofMillis(config.getConnectionTimeout()))
                        .build());
        if (config.isTopologyRefreshEnabled()) {
            // Without a refresh, a resharding or a failover leaves commands routed to a stale slot owner
            // until the server restarts.
            options.topologyRefreshOptions(ClusterTopologyRefreshOptions.builder()
                    .enablePeriodicRefresh(Duration.ofSeconds(config.getTopologyRefreshPeriod()))
                    .enableAllAdaptiveRefreshTriggers()
                    .build());
        }
        newClient.setOptions(options.build());

        boolean adopted = false;
        try {
            StatefulRedisClusterConnection<String, byte[]> newConnection = newClient.connect(CODEC);
            newConnection.setTimeout(Duration.ofMillis(config.getCommandTimeout()));
            adopt(newClient, newConnection, newConnection.sync(), newConnection.async());
            adopted = true;
            return newClient;
        } finally {
            if (!adopted) {
                // The client is not returned to connect() on a failure, so it has to be released here.
                release(null, newClient);
            }
        }
    }

    private void adopt(AbstractRedisClient newClient, StatefulConnection<String, byte[]> newConnection,
                       RedisClusterCommands<String, byte[]> newSyncCommands,
                       RedisClusterAsyncCommands<String, byte[]> newAsyncCommands) {

        release(this.connection, this.client);
        this.client = newClient;
        this.connection = newConnection;
        this.syncCommands = newSyncCommands;
        this.asyncCommands = newAsyncCommands;
    }

    /**
     * Builds the URI of a standalone server, which is the first configured host.
     *
     * @param config Store configuration.
     * @return the server URI.
     * @throws RedisSessionStoreException if the configured port is not a number.
     */
    public static RedisURI buildStandaloneUri(RedisStoreConfig config) throws RedisSessionStoreException {

        String host = config.getHosts().split(HOST_SEPARATOR)[0].trim();
        RedisURI.Builder builder = RedisURI.builder()
                .withHost(resolveHost(host))
                .withPort(resolvePort(host, RedisConstants.DEFAULT_REDIS_PORT))
                .withSsl(config.isSslEnabled())
                .withDatabase(config.getDatabase())
                .withTimeout(Duration.ofMillis(config.getCommandTimeout()));
        applyCredentials(builder, config);
        return builder.build();
    }

    /**
     * Builds the URI of a sentinel deployment, where the configured hosts are the sentinels.
     *
     * @param config Store configuration.
     * @return the sentinel URI.
     * @throws RedisSessionStoreException if the master name or the sentinel hosts are missing.
     */
    public static RedisURI buildSentinelUri(RedisStoreConfig config) throws RedisSessionStoreException {

        if (StringUtils.isBlank(config.getMasterName())) {
            throw new RedisSessionStoreException("Sentinel mode requires '"
                    + RedisConstants.CONF_MASTER_NAME + "' to be configured.");
        }
        boolean hasSentinelPassword = config.hasSentinelPassword();

        RedisURI.Builder builder = null;
        for (String entry : config.getHosts().split(HOST_SEPARATOR)) {
            String host = entry.trim();
            if (StringUtils.isBlank(host)) {
                continue;
            }
            int port = resolvePort(host, RedisConstants.DEFAULT_SENTINEL_PORT);
            if (builder == null) {
                builder = hasSentinelPassword
                        ? RedisURI.Builder.sentinel(resolveHost(host), port, config.getMasterName(),
                                String.valueOf(config.getSentinelPassword()))
                        : RedisURI.Builder.sentinel(resolveHost(host), port, config.getMasterName());
            } else if (hasSentinelPassword) {
                builder.withSentinel(resolveHost(host), port, String.valueOf(config.getSentinelPassword()));
            } else {
                builder.withSentinel(resolveHost(host), port);
            }
        }
        if (builder == null) {
            throw new RedisSessionStoreException("Sentinel mode requires at least one sentinel host to be "
                    + "configured in '" + RedisConstants.CONF_HOSTS + "'.");
        }
        builder.withSsl(config.isSslEnabled())
                .withDatabase(config.getDatabase())
                .withTimeout(Duration.ofMillis(config.getCommandTimeout()));
        applyCredentials(builder, config);
        return builder.build();
    }

    /**
     * Builds a seed URI per configured host, so one unavailable node does not prevent topology
     * discovery.
     *
     * @param config Store configuration.
     * @return the seed node URIs.
     * @throws RedisSessionStoreException if no host is configured, or a database other than zero is.
     */
    public static List<RedisURI> buildClusterUris(RedisStoreConfig config) throws RedisSessionStoreException {

        if (config.getDatabase() != 0) {
            throw new RedisSessionStoreException("Cluster mode supports only database 0 but '"
                    + RedisConstants.CONF_DATABASE + "' is set to " + config.getDatabase() + ".");
        }
        List<RedisURI> uris = new ArrayList<>();
        for (String entry : config.getHosts().split(HOST_SEPARATOR)) {
            String host = entry.trim();
            if (StringUtils.isBlank(host)) {
                continue;
            }
            RedisURI.Builder builder = RedisURI.builder()
                    .withHost(resolveHost(host))
                    .withPort(resolvePort(host, RedisConstants.DEFAULT_REDIS_PORT))
                    .withSsl(config.isSslEnabled())
                    .withTimeout(Duration.ofMillis(config.getCommandTimeout()));
            applyCredentials(builder, config);
            uris.add(builder.build());
        }
        if (uris.isEmpty()) {
            throw new RedisSessionStoreException("Cluster mode requires at least one seed host to be "
                    + "configured in '" + RedisConstants.CONF_HOSTS + "'.");
        }
        return uris;
    }

    private static void applyCredentials(RedisURI.Builder builder, RedisStoreConfig config) {

        if (!config.hasPassword()) {
            return;
        }
        if (StringUtils.isNotBlank(config.getUsername())) {
            builder.withAuthentication(config.getUsername(), config.getPassword());
        } else {
            builder.withPassword(config.getPassword());
        }
    }

    /**
     * Reads the host of one configured entry, which is {@code host}, {@code host:port}, {@code [ipv6]}
     * or {@code [ipv6]:port}.
     */
    private static String resolveHost(String entry) {

        if (entry.startsWith(IPV6_START)) {
            int end = entry.indexOf(IPV6_END);
            return end > 1 ? entry.substring(1, end) : entry;
        }
        int index = entry.indexOf(HOST_PORT_SEPARATOR);
        if (index <= 0 || entry.indexOf(HOST_PORT_SEPARATOR, index + 1) > 0) {
            return entry;
        }
        return entry.substring(0, index);
    }

    /**
     * Reads the port of one configured entry, or the given default when it carries none. An IPv6 address
     * has to be bracketed to carry one, as it holds the separator itself.
     */
    private static int resolvePort(String entry, int defaultPort) throws RedisSessionStoreException {

        String port;
        if (entry.startsWith(IPV6_START)) {
            int end = entry.indexOf(IPV6_END);
            if (end < 0 || !entry.startsWith(IPV6_END + HOST_PORT_SEPARATOR, end)) {
                return defaultPort;
            }
            port = entry.substring(end + 2).trim();
        } else {
            int index = entry.indexOf(HOST_PORT_SEPARATOR);
            if (index <= 0 || entry.indexOf(HOST_PORT_SEPARATOR, index + 1) > 0) {
                return defaultPort;
            }
            port = entry.substring(index + 1).trim();
        }
        if (port.isEmpty()) {
            return defaultPort;
        }
        try {
            return Integer.parseInt(port);
        } catch (NumberFormatException e) {
            throw new RedisSessionStoreException("Invalid port " + port + " configured in '"
                    + RedisConstants.CONF_HOSTS + "'.", e);
        }
    }

    /**
     * Holds off the next attempt, doubling the cooldown up to
     * {@link RedisConstants#MAX_RECONNECT_COOLDOWN_MILLIS}.
     */
    private void deferRetry() {

        nextRetryTime = System.currentTimeMillis() + retryCooldown.get();
        retryCooldown.updateAndGet(current ->
                Math.min(current * 2, RedisConstants.MAX_RECONNECT_COOLDOWN_MILLIS));
    }

    private void resetRetryCooldown() {

        nextRetryTime = 0;
        retryCooldown.set(config == null
                ? RedisConstants.DEFAULT_CONNECTION_TIMEOUT : config.getConnectionTimeout());
    }

    private static void release(StatefulConnection<String, byte[]> connection, AbstractRedisClient client) {

        try {
            if (connection != null) {
                connection.close();
            }
        } catch (RuntimeException e) {
            LOG.warn("Error while closing the previous Redis connection.", e);
        }
        try {
            if (client != null) {
                client.shutdown();
            }
        } catch (RuntimeException e) {
            LOG.warn("Error while shutting down the previous Redis client.", e);
        }
    }

    /**
     * Closes the connection and shuts down the client.
     */
    @Override
    public void close() {

        release(connection, client);
        connection = null;
        client = null;
        syncCommands = null;
        asyncCommands = null;
        resetRetryCooldown();
    }
}
