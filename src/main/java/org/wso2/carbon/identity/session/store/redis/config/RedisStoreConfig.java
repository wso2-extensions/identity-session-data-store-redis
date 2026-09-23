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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.identity.application.authentication.framework.util.SessionMgtUtils;
import org.wso2.carbon.identity.session.store.redis.exception.RedisStoreConfigurationException;
import org.wso2.carbon.identity.session.store.redis.util.RedisConstants;

import java.util.Map;
import java.util.Properties;

/**
 * Configuration of the Redis session store, read from the session storage properties, which are also
 * where the store that reads them is selected.
 */
public class RedisStoreConfig {

    private static final Log LOG = LogFactory.getLog(RedisStoreConfig.class);

    private final String mode;
    private final String hosts;
    private final String masterName;
    private final String username;
    private final char[] password;
    private final char[] sentinelPassword;
    private final int database;
    private final boolean sslEnabled;
    private final long connectionTimeout;
    private final long commandTimeout;
    private final String keyPrefix;
    private final int batchSize;
    private final boolean topologyRefreshEnabled;
    private final long topologyRefreshPeriod;
    private final int maxRedirects;

    private RedisStoreConfig(Builder builder) {

        this.hosts = builder.hosts;
        this.masterName = builder.masterName;
        this.username = builder.username;
        this.password = builder.password;
        this.sentinelPassword = builder.sentinelPassword;
        this.mode = builder.mode;
        this.database = builder.database;
        this.sslEnabled = builder.sslEnabled;
        this.connectionTimeout = builder.connectionTimeout;
        this.commandTimeout = builder.commandTimeout;
        this.keyPrefix = builder.keyPrefix;
        this.batchSize = builder.batchSize;
        this.topologyRefreshEnabled = builder.topologyRefreshEnabled;
        this.topologyRefreshPeriod = builder.topologyRefreshPeriod;
        this.maxRedirects = builder.maxRedirects;
    }

    /**
     * Loads the configuration from the properties configured for the session storage, applying the
     * default of every property that is not configured.
     *
     * @return the loaded configuration.
     * @throws RedisStoreConfigurationException when no host is configured or the mode is unsupported.
     */
    public static RedisStoreConfig load() throws RedisStoreConfigurationException {

        Properties properties = new Properties();
        for (Map.Entry<String, String> property : SessionMgtUtils.getSessionStorageProperties().entrySet()) {
            if (property.getValue() != null) {
                properties.setProperty(property.getKey(), property.getValue());
            }
        }
        return from(properties);
    }

    /**
     * Builds the configuration from properties that have already been read.
     *
     * <p>{@value RedisConstants#CONF_HOSTS} is the one setting without a default: a selected store
     * without a host is rejected here, which fails activation, rather than connecting to a Redis
     * nobody configured.
     *
     * @param properties The configured properties.
     * @return the loaded configuration.
     * @throws RedisStoreConfigurationException when no host is configured or the mode is unsupported.
     */
    static RedisStoreConfig from(Properties properties) throws RedisStoreConfigurationException {

        Builder builder = new Builder()
                .mode(getString(properties, RedisConstants.CONF_MODE, RedisConstants.DEFAULT_MODE))
                .hosts(getString(properties, RedisConstants.CONF_HOSTS, StringUtils.EMPTY))
                .masterName(getString(properties, RedisConstants.CONF_MASTER_NAME, StringUtils.EMPTY))
                .username(getString(properties, RedisConstants.CONF_USERNAME, StringUtils.EMPTY))
                .password(getString(properties, RedisConstants.CONF_PASSWORD, StringUtils.EMPTY))
                .sentinelPassword(getString(properties, RedisConstants.CONF_SENTINEL_PASSWORD,
                        StringUtils.EMPTY))
                .database(getInt(properties, RedisConstants.CONF_DATABASE,
                        RedisConstants.DEFAULT_DATABASE, false))
                .sslEnabled(getBoolean(properties, RedisConstants.CONF_SSL_ENABLED,
                        RedisConstants.DEFAULT_SSL_ENABLED))
                .connectionTimeout(getLong(properties, RedisConstants.CONF_CONNECTION_TIMEOUT,
                        RedisConstants.DEFAULT_CONNECTION_TIMEOUT))
                .commandTimeout(getLong(properties, RedisConstants.CONF_COMMAND_TIMEOUT,
                        RedisConstants.DEFAULT_COMMAND_TIMEOUT))
                .keyPrefix(getString(properties, RedisConstants.CONF_KEY_PREFIX,
                        RedisConstants.DEFAULT_KEY_PREFIX))
                .batchSize(getInt(properties, RedisConstants.CONF_BATCH_SIZE,
                        RedisConstants.DEFAULT_BATCH_SIZE, true))
                .topologyRefreshEnabled(getBoolean(properties, RedisConstants.CONF_TOPOLOGY_REFRESH_ENABLED,
                        RedisConstants.DEFAULT_TOPOLOGY_REFRESH_ENABLED))
                .topologyRefreshPeriod(getLong(properties, RedisConstants.CONF_TOPOLOGY_REFRESH_PERIOD,
                        RedisConstants.DEFAULT_TOPOLOGY_REFRESH_PERIOD))
                .maxRedirects(getInt(properties, RedisConstants.CONF_MAX_REDIRECTS,
                        RedisConstants.DEFAULT_MAX_REDIRECTS, true));

        RedisStoreConfig config = builder.build();
        if (StringUtils.isBlank(config.getHosts())) {
            throw new RedisStoreConfigurationException("The session store is set to "
                    + RedisConstants.STORE_NAME + " but no Redis host is configured. Configure '"
                    + RedisConstants.CONF_HOSTS + "'.");
        }
        LOG.info("Redis session store configuration loaded. Mode: " + config.getMode()
                + ", hosts: " + config.getHosts() + ", database: " + config.getDatabase());
        return config;
    }

    /**
     * Rejects a mode that is not one of the supported topologies.
     */
    private static void validateMode(String mode) {

        if (RedisConstants.MODE_STANDALONE.equalsIgnoreCase(mode)
                || RedisConstants.MODE_SENTINEL.equalsIgnoreCase(mode)
                || RedisConstants.MODE_CLUSTER.equalsIgnoreCase(mode)) {
            return;
        }
        throw new RedisStoreConfigurationException("Unsupported Redis mode: " + mode + ". Supported modes are "
                + RedisConstants.MODE_STANDALONE + ", " + RedisConstants.MODE_SENTINEL + " and "
                + RedisConstants.MODE_CLUSTER + ".");
    }

    private static String getString(Properties properties, String key, String defaultValue) {

        String value = properties.getProperty(key);
        return StringUtils.isBlank(value) ? defaultValue : value.trim();
    }

    private static boolean getBoolean(Properties properties, String key, boolean defaultValue) {

        String value = properties.getProperty(key);
        return StringUtils.isBlank(value) ? defaultValue : Boolean.parseBoolean(value.trim());
    }

    private static long getLong(Properties properties, String key, long defaultValue) {

        String value = properties.getProperty(key);
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        try {
            long parsed = Long.parseLong(value.trim());
            if (parsed > 0) {
                return parsed;
            }
            LOG.warn("Value of " + key + " must be greater than zero. Using the default: " + defaultValue);
        } catch (NumberFormatException e) {
            LOG.warn("Invalid numeric value for " + key + ": " + value + ". Using the default: "
                    + defaultValue);
        }
        return defaultValue;
    }

    private static int getInt(Properties properties, String key, int defaultValue, boolean positiveOnly) {

        String value = properties.getProperty(key);
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        try {
            int parsed = Integer.parseInt(value.trim());
            if (parsed > 0 || (!positiveOnly && parsed == 0)) {
                return parsed;
            }
            LOG.warn("Invalid value for " + key + ": " + parsed + ". Using the default: " + defaultValue);
        } catch (NumberFormatException e) {
            LOG.warn("Invalid numeric value for " + key + ": " + value + ". Using the default: "
                    + defaultValue);
        }
        return defaultValue;
    }

    /**
     * The Redis topology to connect to.
     *
     * @return standalone, sentinel or cluster.
     */
    public String getMode() {

        return mode;
    }

    /**
     * The configured Redis hosts. The one setting that has no default, and the one a loaded
     * configuration is required to carry.
     *
     * @return the hosts, comma separated.
     */
    public String getHosts() {

        return hosts;
    }

    /**
     * The name of the master, used in sentinel mode.
     *
     * @return the master name, or an empty value when it is not configured.
     */
    public String getMasterName() {

        return masterName;
    }

    /**
     * The username used to authenticate against Redis.
     *
     * @return the username, or an empty value when it is not configured.
     */
    public String getUsername() {

        return username;
    }

    /**
     * The password used to authenticate against Redis, held as characters rather than a string so that
     * it is not interned and can be cleared by its caller.
     *
     * @return a copy of the password, or an empty array when it is not configured.
     */
    public char[] getPassword() {

        return password.clone();
    }

    /**
     * Whether a password is configured.
     *
     * @return true when a password is configured.
     */
    public boolean hasPassword() {

        return password.length > 0;
    }

    /**
     * The password used to authenticate against the sentinels.
     *
     * @return a copy of the sentinel password, or an empty array when it is not configured.
     */
    public char[] getSentinelPassword() {

        return sentinelPassword.clone();
    }

    /**
     * Whether a sentinel password is configured.
     *
     * @return true when a sentinel password is configured.
     */
    public boolean hasSentinelPassword() {

        return sentinelPassword.length > 0;
    }

    /**
     * The database the store uses, which must be zero in cluster mode.
     *
     * @return the database index.
     */
    public int getDatabase() {

        return database;
    }

    /**
     * Whether the connection to Redis uses SSL.
     *
     * @return true when SSL is enabled.
     */
    public boolean isSslEnabled() {

        return sslEnabled;
    }

    /**
     * How long a connection attempt may take, and the first cooldown after a failed one.
     *
     * @return the connection timeout in milliseconds.
     */
    public long getConnectionTimeout() {

        return connectionTimeout;
    }

    /**
     * How long a command may take.
     *
     * @return the command timeout in milliseconds.
     */
    public long getCommandTimeout() {

        return commandTimeout;
    }

    /**
     * The prefix of every key the store writes.
     *
     * @return the key prefix.
     */
    public String getKeyPrefix() {

        return keyPrefix;
    }

    /**
     * How many records are handled per batch when several are read or removed together.
     *
     * @return the batch size.
     */
    public int getBatchSize() {

        return batchSize;
    }

    /**
     * Whether the cluster topology is refreshed.
     *
     * @return true when the topology is refreshed.
     */
    public boolean isTopologyRefreshEnabled() {

        return topologyRefreshEnabled;
    }

    /**
     * How often the cluster topology is refreshed.
     *
     * @return the refresh period in seconds.
     */
    public long getTopologyRefreshPeriod() {

        return topologyRefreshPeriod;
    }

    /**
     * How many redirects are followed for one command in cluster mode.
     *
     * @return the maximum number of redirects.
     */
    public int getMaxRedirects() {

        return maxRedirects;
    }

    /**
     * Builder of {@link RedisStoreConfig}.
     */
    public static class Builder {

        private String mode = RedisConstants.DEFAULT_MODE;
        private String hosts = StringUtils.EMPTY;
        private String masterName = StringUtils.EMPTY;
        private String username = StringUtils.EMPTY;
        private char[] password = new char[0];
        private char[] sentinelPassword = new char[0];
        private int database = RedisConstants.DEFAULT_DATABASE;
        private boolean sslEnabled = RedisConstants.DEFAULT_SSL_ENABLED;
        private long connectionTimeout = RedisConstants.DEFAULT_CONNECTION_TIMEOUT;
        private long commandTimeout = RedisConstants.DEFAULT_COMMAND_TIMEOUT;
        private String keyPrefix = RedisConstants.DEFAULT_KEY_PREFIX;
        private int batchSize = RedisConstants.DEFAULT_BATCH_SIZE;
        private boolean topologyRefreshEnabled = RedisConstants.DEFAULT_TOPOLOGY_REFRESH_ENABLED;
        private long topologyRefreshPeriod = RedisConstants.DEFAULT_TOPOLOGY_REFRESH_PERIOD;
        private int maxRedirects = RedisConstants.DEFAULT_MAX_REDIRECTS;

        /**
         * Sets the value of the property.
         *
         * @param mode The Redis topology to connect to.
         * @return this builder.
         */
        public Builder mode(String mode) {

            this.mode = mode;
            return this;
        }

        /**
         * Sets the value of the property.
         *
         * @param hosts The Redis hosts, comma separated.
         * @return this builder.
         */
        public Builder hosts(String hosts) {

            this.hosts = hosts;
            return this;
        }

        /**
         * Sets the value of the property.
         *
         * @param masterName The name of the master, used in sentinel mode.
         * @return this builder.
         */
        public Builder masterName(String masterName) {

            this.masterName = masterName;
            return this;
        }

        /**
         * Sets the value of the property.
         *
         * @param username The user name used to authenticate.
         * @return this builder.
         */
        public Builder username(String username) {

            this.username = username;
            return this;
        }

        /**
         * Sets the value of the property.
         *
         * @param password The password used to authenticate.
         * @return this builder.
         */
        public Builder password(String password) {

            this.password = toCharArray(password);
            return this;
        }

        /**
         * Sets the value of the property.
         *
         * @param sentinelPassword The password used to authenticate against the sentinels.
         * @return this builder.
         */
        public Builder sentinelPassword(String sentinelPassword) {

            this.sentinelPassword = toCharArray(sentinelPassword);
            return this;
        }

        /**
         * Sets the value of the property.
         *
         * @param database The database the store uses.
         * @return this builder.
         */
        public Builder database(int database) {

            this.database = database;
            return this;
        }

        /**
         * Sets the value of the property.
         *
         * @param sslEnabled Whether the connection uses SSL.
         * @return this builder.
         */
        public Builder sslEnabled(boolean sslEnabled) {

            this.sslEnabled = sslEnabled;
            return this;
        }

        /**
         * Sets the value of the property.
         *
         * @param connectionTimeout The connection timeout in milliseconds.
         * @return this builder.
         */
        public Builder connectionTimeout(long connectionTimeout) {

            this.connectionTimeout = connectionTimeout;
            return this;
        }

        /**
         * Sets the value of the property.
         *
         * @param commandTimeout The command timeout in milliseconds.
         * @return this builder.
         */
        public Builder commandTimeout(long commandTimeout) {

            this.commandTimeout = commandTimeout;
            return this;
        }

        /**
         * Sets the value of the property.
         *
         * @param keyPrefix The prefix of every key.
         * @return this builder.
         */
        public Builder keyPrefix(String keyPrefix) {

            this.keyPrefix = keyPrefix;
            return this;
        }

        /**
         * Sets the value of the property.
         *
         * @param batchSize The number of records handled per batch.
         * @return this builder.
         */
        public Builder batchSize(int batchSize) {

            this.batchSize = batchSize;
            return this;
        }

        /**
         * Sets the value of the property.
         *
         * @param topologyRefreshEnabled Whether the cluster topology is refreshed.
         * @return this builder.
         */
        public Builder topologyRefreshEnabled(boolean topologyRefreshEnabled) {

            this.topologyRefreshEnabled = topologyRefreshEnabled;
            return this;
        }

        /**
         * Sets the value of the property.
         *
         * @param topologyRefreshPeriod The cluster topology refresh period in seconds.
         * @return this builder.
         */
        public Builder topologyRefreshPeriod(long topologyRefreshPeriod) {

            this.topologyRefreshPeriod = topologyRefreshPeriod;
            return this;
        }

        /**
         * Sets the value of the property.
         *
         * @param maxRedirects The number of redirects followed for a command.
         * @return this builder.
         */
        public Builder maxRedirects(int maxRedirects) {

            this.maxRedirects = maxRedirects;
            return this;
        }

        /**
         * Builds the configuration.
         *
         * @return the configuration.
         * @throws RedisStoreConfigurationException when the mode is unsupported.
         */
        public RedisStoreConfig build() throws RedisStoreConfigurationException {

            validateMode(mode);
            return new RedisStoreConfig(this);
        }

        private static char[] toCharArray(String value) {

            return StringUtils.isEmpty(value) ? new char[0] : value.toCharArray();
        }
    }
}
